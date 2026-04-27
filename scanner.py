"""
scanner.py v2 — Hyperliquid scanner for BTC / ETH / SOL.

Background thread inside api.py process. Same public contract as v1:
    start_scanner_thread(telegram_token, chat_id, self_webhook_url)

Same /webhook payload as v1 (api.py не трогаем). Дополнительные поля
(grade, funding_risk, rsi_dir, atr_mode) добавлены безопасно — api.py
их игнорирует, бот сможет начать использовать на следующей итерации.

Priority 1 changes vs v1:
    1. Бинарный грейд: A+ / B / drop. Никаких A, B+, trash.
    2. ATR(14) считается на 1H. Абсолютные пороги по активу
       (BTC 1.5%, ETH 2%, SOL 3%) + сравнение с 20-дневным средним.
    3. Зона выдаётся, только если 4H-тренд совпадает с 1H-трендом.
    4. RSI(14) на 1H — направление под сделку, без 70/30.
    5. Funding — контекстный флаг funding_risk, не жёсткий блок.
"""

import os
import json
import time
import threading
from datetime import datetime, timezone

import requests


HL_INFO_URL = "https://api.hyperliquid.xyz/info"

ASSETS = ["BTC", "ETH", "SOL"]

# Абсолютные пороги ATR(1H) как доля цены
ATR_ABS_THRESHOLDS = {
    "BTC": 0.015,
    "ETH": 0.020,
    "SOL": 0.030,
}

SETUP_GRADES = ("A+", "B", "IGNORE")
SMART_STATUSES = ("ACTION", "READY", "WATCH", "IGNORE")

SCAN_INTERVAL_SEC = 5 * 60           # цикл сканера — раз в 5 минут
REPORT_INTERVAL_SEC = 60 * 60
ZONE_HIT_COOLDOWN_SEC = 30 * 60      # кулдаун ZONE_HIT на актив
NEAR_ZONE_COOLDOWN_SEC = 30 * 60     # кулдаун NEAR_ZONE на актив

# Funding — контекст, не блок
FUNDING_LONG_RISK = 0.0003           # > +0.03% — long под вопросом
FUNDING_SHORT_RISK = -0.0003         # < -0.03% — short под вопросом

INTERVAL_MS = {
    "1m": 60_000,
    "5m": 5 * 60_000,
    "15m": 15 * 60_000,
    "30m": 30 * 60_000,
    "1h": 60 * 60_000,
    "2h": 2 * 60 * 60_000,
    "4h": 4 * 60 * 60_000,
    "1d": 24 * 60 * 60_000,
}

_state = {
    "last_zone_hit_at": {a: 0.0 for a in ASSETS},
    "last_near_zone_at": {a: 0.0 for a in ASSETS},
    "last_report_at": 0.0,
    # ключи зон, где цена уже была внутри на прошлом цикле;
    # нужно, чтобы ZONE HIT срабатывал именно на входе в зону, а не каждые 30 минут
    "inside_zone_keys": set(),
    # ключи зон, где цена была "near" (в пределах 0.5 ATR от границы), но снаружи
    "near_zone_keys": set(),
    # защита от ложных ZONE_HIT/NEAR_ZONE после рестарта: первый цикл только прогревает state
    "warmup_done": False,
    "thread": None,
}
_state_lock = threading.Lock()


# ---------------------------------------------------------------- HTTP

def _hl_post(payload, timeout=15):
    r = requests.post(HL_INFO_URL, json=payload, timeout=timeout)
    r.raise_for_status()
    return r.json()


def fetch_candles(coin, interval, lookback_bars):
    """Hyperliquid candleSnapshot. Возвращает список dict t,o,h,l,c,v."""
    interval_ms = INTERVAL_MS[interval]
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - lookback_bars * interval_ms
    payload = {
        "type": "candleSnapshot",
        "req": {
            "coin": coin,
            "interval": interval,
            "startTime": start_ms,
            "endTime": end_ms,
        },
    }
    raw = _hl_post(payload)
    out = []
    for c in raw or []:
        try:
            out.append({
                "t": int(c["t"]),
                "o": float(c["o"]),
                "h": float(c["h"]),
                "l": float(c["l"]),
                "c": float(c["c"]),
                "v": float(c.get("v", 0.0)),
            })
        except (KeyError, TypeError, ValueError):
            continue
    out.sort(key=lambda x: x["t"])
    return out


def fetch_funding():
    """metaAndAssetCtxs → dict coin -> funding (float, fraction)."""
    try:
        data = _hl_post({"type": "metaAndAssetCtxs"})
    except Exception as e:
        print(f"[scanner] funding fetch error: {e}")
        return {}
    if not isinstance(data, list) or len(data) < 2:
        return {}
    meta, ctxs = data[0], data[1]
    universe = meta.get("universe", []) if isinstance(meta, dict) else []
    out = {}
    for i, ctx in enumerate(ctxs or []):
        if i >= len(universe):
            break
        name = universe[i].get("name")
        if not name:
            continue
        try:
            out[name] = float(ctx.get("funding", 0.0))
        except (TypeError, ValueError):
            out[name] = 0.0
    return out


# ---------------------------------------------------------- Indicators

def ema(values, period):
    n = len(values)
    if n < period:
        return [None] * n
    out = [None] * n
    seed = sum(values[:period]) / period
    out[period - 1] = seed
    k = 2 / (period + 1)
    for i in range(period, n):
        out[i] = values[i] * k + out[i - 1] * (1 - k)
    return out


def _true_range(candles, i):
    if i == 0:
        return candles[0]["h"] - candles[0]["l"]
    prev_c = candles[i - 1]["c"]
    h, l = candles[i]["h"], candles[i]["l"]
    return max(h - l, abs(h - prev_c), abs(l - prev_c))


def atr(candles, period=14):
    n = len(candles)
    if n < period + 1:
        return [None] * n
    out = [None] * n
    trs = [_true_range(candles, i) for i in range(n)]
    seed = sum(trs[1:period + 1]) / period
    out[period] = seed
    for i in range(period + 1, n):
        out[i] = (out[i - 1] * (period - 1) + trs[i]) / period
    return out


def rsi(closes, period=14):
    n = len(closes)
    if n < period + 1:
        return [None] * n
    out = [None] * n
    gains, losses = 0.0, 0.0
    for i in range(1, period + 1):
        d = closes[i] - closes[i - 1]
        if d >= 0:
            gains += d
        else:
            losses += -d
    avg_g = gains / period
    avg_l = losses / period
    if avg_l == 0:
        out[period] = 100.0
    else:
        rs = avg_g / avg_l
        out[period] = 100 - 100 / (1 + rs)
    for i in range(period + 1, n):
        d = closes[i] - closes[i - 1]
        g = max(d, 0.0)
        l = max(-d, 0.0)
        avg_g = (avg_g * (period - 1) + g) / period
        avg_l = (avg_l * (period - 1) + l) / period
        if avg_l == 0:
            out[i] = 100.0
        else:
            rs = avg_g / avg_l
            out[i] = 100 - 100 / (1 + rs)
    return out


# ---------------------------------------------------------- Regime / trend

def classify_regime_4h(candles_4h, atr_4h, ema20_4h, ema50_4h):
    """TREND / RANGE / CHAOS на 4H + направление."""
    if not candles_4h or ema20_4h[-1] is None or ema50_4h[-1] is None or atr_4h[-1] is None:
        return "UNKNOWN", None
    price = candles_4h[-1]["c"]
    if price <= 0:
        return "UNKNOWN", None
    atr_pct = atr_4h[-1] / price
    spread_pct = abs(ema20_4h[-1] - ema50_4h[-1]) / price
    direction = "UP" if ema20_4h[-1] > ema50_4h[-1] else "DOWN"
    if atr_pct > 0.04:
        return "CHAOS", direction
    if spread_pct > 0.015:
        return "TREND", direction
    return "RANGE", direction


def trend_1h(candles_1h, ema20_1h, ema50_1h):
    if ema20_1h[-1] is None or ema50_1h[-1] is None:
        return None
    return "UP" if ema20_1h[-1] > ema50_1h[-1] else "DOWN"


def atr_mode(coin, atr_1h_series, price):
    """NORMAL / HIGH / EXTREME — пороги по активу + 2x от среднего за 20 дней (480 баров 1H)."""
    if not atr_1h_series or atr_1h_series[-1] is None or price <= 0:
        return "UNKNOWN", None
    cur = atr_1h_series[-1]
    cur_pct = cur / price
    abs_thr = ATR_ABS_THRESHOLDS.get(coin, 0.02)
    # Средний ATR сравнения — без текущего последнего ATR (чтобы не "сам с собой").
    hist = atr_1h_series[-481:-1] if len(atr_1h_series) >= 2 else []
    valid = [v for v in hist[-480:] if v is not None]
    mean_atr = sum(valid) / len(valid) if valid else cur
    ratio = cur / mean_atr if mean_atr > 0 else 1.0
    if cur_pct > abs_thr * 1.5 or ratio > 2.5:
        mode = "EXTREME"
    elif cur_pct > abs_thr or ratio > 2.0:
        mode = "HIGH"
    else:
        mode = "NORMAL"
    return mode, cur_pct


def rsi_direction(rsi_series, lookback=3, eps=1.0):
    """UP/DOWN/FLAT за последние lookback баров — без порогов 70/30."""
    if not rsi_series or len(rsi_series) < lookback + 1:
        return "FLAT"
    last = rsi_series[-1]
    prev = rsi_series[-1 - lookback]
    if last is None or prev is None:
        return "FLAT"
    if last > prev + eps:
        return "UP"
    if last < prev - eps:
        return "DOWN"
    return "FLAT"


# ---------------------------------------------------------- Zone detection

def find_zones(candles_4h, atr_4h):
    """
    Зона = тело свечи противоположного цвета прямо перед импульсом
    (тело импульса >= 1.5 * ATR на 4H).
    """
    zones = []
    n = len(candles_4h)
    for i in range(1, n - 1):
        a = atr_4h[i] if i < len(atr_4h) else None
        if a is None or a <= 0:
            continue
        c = candles_4h[i]
        body = abs(c["c"] - c["o"])
        if body < 1.5 * a:
            continue
        prev = candles_4h[i - 1]
        impulse_up = c["c"] > c["o"]
        if impulse_up:
            if prev["c"] >= prev["o"]:
                continue
            zone_low = min(prev["o"], prev["c"])
            zone_high = max(prev["o"], prev["c"])
            direction = "LONG"
        else:
            if prev["c"] <= prev["o"]:
                continue
            zone_low = min(prev["o"], prev["c"])
            zone_high = max(prev["o"], prev["c"])
            direction = "SHORT"
        if zone_high - zone_low <= 0:
            continue

        retests = 0
        # Не считаем импульсную свечу и не считаем последнюю 4H-свечу,
        # потому что она может быть ещё не закрыта. Иначе текущий вход в зону
        # сразу ухудшает grade.
        for j in range(i + 1, n - 1):
            cj = candles_4h[j]
            if cj["l"] <= zone_high and cj["h"] >= zone_low:
                retests += 1

        zones.append({
            "low": float(zone_low),
            "high": float(zone_high),
            "direction": direction,
            # Зона сформирована на origin-свече, а не на импульсной свече.
            "formed_at_idx": i - 1,
            "impulse_at_idx": i,
            "impulse_body": float(body),
            "atr_at_form": float(a),
            "retests": retests,
        })
    return zones


def grade_zone(zone, candles_4h, ema20_4h, ema50_4h):
    """
    Бинарный грейд: A+ / B / None.

    4 критерия:
      1. Сильный импульс: тело >= 2 * ATR.
      2. Свежесть: 0 ретестов после формирования.
      3. Конфлюэнс с EMA20 или EMA50 на 4H в момент формирования.
      4. Структурный экстремум: low/high зоны == локальный экстремум
         за 20 предыдущих 4H баров.

    A+ = 4/4. B = ровно 3/4. Иначе drop.
    """
    score = 0

    if zone["impulse_body"] >= 2.0 * zone["atr_at_form"]:
        score += 1

    if zone["retests"] == 0:
        score += 1

    i = zone["formed_at_idx"]
    e20 = ema20_4h[i] if 0 <= i < len(ema20_4h) else None
    e50 = ema50_4h[i] if 0 <= i < len(ema50_4h) else None
    confluence = False
    for e in (e20, e50):
        if e is not None and zone["low"] <= e <= zone["high"]:
            confluence = True
            break
    if confluence:
        score += 1

    lookback = 20
    lo_idx = max(0, i - lookback)
    hi_idx = i
    if hi_idx > lo_idx:
        if zone["direction"] == "LONG":
            prior_low = min(c["l"] for c in candles_4h[lo_idx:hi_idx])
            if zone["low"] <= prior_low * 1.001:
                score += 1
        else:
            prior_high = max(c["h"] for c in candles_4h[lo_idx:hi_idx])
            if zone["high"] >= prior_high * 0.999:
                score += 1

    if score >= 4:
        return "A+", score
    if score == 3:
        return "B", score
    return None, score


def best_active_zone(zones, current_price):
    """Ближайшая валидная зона с правильной стороны от цены."""
    candidates = []
    for z in zones:
        if z["retests"] > 2:
            continue
        # LONG-зону ждём сверху-вниз (цена должна быть выше или внутри)
        if z["direction"] == "LONG" and current_price < z["low"] * 0.995:
            continue
        # SHORT-зону ждём снизу-вверх
        if z["direction"] == "SHORT" and current_price > z["high"] * 1.005:
            continue
        if current_price <= 0:
            continue
        # Выбираем ближайшую зону по расстоянию до ближайшей границы (или 0, если внутри).
        if z["low"] <= current_price <= z["high"]:
            dist = 0.0
        elif current_price < z["low"]:
            dist = z["low"] - current_price
        else:
            dist = current_price - z["high"]
        rel_dist = dist / current_price
        candidates.append((rel_dist, z))
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0])
    return candidates[0][1]


def _zone_key(coin, z):
    return f"{coin}:{z['direction']}:{round(z['low'], 4)}:{round(z['high'], 4)}"


def _zone_proximity(price, zone, atr_1h):
    """
    Возвращает (proximity, edge_dist, near_band).
      proximity: INSIDE | NEAR | FAR
      edge_dist: расстояние до ближайшей границы (None если INSIDE)
      near_band: 0.5 * ATR(1H) (None если atr_1h None)
    """
    if zone is None or price is None:
        return "FAR", None, None
    if zone["low"] <= price <= zone["high"]:
        return "INSIDE", None, (0.5 * atr_1h) if (atr_1h is not None and atr_1h > 0) else None
    if price < zone["low"]:
        edge_dist = zone["low"] - price
    else:
        edge_dist = price - zone["high"]
    near_band = (0.5 * atr_1h) if (atr_1h is not None and atr_1h > 0) else None
    if near_band is not None and edge_dist <= near_band:
        return "NEAR", edge_dist, near_band
    return "FAR", edge_dist, near_band


def evaluate_setup(asset_row):
    """
    Строгая система оценки для торгового протокола.
    Обязательные поля в результате:
      setup_grade: "A+" | "B" | "IGNORE"
      smart_status: "ACTION" | "READY" | "WATCH" | "IGNORE"
      reasons: list[str]
      priority_score: int
    """
    reasons = []
    best = asset_row.get("best_zone")
    price = asset_row.get("price")
    a_mode = asset_row.get("atr_mode")
    trend_aligned = bool(asset_row.get("trend_aligned"))
    rsi_dir = asset_row.get("rsi_dir")
    atr_1h = asset_row.get("atr_1h")

    has_zone = best is not None
    proximity, edge_dist, near_band = _zone_proximity(price, best, atr_1h)

    if not has_zone:
        reasons.append("no valid zone")
    if not trend_aligned:
        reasons.append("4H/1H mismatch")
    if a_mode == "EXTREME":
        reasons.append("ATR EXTREME")

    trade_dir = best["direction"] if has_zone else None
    rsi_ok = False
    if has_zone:
        if trade_dir == "LONG":
            rsi_ok = (rsi_dir == "UP")
        elif trade_dir == "SHORT":
            rsi_ok = (rsi_dir == "DOWN")
    if has_zone and rsi_dir == "FLAT":
        reasons.append("RSI FLAT")
    elif has_zone and not rsi_ok:
        reasons.append("RSI против сделки")

    level_score = int(best.get("score", 0)) if has_zone else 0
    impulse_strong = bool(has_zone and best.get("impulse_body") is not None and best.get("atr_at_form") and best["impulse_body"] >= 2.0 * best["atr_at_form"])
    zone_fresh = bool(has_zone and best.get("retests", 999) == 0)
    zone_trash = bool(has_zone and (best.get("retests", 0) > 2 or level_score < 3))

    if has_zone and zone_trash:
        reasons.append("zone trash")
    if has_zone and proximity == "FAR":
        reasons.append("zone far")

    near_or_inside = proximity in ("INSIDE", "NEAR")

    aplus_conditions = []
    if has_zone:
        aplus_conditions.append(("valid zone", True))
        aplus_conditions.append(("price near/inside zone", near_or_inside))
        aplus_conditions.append(("4H/1H align OK", trend_aligned))
        aplus_conditions.append(("ATR != EXTREME", a_mode != "EXTREME"))
        aplus_conditions.append(("RSI direction matches trade", rsi_ok and rsi_dir != "FLAT"))
        aplus_conditions.append(("zone fresh / not trash", zone_fresh and not zone_trash))
        aplus_conditions.append(("impulse strong", impulse_strong))
        aplus_conditions.append(("level_score >= 3", level_score >= 3))

    allow_b = (
        has_zone
        and near_or_inside
        and trend_aligned
        and a_mode != "EXTREME"
        and rsi_ok
        and rsi_dir != "FLAT"
        and not zone_trash
        and level_score >= 3
    )

    hard_ignore = (
        (not has_zone)
        or (not trend_aligned)
        or (a_mode == "EXTREME")
        or (has_zone and (rsi_dir == "FLAT" or not rsi_ok))
        or (has_zone and zone_trash)
    )

    setup_grade = "IGNORE"
    if has_zone and near_or_inside and (not hard_ignore):
        if all(ok for _, ok in aplus_conditions):
            setup_grade = "A+"
        elif allow_b:
            setup_grade = "B"

    smart_status = "IGNORE"
    if not has_zone:
        smart_status = "IGNORE"
    elif proximity == "INSIDE" and setup_grade in ("A+", "B"):
        smart_status = "ACTION"
    elif proximity == "NEAR" and setup_grade in ("A+", "B"):
        smart_status = "READY"
    elif has_zone and proximity == "FAR":
        smart_status = "WATCH"
    else:
        smart_status = "IGNORE"

    if setup_grade == "A+":
        reasons = [r for r in reasons if r not in ("zone far",)]
        if proximity == "NEAR" and edge_dist is not None and price:
            reasons.append(f"near zone (~{(edge_dist / price) * 100:.2f}%)")
        if proximity == "INSIDE":
            reasons.append("price inside zone")
    elif setup_grade == "B":
        missing = [name for name, ok in aplus_conditions if not ok]
        if missing:
            reasons.append("missing for A+: " + ", ".join(missing[:4]))

    base = 0
    if setup_grade == "A+":
        base = 80
    elif setup_grade == "B":
        base = 55
    if smart_status == "ACTION":
        base += 20
    elif smart_status == "READY":
        base += 10
    base += min(25, level_score * 5)
    if asset_row.get("funding_risk"):
        base -= 15
    if a_mode == "HIGH":
        base -= 10
    if a_mode == "EXTREME":
        base = 0
    priority_score = int(max(0, min(100, round(base))))

    if setup_grade not in SETUP_GRADES:
        setup_grade = "IGNORE"
    if smart_status not in SMART_STATUSES:
        smart_status = "IGNORE"

    coin = asset_row.get("coin")
    priority_rank = 0
    if setup_grade == "A+":
        if coin == "BTC":
            priority_rank = 1
        elif coin == "ETH":
            priority_rank = 2
        elif coin == "SOL":
            priority_rank = 3
    elif setup_grade == "B":
        if coin == "BTC":
            priority_rank = 4
        elif coin == "ETH":
            priority_rank = 5
        elif coin == "SOL":
            priority_rank = 6

    if priority_rank > 0 and coin in ("BTC", "ETH", "SOL"):
        priority_label = f"Priority #{priority_rank} — {coin} {setup_grade}"
    else:
        priority_label = "No priority — IGNORE"

    asset_row["setup_grade"] = setup_grade
    asset_row["smart_status"] = smart_status
    asset_row["reasons"] = reasons
    asset_row["priority_score"] = priority_score
    asset_row["priority_rank"] = int(priority_rank)
    asset_row["priority_label"] = priority_label
    asset_row["proximity"] = proximity
    asset_row["edge_dist"] = edge_dist
    asset_row["near_band"] = near_band
    return asset_row


# ---------------------------------------------------------- Per-asset

def analyse_asset(coin, fundings):
    candles_4h = fetch_candles(coin, "4h", 200)
    candles_1h_raw = fetch_candles(coin, "1h", 500)
    # Индикаторы считаем только по закрытым 1H свечам. Текущую цену берём из последней сырой.
    candles_1h = candles_1h_raw[:-1] if len(candles_1h_raw) >= 2 else []
    if len(candles_4h) < 60 or len(candles_1h) < 60:
        return None

    closes_4h = [c["c"] for c in candles_4h]
    closes_1h = [c["c"] for c in candles_1h]

    ema20_4h = ema(closes_4h, 20)
    ema50_4h = ema(closes_4h, 50)
    atr_4h_series = atr(candles_4h, 14)

    ema20_1h = ema(closes_1h, 20)
    ema50_1h = ema(closes_1h, 50)
    atr_1h_series = atr(candles_1h, 14)
    rsi_1h_series = rsi(closes_1h, 14)

    price = candles_1h_raw[-1]["c"]
    atr_1h = atr_1h_series[-1] if atr_1h_series else None

    regime_4h, dir_4h = classify_regime_4h(candles_4h, atr_4h_series, ema20_4h, ema50_4h)
    dir_1h = trend_1h(candles_1h, ema20_1h, ema50_1h)
    trend_aligned = (dir_4h is not None and dir_1h is not None and dir_4h == dir_1h)

    a_mode, atr_pct_1h = atr_mode(coin, atr_1h_series, price)
    rsi_val = rsi_1h_series[-1] if rsi_1h_series else None
    rsi_dir = rsi_direction(rsi_1h_series)

    funding = fundings.get(coin, 0.0)

    # Зоны на 4H + бинарный грейд
    raw_zones = find_zones(candles_4h, atr_4h_series)
    valid_zones = []
    for z in raw_zones:
        grade, score = grade_zone(z, candles_4h, ema20_4h, ema50_4h)
        if grade is None:
            continue

        z["grade"] = grade
        z["score"] = score
        valid_zones.append(z)

    best = best_active_zone(valid_zones, price)

    status = "no zone"
    distance_pct = None
    inside = False
    if best is not None:
        proximity, edge_dist, _near_band = _zone_proximity(price, best, atr_1h)
        if proximity == "INSIDE":
            status = "inside"
            inside = True
            distance_pct = 0.0
        elif proximity in ("NEAR", "FAR"):
            status = "near" if proximity == "NEAR" else "far"
            distance_pct = (edge_dist / price) if (edge_dist is not None and price and price > 0) else None

    funding_risk = False
    if best is not None:
        if best["direction"] == "LONG" and funding > FUNDING_LONG_RISK:
            funding_risk = True
        if best["direction"] == "SHORT" and funding < FUNDING_SHORT_RISK:
            funding_risk = True

    if best is None:
        if not trend_aligned:
            action = "wait — 4H/1H trend mismatch"
        else:
            action = "wait — no valid zone"
    elif a_mode == "EXTREME":
        action = "skip — ATR EXTREME"
    elif inside:
        action = f"react — price inside {best['grade']} zone {best['direction']}"
    else:
        dist_str = f"{(distance_pct * 100):.2f}%" if (distance_pct is not None) else "—"
        if status == "near":
            action = f"prep — {best['grade']} {best['direction']} {dist_str} away"
        else:
            action = f"watch — {best['grade']} {best['direction']} {dist_str} away"

    row = {
        "coin": coin,
        "price": price,
        "regime_4h": regime_4h,
        "dir_4h": dir_4h,
        "dir_1h": dir_1h,
        "trend_aligned": trend_aligned,
        "atr_mode": a_mode,
        "atr_pct_1h": atr_pct_1h,
        "atr_1h": atr_1h,
        "rsi_1h": rsi_val,
        "rsi_dir": rsi_dir,
        "funding": funding,
        "funding_risk": funding_risk,
        "best_zone": best,
        "status": status,
        "distance_pct": distance_pct,
        "inside": inside,
        "action": action,
    }
    return evaluate_setup(row)


# ---------------------------------------------------------- Reporting

def _fmt_price(x):
    if x is None:
        return "—"
    if x >= 1000:
        return f"{x:,.2f}"
    if x >= 10:
        return f"{x:.2f}"
    return f"{x:.4f}"


def format_report(rows):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [f"<b>Scanner v2 report</b>  <i>{now}</i>"]
    for r in rows:
        if r is None:
            continue
        setup = r.get("setup_grade", "IGNORE")
        smart = r.get("smart_status", "IGNORE")
        reasons = r.get("reasons") or []

        if r["best_zone"] is not None:
            z = r["best_zone"]
            zone_lines = [
                f"- {z['direction']} {_fmt_price(z['low'])}–{_fmt_price(z['high'])}",
                f"- level_score: {int(z.get('score', 0))}  retests: {int(z.get('retests', 0))}",
            ]
        else:
            zone_lines = ["- —"]

        if reasons:
            reasons_lines = "\n".join(f"- {x}" for x in reasons[:8])
        else:
            reasons_lines = "- —"

        lines.append(
            f"\n<b>{r['coin']}</b> @ {_fmt_price(r['price'])}\n"
            f"Setup: <b>{setup}</b>\n"
            f"Status: <b>{smart}</b>\n"
            f"Reasons:\n{reasons_lines}\n"
            f"Zone:\n" + "\n".join(zone_lines) + "\n"
            f"Priority: {r.get('priority_label', 'No priority — IGNORE')}"
        )
    return "\n".join(lines)


def send_telegram(token, chat_id, text):
    if not token or not chat_id:
        print(f"[scanner] skip telegram (no creds): {text[:80]}")
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        requests.post(url, json={
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }, timeout=10)
    except Exception as e:
        print(f"[scanner] telegram error: {e}")


# ---------------------------------------------------------- Webhook

def fire_zone_hit(self_webhook_url, asset_row):
    z = asset_row.get("best_zone")
    if z is None:
        return False
    payload = {
        # Контракт v1 — api.py принимает без правок
        "alert_type": "ZONE_HIT",
        "asset": asset_row["coin"],
        "direction": z["direction"],
        "zone_low": z["low"],
        "zone_high": z["high"],
        "entry_tf": "1H",
        "context_tf": "4H",
        "price": asset_row["price"],
        "trigger_price": asset_row["price"],
        # Расширения v2 — безопасно для api.py (он их не читает)
        "grade": asset_row.get("setup_grade", z.get("grade", "IGNORE")),
        "funding_risk": asset_row["funding_risk"],
        "rsi_dir": asset_row["rsi_dir"],
        "atr_mode": asset_row["atr_mode"],
        "priority_score": int(asset_row.get("priority_score", 0)),
        "reasons": asset_row.get("reasons") or [],
    }
    try:
        requests.post(self_webhook_url, json=payload, timeout=8)
        return True
    except Exception as e:
        print(f"[scanner] webhook fire error: {e}")
        return False


def log_signal(self_webhook_url, signal_type, asset_row):
    """
    Отправляет signal payload в app.py /log_signal.
    НЕ влияет на логику сигналов и не должен падать при ошибках.
    """
    z = asset_row.get("best_zone")
    if z is None:
        return False
    payload = {
        "type": signal_type,
        "asset": asset_row["coin"],
        "direction": z["direction"],
        "price": asset_row["price"],
        "zone_low": z["low"],
        "zone_high": z["high"],
        "setup_grade": asset_row.get("setup_grade", "IGNORE"),
        "smart_status": asset_row.get("smart_status", "IGNORE"),
        "priority_rank": int(asset_row.get("priority_rank", 0)),
        "priority_label": asset_row.get("priority_label", ""),
        "atr_mode": asset_row.get("atr_mode", ""),
        "rsi_dir": asset_row.get("rsi_dir", ""),
        "funding_risk": bool(asset_row.get("funding_risk", False)),
        "reasons": asset_row.get("reasons") or [],
    }
    url = self_webhook_url.rsplit("/", 1)[0] + "/log_signal"
    try:
        r = requests.post(url, json=payload, timeout=8)
        r.raise_for_status()
        print("LOG_SIGNAL SENT:", asset_row["coin"], signal_type)
        return True
    except Exception as e:
        print("[scanner] log signal error:", e)
        return False


# ---------------------------------------------------------- Main loop

def _scanner_loop(telegram_token, chat_id, self_webhook_url):
    print("[scanner] v2 loop started")
    # Первый отчёт сразу после старта — чтобы видеть, что сканер живой.
    with _state_lock:
        _state["last_report_at"] = 0.0

    while True:
        cycle_start = time.time()
        try:
            with _state_lock:
                warmup_done = bool(_state.get("warmup_done", False))

            fundings = fetch_funding()
            rows = []
            current_inside_zone_keys = set()
            current_near_zone_keys = set()
            for coin in ASSETS:
                try:
                    r = analyse_asset(coin, fundings)
                except Exception as e:
                    print(f"[scanner] {coin} analyse error: {e}")
                    r = None
                rows.append(r)

                if (
                    r
                    and r.get("best_zone") is not None
                ):
                    z = r["best_zone"]
                    zone_key = _zone_key(coin, z)
                    price = r.get("price")
                    inside = bool(price is not None and z["low"] <= price <= z["high"])
                    if inside:
                        current_inside_zone_keys.add(zone_key)

                    now = time.time()
                    with _state_lock:
                        last = _state["last_zone_hit_at"].get(coin, 0.0)
                        already_inside = zone_key in _state.get("inside_zone_keys", set())

                    # Сигнал только на ВХОДЕ в зону. Пока цена остаётся внутри — не спамим.
                    if inside and warmup_done and (not already_inside) and now - last >= ZONE_HIT_COOLDOWN_SEC:
                        log_signal(self_webhook_url, "ZONE_HIT", r)
                        ok = fire_zone_hit(self_webhook_url, r)
                        if ok:
                            print("WEBHOOK SENT:", coin, "ZONE HIT")
                            with _state_lock:
                                _state["last_zone_hit_at"][coin] = now
                            send_telegram(
                                telegram_token, chat_id,
                                (f"🔥 <b>ZONE HIT — смотри реакцию</b>\n"
                                 f"{coin} {z['direction']}\n"
                                 f"Setup: <b>{r.get('setup_grade', 'IGNORE')}</b>\n"
                                 f"{r.get('priority_label', 'No priority — IGNORE')}\n"
                                 f"Price: {_fmt_price(r.get('price'))}\n"
                                 f"Zone: {_fmt_price(z['low'])}–{_fmt_price(z['high'])}\n"
                                 f"ATR mode: {r.get('atr_mode', 'UNKNOWN')}\n"
                                 f"RSI dir: {r.get('rsi_dir', 'FLAT')}\n"
                                 f"Funding risk: {'YES ⚠️' if r.get('funding_risk') else 'NO'}\n"
                                 f"\n<b>Правило</b>:\n"
                                 f"- A+ только при реакции 3/3\n"
                                 f"- B максимум при реакции 2/3\n"
                                 f"- После реакции отправь: entry ... stop ... take ...\n"
                                 f"- План сделки бот НЕ открывает автоматически. После реакции отправь entry ... stop ... take ... — бот посчитает размер позиции, плечо, риск и RR.\n"
                                 f"- Бот проверит RR ≥ 2.3")
                            )

                # NEAR_ZONE: цена близко к зоне (<= 0.5 ATR(1H)), но НЕ внутри. Только Telegram, без webhook.
                if (
                    r
                    and r.get("smart_status") == "READY"
                    and r.get("setup_grade") in ("A+", "B")
                    and r.get("best_zone") is not None
                ):
                    z = r["best_zone"]
                    zone_key = _zone_key(coin, z)
                    current_near_zone_keys.add(zone_key)

                    now = time.time()
                    with _state_lock:
                        last = _state["last_near_zone_at"].get(coin, 0.0)
                        already_near = zone_key in _state.get("near_zone_keys", set())

                    # Сигнал только на "входе" в near-band + кулдаун
                    if warmup_done and (not already_near) and now - last >= NEAR_ZONE_COOLDOWN_SEC:
                        with _state_lock:
                            _state["last_near_zone_at"][coin] = now
                        price = r.get("price")
                        edge_dist = r.get("edge_dist")
                        near_band = r.get("near_band")
                        dist_pct = (edge_dist / price) if (edge_dist is not None and price and price > 0) else 0.0
                        log_signal(self_webhook_url, "NEAR_ZONE", r)
                        send_telegram(
                            telegram_token, chat_id,
                            (f"🟡 <b>NEAR ZONE — готовься, НЕ вход</b>\n"
                             f"{coin} {z['direction']}\n"
                             f"Setup: <b>{r.get('setup_grade', 'IGNORE')}</b>\n"
                             f"{r.get('priority_label', 'No priority — IGNORE')}\n"
                             f"Price: {_fmt_price(price)}\n"
                             f"Zone: {_fmt_price(z['low'])}–{_fmt_price(z['high'])}\n"
                             f"Distance: {_fmt_price(edge_dist)} (~{dist_pct * 100:.2f}%)\n"
                             f"ATR mode: {r.get('atr_mode', 'UNKNOWN')}\n"
                             f"RSI dir: {r.get('rsi_dir', 'FLAT')}\n"
                             f"Funding risk: {'YES ⚠️' if r.get('funding_risk') else 'NO'}\n"
                             f"\n<b>Правило</b>:\n"
                             f"- Вход запрещён. Жди ZONE HIT + реакцию.")
                        )

            with _state_lock:
                _state["inside_zone_keys"] = current_inside_zone_keys
                _state["near_zone_keys"] = current_near_zone_keys
                if not _state.get("warmup_done", False):
                    _state["warmup_done"] = True

            now = time.time()
            with _state_lock:
                last_report = _state["last_report_at"]
            if now - last_report >= REPORT_INTERVAL_SEC:
                msg = format_report(rows)
                send_telegram(telegram_token, chat_id, msg)
                with _state_lock:
                    _state["last_report_at"] = now

        except Exception as e:
            print(f"[scanner] loop error: {e}")

        elapsed = time.time() - cycle_start
        sleep_for = max(5.0, SCAN_INTERVAL_SEC - elapsed)
        time.sleep(sleep_for)


def start_scanner_thread(telegram_token, chat_id, self_webhook_url):
    """Запуск фонового потока. Сохранённая сигнатура — api.py не меняем."""
    with _state_lock:
        old_thread = _state.get("thread")
        if old_thread and old_thread.is_alive():
            print("[scanner] v2 thread already running")
            return old_thread

        t = threading.Thread(
            target=_scanner_loop,
            args=(telegram_token, chat_id, self_webhook_url),
            daemon=True,
            name="scanner-v2",
        )
        _state["thread"] = t
        t.start()
        return t


# ---------------------------------------------------------- CLI smoke test

if __name__ == "__main__":
    fundings = fetch_funding()
    rows = []
    for coin in ASSETS:
        r = analyse_asset(coin, fundings)
        rows.append(r)
        if r is None:
            print(f"{coin}: no data")
            continue
        printable = {k: v for k, v in r.items() if k != "best_zone"}
        print(json.dumps(printable, indent=2, default=str))
        if r["best_zone"]:
            print("BEST ZONE:", json.dumps(r["best_zone"], indent=2, default=str))
        print("---")
    print(format_report(rows))
