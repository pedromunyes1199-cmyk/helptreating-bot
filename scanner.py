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

SCAN_INTERVAL_SEC = 30 * 60          # цикл сканера — раз в 30 минут
REPORT_INTERVAL_SEC = 4 * 60 * 60    # сводный отчёт — раз в 4 часа
ZONE_HIT_COOLDOWN_SEC = 30 * 60      # кулдаун ZONE_HIT на актив

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
    "last_report_at": 0.0,
    # ключи зон, где цена уже была внутри на прошлом цикле;
    # нужно, чтобы ZONE HIT срабатывал именно на входе в зону, а не каждые 30 минут
    "inside_zone_keys": set(),
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
    valid = [v for v in atr_1h_series[-480:] if v is not None]
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
        for j in range(i + 2, max(i + 2, n - 1)):
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
        mid = (z["low"] + z["high"]) / 2
        dist = abs(current_price - mid) / current_price
        candidates.append((dist, z))
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0])
    return candidates[0][1]


# ---------------------------------------------------------- Per-asset

def analyse_asset(coin, fundings):
    candles_4h = fetch_candles(coin, "4h", 200)
    candles_1h = fetch_candles(coin, "1h", 500)
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

    price = closes_1h[-1]

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

        # CHAOS на 4H — не ищем сделку.
        if regime_4h == "CHAOS":
            continue

        # По протоколу HIGH ATR — только A+. EXTREME ниже блокируется в action и ZONE HIT.
        if a_mode == "HIGH" and grade != "A+":
            continue

        # Жёсткий фильтр: тренды должны совпадать
        if not trend_aligned:
            continue

        # Направление зоны должно совпадать с направлением выровненного тренда
        if z["direction"] == "LONG" and dir_4h != "UP":
            continue
        if z["direction"] == "SHORT" and dir_4h != "DOWN":
            continue

        # RSI должен идти строго в сторону сделки: rising для LONG, falling для SHORT.
        # FLAT не считается подтверждением.
        if z["direction"] == "LONG" and rsi_dir != "UP":
            continue
        if z["direction"] == "SHORT" and rsi_dir != "DOWN":
            continue

        z["grade"] = grade
        z["score"] = score
        valid_zones.append(z)

    best = best_active_zone(valid_zones, price)

    status = "no zone"
    distance_pct = None
    inside = False
    if best is not None:
        if best["low"] <= price <= best["high"]:
            status = "inside"
            inside = True
            distance_pct = 0.0
        else:
            mid = (best["low"] + best["high"]) / 2
            distance_pct = abs(price - mid) / price
            if distance_pct < 0.005:
                status = "near"
            elif distance_pct < 0.02:
                status = "approaching"
            else:
                status = "far"

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
    elif status == "near":
        action = f"prep — {best['grade']} {best['direction']} {distance_pct * 100:.2f}% away"
    else:
        action = f"watch — {best['grade']} {best['direction']} {distance_pct * 100:.2f}% away"

    return {
        "coin": coin,
        "price": price,
        "regime_4h": regime_4h,
        "dir_4h": dir_4h,
        "dir_1h": dir_1h,
        "trend_aligned": trend_aligned,
        "atr_mode": a_mode,
        "atr_pct_1h": atr_pct_1h,
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
        align = "OK" if r["trend_aligned"] else "MISMATCH"
        funding_str = f"{r['funding'] * 100:+.4f}%"
        if r["funding_risk"]:
            funding_str += " ⚠️"
        atr_str = r["atr_mode"]
        if r["atr_pct_1h"] is not None:
            atr_str += f" ({r['atr_pct_1h'] * 100:.2f}%)"
        if r["rsi_1h"] is not None:
            rsi_str = f"{r['rsi_1h']:.1f} {r['rsi_dir']}"
        else:
            rsi_str = "n/a"
        if r["best_zone"] is not None:
            z = r["best_zone"]
            zone_str = (
                f"{z['grade']} {z['direction']} "
                f"{_fmt_price(z['low'])}–{_fmt_price(z['high'])}"
            )
        else:
            zone_str = "—"

        lines.append(
            f"\n<b>{r['coin']}</b> @ {_fmt_price(r['price'])}\n"
            f"  4H: {r['regime_4h']}/{r['dir_4h']}  1H: {r['dir_1h']}  align: {align}\n"
            f"  ATR(1H): {atr_str}  RSI(1H): {rsi_str}\n"
            f"  Funding: {funding_str}\n"
            f"  Zone: {zone_str}\n"
            f"  Status: {r['status']}  →  {r['action']}"
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
        "grade": z["grade"],
        "funding_risk": asset_row["funding_risk"],
        "rsi_dir": asset_row["rsi_dir"],
        "atr_mode": asset_row["atr_mode"],
    }
    try:
        requests.post(self_webhook_url, json=payload, timeout=8)
        return True
    except Exception as e:
        print(f"[scanner] webhook fire error: {e}")
        return False


# ---------------------------------------------------------- Main loop

def _scanner_loop(telegram_token, chat_id, self_webhook_url):
    print("[scanner] v2 loop started")
    # Первый отчёт — через REPORT_INTERVAL_SEC после старта,
    # чтобы не спамить при каждом перезапуске Railway.
    with _state_lock:
        _state["last_report_at"] = time.time()

    while True:
        cycle_start = time.time()
        try:
            fundings = fetch_funding()
            rows = []
            current_inside_zone_keys = set()
            for coin in ASSETS:
                try:
                    r = analyse_asset(coin, fundings)
                except Exception as e:
                    print(f"[scanner] {coin} analyse error: {e}")
                    r = None
                rows.append(r)

                if (
                    r
                    and r.get("inside")
                    and r.get("best_zone") is not None
                    and r.get("atr_mode") != "EXTREME"
                ):
                    z = r["best_zone"]
                    zone_key = f"{coin}:{z['direction']}:{round(z['low'], 4)}:{round(z['high'], 4)}"
                    current_inside_zone_keys.add(zone_key)

                    now = time.time()
                    with _state_lock:
                        last = _state["last_zone_hit_at"].get(coin, 0.0)
                        already_inside = zone_key in _state.get("inside_zone_keys", set())

                    # Сигнал только на ВХОДЕ в зону. Пока цена остаётся внутри — не спамим.
                    if (not already_inside) and now - last >= ZONE_HIT_COOLDOWN_SEC:
                        ok = fire_zone_hit(self_webhook_url, r)
                        if ok:
                            with _state_lock:
                                _state["last_zone_hit_at"][coin] = now
                            send_telegram(
                                telegram_token, chat_id,
                                (f"🚨 <b>ZONE HIT</b> {coin} "
                                 f"{r['best_zone']['direction']} "
                                 f"@ {_fmt_price(r['price'])} "
                                 f"({r['best_zone']['grade']})"
                                 + (" ⚠️ funding" if r['funding_risk'] else ""))
                            )

            with _state_lock:
                _state["inside_zone_keys"] = current_inside_zone_keys

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
