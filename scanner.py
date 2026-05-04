"""
scanner.py v2 — Hyperliquid scanner for BTC / ETH / SOL.

Background thread inside api.py process. Same public contract as v1:
    start_scanner_thread(telegram_token, chat_id, self_webhook_url)

Same /webhook payload as v1 (api.py не трогаем). Дополнительные поля
(grade, funding_risk, rsi_dir, atr_mode) добавлены безопасно — api.py
их игнорирует, бот сможет начать использовать на следующей итерации.

После деплоя диагностического логирования (scanner_decisions.jsonl):
  1) Гонять сканер минимум 7 дней без смены фильтров.
  2) Собрать статистику: NEAR/INSIDE, сколько раз RSI и 4H/1H блокировали,
     долю FAR/WATCH.
  3) Только при подтверждении гипотезы — включать RSI_FILTER_MODE=zone_reaction
     (не по умолчанию).
  4) Новый RSI-режим: paper / ручная сверка 10–15 сигналов.
  5) Не убирать reaction-gate в app.py и не автоматизировать сделки.

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
# Hourly report в Telegram (Scanner v2 report). Event-based alerts не зависят от этого флага.
SEND_HOURLY_REPORTS = os.getenv("SEND_HOURLY_REPORTS", "true").lower() in ("1", "true", "yes")
# Антиспам Telegram/log_signal: не чаще одного раза на (symbol + zone + proximity) за окно минут (30–60).
def _proximity_alert_cooldown_sec():
    try:
        m = int(os.getenv("PROXIMITY_ALERT_COOLDOWN_MIN", "45"))
    except ValueError:
        m = 45
    m = max(30, min(60, m))
    return m * 60

DEBUG_ZONE_LOG = os.getenv("DEBUG_ZONE_LOG", "1") == "1"
DEBUG_ZONE_COOLDOWN_SEC = 30 * 60

# Автооценка реакции на 15m после ZONE_HIT — только наблюдение, не сигнал и не вход.
OBSERVATION_MODE = os.getenv("OBSERVATION_MODE", "false").lower() in ("1", "true", "yes")

# Авто-план (Telegram + расчёт) только при A+ и 3/3; для B — выключено по умолчанию.
AUTO_PLAN_FOR_B = os.getenv("AUTO_PLAN_FOR_B", "false").lower() in ("1", "true", "yes")

_active_reaction_watches: dict = {}
_reaction_watch_lock = threading.Lock()
_reaction_log_lock = threading.Lock()
_auto_plan_log_lock = threading.Lock()

# Буфер стопа для auto plan (не смешивать с зоновой логикой оценки сетапа)
_AUTO_PLAN_ATR_MIN = {
    "BTC": (0.5, 0.005),
    "ETH": (0.5, 0.005),
    "SOL": (0.8, 0.007),
}
RR_TARGET = 2.3


def _account_balance_usdc():
    raw = os.getenv("ACCOUNT_BALANCE", "").strip()
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _risk_pct_for_grade(setup_grade: str) -> float:
    if setup_grade == "A+":
        return 0.01
    if setup_grade == "B":
        return 0.005
    return 0.0


def _auto_plan_buffer(symbol, entry_ref, atr_1h):
    atr_mult, min_pct = _AUTO_PLAN_ATR_MIN.get(symbol, (0.5, 0.005))
    if atr_1h is None or atr_1h <= 0:
        return None
    return max(atr_mult * float(atr_1h), min_pct * float(entry_ref))


def compute_auto_plan_metrics(symbol, trade_dir, zone_low, zone_high, entry_ref, atr_1h):
    """
    Расчёт уровней auto plan (не торговый ордер).
    Возвращает dict с полями valid, entry_ref, stop_ref, min_tp_for_RR_2_3, risk_per_unit, buffer, rr_target.
    """
    zl, zh = float(zone_low), float(zone_high)
    entry_ref = float(entry_ref)
    buf = _auto_plan_buffer(symbol, entry_ref, atr_1h)
    out = {
        "valid": False,
        "entry_ref": entry_ref,
        "stop_ref": None,
        "min_tp_for_RR_2_3": None,
        "risk_per_unit": None,
        "buffer": buf,
        "rr_target": RR_TARGET,
    }
    if buf is None:
        return out

    if trade_dir == "SHORT":
        stop_ref = zh + buf
        risk_per_unit = stop_ref - entry_ref
        min_tp = entry_ref - risk_per_unit * RR_TARGET
        ok = stop_ref > entry_ref and min_tp < entry_ref and risk_per_unit > 0
        out.update(
            stop_ref=stop_ref,
            min_tp_for_RR_2_3=min_tp,
            risk_per_unit=risk_per_unit,
            valid=bool(ok),
        )
    elif trade_dir == "LONG":
        stop_ref = zl - buf
        risk_per_unit = entry_ref - stop_ref
        min_tp = entry_ref + risk_per_unit * RR_TARGET
        ok = stop_ref < entry_ref and min_tp > entry_ref and risk_per_unit > 0
        out.update(
            stop_ref=stop_ref,
            min_tp_for_RR_2_3=min_tp,
            risk_per_unit=risk_per_unit,
            valid=bool(ok),
        )
    return out


def add_sizing_to_plan(metrics: dict, setup_grade: str, account_balance_usdc):
    """Добавляет risk_usdc, position_size_usdc, estimated_leverage если задан баланс."""
    out = dict(metrics)
    out["risk_usdc"] = None
    out["position_size_usdc"] = None
    out["estimated_leverage"] = None
    out["account_balance_used"] = None
    if account_balance_usdc is None or account_balance_usdc <= 0:
        return out
    if not metrics.get("valid"):
        out["account_balance_used"] = account_balance_usdc
        return out
    rpct = _risk_pct_for_grade(setup_grade)
    if rpct <= 0:
        out["account_balance_used"] = account_balance_usdc
        return out
    risk_usdc = account_balance_usdc * rpct
    ru = metrics["risk_per_unit"]
    entry_ref = metrics["entry_ref"]
    if ru is None or ru <= 0:
        out["account_balance_used"] = account_balance_usdc
        return out
    pos = risk_usdc / ru * entry_ref
    lev = pos / account_balance_usdc
    out["risk_usdc"] = risk_usdc
    out["position_size_usdc"] = pos
    out["estimated_leverage"] = lev
    out["account_balance_used"] = account_balance_usdc
    return out


def _should_emit_auto_plan(w):
    if w.get("score", 0) < 3:
        return False
    sg = w.get("setup_grade", "IGNORE")
    if sg == "A+":
        return True
    if sg == "B" and AUTO_PLAN_FOR_B:
        return True
    return False


def _auto_plan_log_file_path():
    state_path = os.environ.get("STATE_PATH", "state.json")
    abs_state = os.path.abspath(state_path)
    parent = os.path.dirname(abs_state) or os.getcwd()
    return os.path.join(parent, "auto_plan_decisions.jsonl")


def append_auto_plan_jsonl(record: dict) -> None:
    line = json.dumps(record, ensure_ascii=False, default=str) + "\n"
    path = _auto_plan_log_file_path()
    with _auto_plan_log_lock:
        with open(path, "a", encoding="utf-8") as f:
            f.write(line)


def _print_auto_plan(record: dict) -> None:
    print("[auto_plan]", json.dumps(record, ensure_ascii=False, default=str))

# Конфиг фильтров (дефолты = текущее поведение v2; не меняют логику до явной смены env)
STRICT_TF_ALIGNMENT = os.getenv("STRICT_TF_ALIGNMENT", "true").lower() in ("1", "true", "yes")
RSI_FILTER_MODE = os.getenv("RSI_FILTER_MODE", "legacy").strip().lower()
RSI_FILTER_MODES_ALLOWED = ("legacy", "zone_reaction")
RSI_LOOKBACK_BARS = 3
RSI_LOOKBACK_EPS = 1.0


def _effective_rsi_filter_mode():
    m = RSI_FILTER_MODE
    return m if m in RSI_FILTER_MODES_ALLOWED else "legacy"

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
    # ключ: f"{coin}|{zone_key}|INSIDE" или "|NEAR" -> unix time последнего ZONE_HIT / NEAR_ZONE alert
    "last_proximity_alert_at": {},
    "last_debug_zone_at": {a: 0.0 for a in ASSETS},
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
_decision_log_lock = threading.Lock()


def _decision_log_file_path():
    """Рядом с state.json (STATE_PATH), как scanner_decisions.jsonl."""
    state_path = os.environ.get("STATE_PATH", "state.json")
    abs_state = os.path.abspath(state_path)
    parent = os.path.dirname(abs_state) or os.getcwd()
    return os.path.join(parent, "scanner_decisions.jsonl")


def append_decision_jsonl(record: dict) -> None:
    """Один объект JSON на строку; не использовать для USER_STATE signal_log."""
    line = json.dumps(record, ensure_ascii=False, default=str) + "\n"
    path = _decision_log_file_path()
    with _decision_log_lock:
        with open(path, "a", encoding="utf-8") as f:
            f.write(line)


def _print_decision(record: dict) -> None:
    print("[decision]", json.dumps(record, ensure_ascii=False, default=str))


def _reaction_log_file_path():
    state_path = os.environ.get("STATE_PATH", "state.json")
    abs_state = os.path.abspath(state_path)
    parent = os.path.dirname(abs_state) or os.getcwd()
    return os.path.join(parent, "reaction_decisions.jsonl")


def append_reaction_jsonl(record: dict) -> None:
    line = json.dumps(record, ensure_ascii=False, default=str) + "\n"
    path = _reaction_log_file_path()
    with _reaction_log_lock:
        with open(path, "a", encoding="utf-8") as f:
            f.write(line)


def _print_reaction(record: dict) -> None:
    print("[reaction]", json.dumps(record, ensure_ascii=False, default=str))


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


# ---------------------------------------------------------- Reaction observation (15m, OBSERVATION_MODE only)

REACTION_WINDOW_BARS = 4


def _candle_end_ms(c, interval="15m"):
    return int(c["t"]) + INTERVAL_MS[interval]


def _last_closed_candles_before(hit_ts_ms, candles, interval="15m"):
    """Свечи, у которых полное закрытие <= hit_ts_ms."""
    iv = INTERVAL_MS[interval]
    closed = [c for c in candles if _candle_end_ms(c, interval) <= hit_ts_ms]
    closed.sort(key=lambda x: x["t"])
    return closed


def _last_n_closed_before_hit(hit_ts_ms, candles, n=5, interval="15m"):
    closed = _last_closed_candles_before(hit_ts_ms, candles, interval)
    return closed[-n:] if len(closed) >= n else closed


def _first_four_closed_after_hit(hit_ts_ms, candles, now_ms, interval="15m"):
    """Первые четыре полностью закрытые свечи после hit (close > hit, close <= now)."""
    iv = INTERVAL_MS[interval]
    out = []
    for c in sorted(candles, key=lambda x: x["t"]):
        end = _candle_end_ms(c, interval)
        if end <= hit_ts_ms:
            continue
        if end > now_ms:
            continue
        out.append(c)
        if len(out) >= REACTION_WINDOW_BARS:
            break
    return out


def short_rejection_candle(c, zone_low):
    return c["h"] >= zone_low and c["c"] < zone_low


def long_rejection_candle(c, zone_high):
    return c["l"] <= zone_high and c["c"] > zone_high


def bearish_engulfing(prev, cur):
    if prev is None:
        return False
    po, pc = prev["o"], prev["c"]
    co, cc = cur["o"], cur["c"]
    if not (pc > po):
        return False
    if not (cc < co):
        return False
    if not (co >= pc):
        return False
    if not (cc <= po):
        return False
    return True


def bullish_engulfing(prev, cur):
    if prev is None:
        return False
    po, pc = prev["o"], prev["c"]
    co, cc = cur["o"], cur["c"]
    if not (pc < po):
        return False
    if not (cc > co):
        return False
    if not (co <= pc):
        return False
    if not (cc >= po):
        return False
    return True


def bearish_pin_bar(c):
    o, h, l, cl = c["o"], c["h"], c["l"], c["c"]
    if cl >= o:
        return False
    body = o - cl
    if body <= 0:
        return False
    upper_wick = h - o
    rng = h - l
    if rng <= 0:
        return False
    if upper_wick < 2 * body:
        return False
    third_boundary = l + rng / 3.0
    if o > third_boundary:
        return False
    return True


def bullish_pin_bar(c):
    o, h, l, cl = c["o"], c["h"], c["l"], c["c"]
    if cl <= o:
        return False
    body = cl - o
    if body <= 0:
        return False
    lower_wick = o - l
    rng = h - l
    if rng <= 0:
        return False
    if lower_wick < 2 * body:
        return False
    third_boundary = h - rng / 3.0
    if o < third_boundary:
        return False
    return True


def _short_trigger_pattern(prev, cur):
    if bearish_engulfing(prev, cur):
        return "bearish engulfing"
    if bearish_pin_bar(cur):
        return "bearish pin bar"
    return None


def _long_trigger_pattern(prev, cur):
    if bullish_engulfing(prev, cur):
        return "bullish engulfing"
    if bullish_pin_bar(cur):
        return "bullish pin bar"
    return None


def _apply_reaction_candle(w, prev, cur):
    """
    Обновляет criteria_hit / touch_seen / score. prev — предыдущая закрытая 15M (может быть до окна).
    Возвращает список названий паттернов триггера с этой свечи (для лога).
    """
    td = w["trade_dir"]
    zl, zh = w["zone_low"], w["zone_high"]
    patterns = []
    ch = w["criteria_hit"]

    if td == "SHORT":
        if short_rejection_candle(cur, zl):
            ch["rejection"] = True
        tp = _short_trigger_pattern(prev, cur)
        if tp:
            ch["trigger"] = True
            patterns.append(tp)
            w.setdefault("trigger_detail", tp)
        if cur["h"] >= zl:
            w["touch_seen"] = True
        if w["touch_seen"] and cur["c"] < w["pre_hit_local_low_5"]:
            ch["structure_break"] = True
    elif td == "LONG":
        if long_rejection_candle(cur, zh):
            ch["rejection"] = True
        tp = _long_trigger_pattern(prev, cur)
        if tp:
            ch["trigger"] = True
            patterns.append(tp)
            w.setdefault("trigger_detail", tp)
        if cur["l"] <= zh:
            w["touch_seen"] = True
        if w["touch_seen"] and cur["c"] > w["pre_hit_local_high_5"]:
            ch["structure_break"] = True

    w["score"] = sum(1 for v in ch.values() if v)
    return patterns


def _reaction_status_lines(w):
    ch = w["criteria_hit"]
    default_td = (
        "bullish engulfing / pin bar" if w["trade_dir"] == "LONG" else "bearish engulfing / pin bar"
    )
    td = w.get("trigger_detail") or default_td
    lines = [f"{'✓' if ch['rejection'] else '✗'} rejection"]
    if ch["trigger"]:
        lines.append(f"✓ trigger candle: {td}")
    else:
        lines.append("✗ trigger candle")
    lines.append(f"{'✓' if ch['structure_break'] else '✗'} structure break")
    return lines


def _build_reaction_log_record(
    w, watch_key, candle_end_ms, window_label, status, pattern_detected, structure_level, cur,
):
    ts = datetime.now(timezone.utc).isoformat()
    return {
        "timestamp": ts,
        "symbol": w["symbol"],
        "trade_dir": w["trade_dir"],
        "zone_low": w["zone_low"],
        "zone_high": w["zone_high"],
        "zone_hit_price": w.get("zone_hit_price"),
        "candle_time": candle_end_ms,
        "reaction_window": window_label,
        "criteria_hit": dict(w["criteria_hit"]),
        "score": w["score"],
        "status": status,
        "pattern_detected": pattern_detected,
        "structure_level": structure_level,
        "close": cur["c"],
        "high": cur["h"],
        "low": cur["l"],
        "watch_key": watch_key,
        "observation_only": True,
    }


def _send_reaction_telegram_confirmed(telegram_token, chat_id, w):
    sym = w["symbol"]
    td = w["trade_dir"]
    sc = w["score"]
    lines = _reaction_status_lines(w)
    text = (
        f"🧪 <b>AUTO REACTION CHECK — OBSERVATION ONLY</b>\n"
        f"{sym} {td}\n"
        f"Reaction score: {sc}/3\n"
        + "\n".join(lines)
        + "\n\nManual reaction-gate remains primary.\n"
        f"<b>НЕ вход.</b> Сравни с графиком."
    )
    send_telegram(telegram_token, chat_id, text)


def _send_reaction_telegram_failed(telegram_token, chat_id, w, reason_lines):
    sym = w["symbol"]
    td = w["trade_dir"]
    sc = w["score"]
    extra = "\n".join(reason_lines) if reason_lines else "—"
    text = (
        f"🧪 <b>AUTO REACTION FAILED — OBSERVATION ONLY</b>\n"
        f"{sym} {td}\n"
        f"Reaction score: {sc}/3\n"
        f"Причина: {extra}\n"
        f"<b>НЕ вход.</b>"
    )
    send_telegram(telegram_token, chat_id, text)


def _failure_reason_summary(w):
    miss = []
    ch = w["criteria_hit"]
    if not ch["rejection"]:
        miss.append("rejection")
    if not ch["trigger"]:
        miss.append("trigger candle")
    if not ch["structure_break"]:
        miss.append("structure break")
    if not miss:
        return "score ≤ 1 после 4 свечей"
    return "не было: " + " / ".join(miss)


def register_reaction_watch_on_zone_hit(coin, zone_dict, asset_row, hit_ts_ms):
    """Вызывается при ZONE_HIT; не дублирует watch по одному zone_key."""
    if not OBSERVATION_MODE:
        return
    wk = _zone_key(coin, zone_dict)
    with _reaction_watch_lock:
        if wk in _active_reaction_watches:
            return
    try:
        candles = fetch_candles(coin, "15m", 200)
    except Exception as e:
        print(f"[reaction] 15m fetch for watch: {e}")
        return
    pre5 = _last_n_closed_before_hit(hit_ts_ms, candles, 5)
    if len(pre5) < 5:
        print(f"[reaction] skip watch {wk}: need 5 closed 15m before hit, got {len(pre5)}")
        return
    pre_low = min(c["l"] for c in pre5)
    pre_high = max(c["h"] for c in pre5)
    with _reaction_watch_lock:
        if wk in _active_reaction_watches:
            return
        _active_reaction_watches[wk] = {
            "symbol": coin,
            "trade_dir": zone_dict["direction"],
            "zone_low": float(zone_dict["low"]),
            "zone_high": float(zone_dict["high"]),
            "hit_ts_ms": int(hit_ts_ms),
            "zone_hit_price": float(asset_row.get("price") or 0),
            "pre_hit_local_low_5": pre_low,
            "pre_hit_local_high_5": pre_high,
            "setup_grade": str(asset_row.get("setup_grade", "IGNORE")).upper().strip(),
            "atr_1h": asset_row.get("atr_1h"),
            "criteria_hit": {"rejection": False, "trigger": False, "structure_break": False},
            "touch_seen": False,
            "score": 0,
            "processed_ends": [],
            "trigger_detail": None,
            "sent_observation_confirmed": False,
            "sent_auto_plan": False,
            "observation_only": True,
        }


def process_reaction_watches(telegram_token, chat_id):
    """Каждый цикл сканера: новые закрытые 15m в окне, обновление критериев, финал."""
    if not OBSERVATION_MODE:
        return
    now_ms = int(time.time() * 1000)
    with _reaction_watch_lock:
        keys = list(_active_reaction_watches.keys())

    for wk in keys:
        with _reaction_watch_lock:
            w = _active_reaction_watches.get(wk)
        if not w:
            continue
        sym = w["symbol"]
        try:
            candles = fetch_candles(sym, "15m", 200)
        except Exception as e:
            print(f"[reaction] fetch 15m: {e}")
            continue

        hit_ms = w["hit_ts_ms"]
        last_before = _last_closed_candles_before(hit_ms, candles)
        prev_anchor = last_before[-1] if last_before else None

        window = _first_four_closed_after_hit(hit_ms, candles, now_ms)
        processed_ends = list(w["processed_ends"])

        for i, cur in enumerate(window):
            end_ms = _candle_end_ms(cur)
            if end_ms in processed_ends:
                continue
            prev = window[i - 1] if i > 0 else prev_anchor
            patterns = _apply_reaction_candle(w, prev, cur)
            processed_ends.append(end_ms)
            w["processed_ends"] = processed_ends

            window_label = f"{len(processed_ends)}/4"
            st_lvl = (
                w["pre_hit_local_low_5"] if w["trade_dir"] == "SHORT" else w["pre_hit_local_high_5"]
            )

            if len(processed_ends) >= REACTION_WINDOW_BARS and w["score"] <= 1:
                status = "OBSERVATION_ONLY_FAILED"
            else:
                status = "OBSERVATION_ONLY_TICK"

            rec = _build_reaction_log_record(
                w,
                wk,
                end_ms,
                window_label,
                status,
                ", ".join(patterns) if patterns else None,
                st_lvl,
                cur,
            )
            append_reaction_jsonl(rec)
            _print_reaction(rec)

            # Первое достижение score≥2: 🧪 observation (кроме A+ 3/3 — там только plan)
            if w["score"] >= 2 and not w.get("sent_observation_confirmed"):
                skip_obs = w["score"] == 3 and w.get("setup_grade") == "A+" and _should_emit_auto_plan(
                    w
                )
                if not skip_obs:
                    _send_reaction_telegram_confirmed(telegram_token, chat_id, w)
                w["sent_observation_confirmed"] = True

            plan_done = False
            if _should_emit_auto_plan(w) and not w.get("sent_auto_plan"):
                plan_done = bool(_emit_auto_plan_if_eligible(w, cur, wk, telegram_token, chat_id))

            if len(processed_ends) >= REACTION_WINDOW_BARS and w["score"] <= 1:
                reason = _failure_reason_summary(w)
                _send_reaction_telegram_failed(telegram_token, chat_id, w, [reason])
                with _reaction_watch_lock:
                    _active_reaction_watches.pop(wk, None)
                break

            if plan_done:
                with _reaction_watch_lock:
                    _active_reaction_watches.pop(wk, None)
                break

            if len(processed_ends) >= REACTION_WINDOW_BARS:
                with _reaction_watch_lock:
                    _active_reaction_watches.pop(wk, None)
                break


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


def _resolve_atr_1h_last(symbol, stored_atr):
    if stored_atr is not None:
        try:
            v = float(stored_atr)
            if v > 0:
                return v
        except (TypeError, ValueError):
            pass
    try:
        c1h = fetch_candles(symbol, "1h", 80)
        if len(c1h) < 20:
            return None
        s = atr(c1h, 14)
        if s and s[-1] is not None:
            return float(s[-1])
    except Exception as e:
        print(f"[auto_plan] ATR 1H fallback error: {e}")
    return None


def _emit_auto_plan_if_eligible(w, cur, wk, telegram_token, chat_id):
    """Telegram + auto_plan_decisions.jsonl; без ордеров и без app.py flow."""
    if not _should_emit_auto_plan(w) or w.get("sent_auto_plan"):
        return False
    sym = w["symbol"]
    sg = w.get("setup_grade", "IGNORE")
    atr_v = _resolve_atr_1h_last(sym, w.get("atr_1h"))
    entry_ref = float(cur["c"])
    m = compute_auto_plan_metrics(sym, w["trade_dir"], w["zone_low"], w["zone_high"], entry_ref, atr_v)
    bal = _account_balance_usdc()
    full = add_sizing_to_plan(m, sg, bal)

    ch = w["criteria_hit"]
    criteria_txt = (
        f"rejection: {'✓' if ch['rejection'] else '✗'}, "
        f"trigger: {'✓' if ch['trigger'] else '✗'}, "
        f"structure break: {'✓' if ch['structure_break'] else '✗'}"
    )

    if not full.get("valid"):
        rec = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "symbol": sym,
            "direction": w["trade_dir"],
            "setup_grade": sg,
            "reaction_score": w.get("score"),
            "entry_ref": full.get("entry_ref"),
            "stop_ref": full.get("stop_ref"),
            "min_tp_for_RR_2_3": full.get("min_tp_for_RR_2_3"),
            "risk_usdc": full.get("risk_usdc"),
            "position_size_usdc": full.get("position_size_usdc"),
            "estimated_leverage": full.get("estimated_leverage"),
            "account_balance_used": full.get("account_balance_used"),
            "status": "AUTO_PLAN_INVALID_GEOMETRY",
            "observation_only": True,
        }
        append_auto_plan_jsonl(rec)
        _print_auto_plan(rec)
        text = (
            f"✅ <b>AUTO REACTION CONFIRMED — PLAN READY</b>\n"
            f"{sym} {w['trade_dir']}\n"
            f"Setup: <b>{sg}</b> | Reaction score: <b>3/3</b>\n"
            f"{criteria_txt}\n\n"
            f"⚠️ Геометрия плана не прошла проверку (stop/tp). Перепроверь уровни на графике.\n\n"
            f"Это <b>НЕ</b> авто-вход. Бот не открыл сделку. Проверь график и открой руками, если согласен."
        )
        send_telegram(telegram_token, chat_id, text)
        w["sent_auto_plan"] = True
        return True

    risk_u = full.get("risk_usdc")
    pos_u = full.get("position_size_usdc")
    lev = full.get("estimated_leverage")
    bal_u = full.get("account_balance_used")

    rec_ok = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "symbol": sym,
        "direction": w["trade_dir"],
        "setup_grade": sg,
        "reaction_score": 3,
        "entry_ref": full["entry_ref"],
        "stop_ref": full["stop_ref"],
        "min_tp_for_RR_2_3": full["min_tp_for_RR_2_3"],
        "risk_usdc": risk_u,
        "position_size_usdc": pos_u,
        "estimated_leverage": lev,
        "account_balance_used": bal_u,
        "status": "AUTO_PLAN_READY_NOT_EXECUTED",
        "rr_target": RR_TARGET,
        "buffer": full.get("buffer"),
        "risk_per_unit": full.get("risk_per_unit"),
        "watch_key": wk,
        "observation_only": True,
    }
    append_auto_plan_jsonl(rec_ok)
    _print_auto_plan(rec_ok)

    sz_note = ""
    if bal_u is not None:
        sz_note = (
            f"\n<b>Risk (USDC)</b>: {_fmt_price(risk_u)}\n"
            f"<b>Position size (USDC)</b>: {_fmt_price(pos_u)}\n"
            f"<b>Est. leverage</b>: {lev:.2f}x\n"
            f"<b>Balance used</b>: {_fmt_price(bal_u)}"
        )
    else:
        sz_note = (
            "\n<i>Sizing не считался: задай ACCOUNT_BALANCE в env для risk/size/leverage.</i>"
        )

    text_ok = (
        f"✅ <b>AUTO REACTION CONFIRMED — PLAN READY</b>\n"
        f"{sym} {w['trade_dir']}\n"
        f"Setup: <b>{sg}</b> | Reaction score: <b>3/3</b>\n"
        f"{criteria_txt}\n\n"
        f"<b>entry_ref</b>: {_fmt_price(full['entry_ref'])}\n"
        f"<b>stop_ref</b>: {_fmt_price(full['stop_ref'])}\n"
        f"<b>min_tp (RR≥{RR_TARGET})</b>: {_fmt_price(full['min_tp_for_RR_2_3'])}\n"
        f"<b>RR</b>: {RR_TARGET} (по min_tp)\n"
        f"{sz_note}\n\n"
        f"⚠️ Ордер <b>НЕ</b> открыт. Открывай руками, если согласен.\n"
        f"Это <b>НЕ</b> авто-вход. Бот не открыл сделку. Проверь график и открой руками, если согласен."
    )
    send_telegram(telegram_token, chat_id, text_ok)
    w["sent_auto_plan"] = True
    return True


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


ZONE_REACTION_RSI_FAIL = "RSI reaction not confirmed yet"


def _legacy_rsi_ok(trade_dir, rsi_dir):
    """Текущее v2: LONG → UP, SHORT → DOWN."""
    if trade_dir == "LONG":
        return rsi_dir == "UP"
    if trade_dir == "SHORT":
        return rsi_dir == "DOWN"
    return False


def zone_reaction_rsi_detail(trade_dir, proximity, rsi_dir):
    """
    Будущий режим RSI_FILTER_MODE=zone_reaction (тесты + опциональное включение).
    NEAR: направление к зоне часто против «классического» импульса RSI.
    INSIDE: ждём подтверждения реакции (LONG не на сыром DOWN и т.д.).
    FAR: RSI не даёт «готовности» в этом режиме (NEAR/INSIDE отдельно).
    Возвращает (ok, reason_or_None).
    """
    if rsi_dir not in ("UP", "DOWN", "FLAT"):
        rsi_dir = "FLAT"
    if proximity == "FAR" or proximity is None:
        return False, None
    if trade_dir not in ("LONG", "SHORT"):
        return False, None
    if proximity == "NEAR":
        if trade_dir == "LONG":
            return (rsi_dir == "DOWN"), None
        return (rsi_dir == "UP"), None
    if proximity == "INSIDE":
        if trade_dir == "LONG":
            if rsi_dir in ("UP", "FLAT"):
                return True, None
            return False, ZONE_REACTION_RSI_FAIL
        if rsi_dir in ("DOWN", "FLAT"):
            return True, None
        return False, ZONE_REACTION_RSI_FAIL
    return False, None


def zone_reaction_rsi_ok(trade_dir, proximity, rsi_dir):
    ok, _ = zone_reaction_rsi_detail(trade_dir, proximity, rsi_dir)
    return ok


def _rsi_ok_for_mode(trade_dir, proximity, rsi_dir, mode):
    if mode == "zone_reaction":
        return zone_reaction_rsi_ok(trade_dir, proximity, rsi_dir)
    return _legacy_rsi_ok(trade_dir, rsi_dir)


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


def zone_score_breakdown(zone, candles_4h, ema20_4h, ema50_4h):
    """Те же 4 критерия, что grade_zone — для диагностики и отчётов."""
    impulse_strong = zone["impulse_body"] >= 2.0 * zone["atr_at_form"]
    fresh_zero_retests = zone["retests"] == 0
    i = zone["formed_at_idx"]
    e20 = ema20_4h[i] if 0 <= i < len(ema20_4h) else None
    e50 = ema50_4h[i] if 0 <= i < len(ema50_4h) else None
    ema_confluence = False
    for e in (e20, e50):
        if e is not None and zone["low"] <= e <= zone["high"]:
            ema_confluence = True
            break
    lookback = 20
    lo_idx = max(0, i - lookback)
    hi_idx = i
    structural_extreme = False
    if hi_idx > lo_idx:
        if zone["direction"] == "LONG":
            prior_low = min(c["l"] for c in candles_4h[lo_idx:hi_idx])
            structural_extreme = zone["low"] <= prior_low * 1.001
        else:
            prior_high = max(c["h"] for c in candles_4h[lo_idx:hi_idx])
            structural_extreme = zone["high"] >= prior_high * 0.999
    return {
        "impulse_strong": impulse_strong,
        "fresh_zero_retests": fresh_zero_retests,
        "ema_confluence": ema_confluence,
        "structural_extreme": structural_extreme,
    }


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


def _proximity_alert_spam_key(coin, zone_key, proximity_kind):
    """Стабильный ключ для антиспама: символ + зона + NEAR|INSIDE (как в proximity)."""
    return f"{coin}|{zone_key}|{proximity_kind}"


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


def _bars_1h_in_direction(candles_1h, ema20_1h, ema50_1h, dir_1h):
    """Подряд закрытых 1H баров, где EMA20/EMA50 согласованы с dir_1h."""
    if dir_1h not in ("UP", "DOWN") or not candles_1h:
        return 0
    n = 0
    for i in range(len(candles_1h) - 1, -1, -1):
        e20 = ema20_1h[i] if i < len(ema20_1h) else None
        e50 = ema50_1h[i] if i < len(ema50_1h) else None
        if e20 is None or e50 is None:
            break
        up = e20 > e50
        if dir_1h == "UP" and up:
            n += 1
        elif dir_1h == "DOWN" and not up:
            n += 1
        else:
            break
    return n


def _pullback_context_simple(dir_4h, dir_1h, candles_1h):
    """Только для логов; не вход."""
    if len(candles_1h) < 4:
        return None
    if dir_4h == "UP" and dir_1h == "DOWN":
        last = candles_1h[-1]
        if last["c"] <= last["o"]:
            return None
        bear_run = 0
        for i in range(len(candles_1h) - 2, -1, -1):
            c = candles_1h[i]
            if c["c"] < c["o"]:
                bear_run += 1
            else:
                break
        if bear_run >= 1:
            return "possible_bullish_pullback_reaction"
    elif dir_4h == "DOWN" and dir_1h == "UP":
        last = candles_1h[-1]
        if last["c"] >= last["o"]:
            return None
        bull_run = 0
        for i in range(len(candles_1h) - 2, -1, -1):
            c = candles_1h[i]
            if c["c"] > c["o"]:
                bull_run += 1
            else:
                break
        if bull_run >= 1:
            return "possible_bearish_pullback_reaction"
    return None


def _finalize_decision_diagnostics(asset_row):
    """reject_reasons / decision_state / raw_reason_flags — не для расширения smart_status."""
    best = asset_row.get("best_zone")
    has_zone = best is not None
    prox = asset_row.get("proximity", "FAR")
    ss = asset_row.get("smart_status", "IGNORE")
    ta = bool(asset_row.get("trend_aligned"))
    am = asset_row.get("atr_mode")
    rsi_dir = asset_row.get("rsi_dir") or "FLAT"
    diag = asset_row.get("_scanner_diag") or {}
    rsi_ok = bool(diag.get("rsi_ok"))
    zone_trash = bool(diag.get("zone_trash"))

    reject = []
    if not has_zone:
        reject.append("no valid zone")
    if has_zone and not ta:
        reject.append("4H/1H mismatch")
    if am == "EXTREME":
        reject.append("ATR EXTREME")
    if has_zone and rsi_dir == "FLAT":
        reject.append("RSI FLAT")
    elif has_zone and not rsi_ok:
        reject.append("RSI против сделки")
    if has_zone and zone_trash:
        reject.append("zone trash")

    flags = {
        "no_zone": not has_zone,
        "tf_mismatch": has_zone and not ta,
        "atr_extreme": am == "EXTREME",
        "rsi_flat": has_zone and rsi_dir == "FLAT",
        "rsi_against": has_zone and rsi_dir != "FLAT" and not rsi_ok,
        "zone_trash": has_zone and zone_trash,
        "proximity_far": has_zone and prox == "FAR",
    }

    if not has_zone:
        dst = "NO_VALID_ZONE"
    elif not ta:
        dst = "TF_ALIGNMENT_BLOCK"
    elif am == "EXTREME":
        dst = "ATR_EXTREME_BLOCK"
    elif zone_trash:
        dst = "ZONE_QUALITY_BLOCK"
    elif prox == "FAR" and ss == "WATCH":
        dst = "WATCH_ZONE"
    elif prox == "FAR":
        dst = "FAR_FROM_ZONE"
    elif ss == "ACTION":
        dst = "ACTION_READY"
    elif ss == "READY":
        dst = "SETUP_READY"
    elif prox == "INSIDE" and ss == "IGNORE":
        dst = "INSIDE_ZONE_REACTION_PENDING"
    elif prox == "NEAR" and ss == "IGNORE":
        if rsi_dir == "FLAT" or not rsi_ok:
            dst = "RSI_BLOCK"
        else:
            dst = "NEAR_ZONE"
    else:
        dst = "IGNORE_OTHER"

    asset_row["reject_reasons"] = reject
    asset_row["raw_reason_flags"] = flags
    asset_row["decision_state"] = dst


def build_decision_record(asset_row):
    """Одна строка JSONL на актив за цикл."""
    if asset_row is None:
        return None
    z = asset_row.get("best_zone") or {}
    edge = asset_row.get("edge_dist")
    price = asset_row.get("price")
    prox = asset_row.get("proximity")
    dist_pct = None
    if prox == "INSIDE":
        dist_pct = 0.0
    elif edge is not None and price:
        try:
            dist_pct = (float(edge) / float(price)) * 100.0
        except (TypeError, ValueError, ZeroDivisionError):
            dist_pct = None
    ts = datetime.now(timezone.utc).isoformat()
    diag = asset_row.get("_scanner_diag") or {}
    return {
        "timestamp": ts,
        "symbol": asset_row.get("coin"),
        "price": price,
        "trade_dir": z.get("direction"),
        "zone_low": z.get("low"),
        "zone_high": z.get("high"),
        "proximity": prox,
        "distance_pct": dist_pct,
        "near_band": asset_row.get("near_band"),
        "level_score": int(z.get("score", 0)) if z else 0,
        "setup_grade": asset_row.get("setup_grade"),
        "score_breakdown": asset_row.get("score_breakdown"),
        "rsi_value": asset_row.get("rsi_1h"),
        "rsi_dir": asset_row.get("rsi_dir"),
        "rsi_lookback": asset_row.get("rsi_lookback"),
        "rsi_delta": asset_row.get("rsi_delta"),
        "rsi_ok": diag.get("rsi_ok"),
        "atr_1h": asset_row.get("atr_1h"),
        "dir_4h": asset_row.get("dir_4h"),
        "dir_1h": asset_row.get("dir_1h"),
        "regime_4h": asset_row.get("regime_4h"),
        "trend_aligned": asset_row.get("trend_aligned"),
        "bars_1h_in_direction": asset_row.get("bars_1h_in_direction"),
        "ema20_1h": asset_row.get("ema20_1h"),
        "ema50_1h": asset_row.get("ema50_1h"),
        "ema20_4h": asset_row.get("ema20_4h"),
        "ema50_4h": asset_row.get("ema50_4h"),
        "pullback_context": asset_row.get("pullback_context"),
        "funding": asset_row.get("funding"),
        "funding_risk": asset_row.get("funding_risk"),
        "smart_status": asset_row.get("smart_status"),
        "decision_state": asset_row.get("decision_state"),
        "reject_reasons": asset_row.get("reject_reasons"),
        "raw_reason_flags": asset_row.get("raw_reason_flags"),
        "strict_tf_alignment": STRICT_TF_ALIGNMENT,
        "rsi_filter_mode": _effective_rsi_filter_mode(),
        "rsi_filter_mode_raw": RSI_FILTER_MODE,
    }


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
        rsi_ok = _rsi_ok_for_mode(trade_dir, proximity, rsi_dir, _effective_rsi_filter_mode())
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
    asset_row["_scanner_diag"] = {
        "rsi_ok": bool(rsi_ok),
        "zone_trash": bool(zone_trash),
        "hard_ignore": bool(hard_ignore),
        "near_or_inside": bool(near_or_inside),
        "allow_b": bool(allow_b),
    }
    _finalize_decision_diagnostics(asset_row)
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
    rsi_dir = rsi_direction(rsi_1h_series, lookback=RSI_LOOKBACK_BARS, eps=RSI_LOOKBACK_EPS)

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

    rsi_delta = None
    if rsi_1h_series and len(rsi_1h_series) >= RSI_LOOKBACK_BARS + 1:
        a, b = rsi_1h_series[-1], rsi_1h_series[-1 - RSI_LOOKBACK_BARS]
        if a is not None and b is not None:
            rsi_delta = a - b

    score_breakdown = zone_score_breakdown(best, candles_4h, ema20_4h, ema50_4h) if best else None
    bars_1h_in_direction = _bars_1h_in_direction(candles_1h, ema20_1h, ema50_1h, dir_1h)
    pullback_context = _pullback_context_simple(dir_4h, dir_1h, candles_1h)

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
        "rsi_lookback": RSI_LOOKBACK_BARS,
        "rsi_delta": rsi_delta,
        "funding": funding,
        "funding_risk": funding_risk,
        "best_zone": best,
        "status": status,
        "distance_pct": distance_pct,
        "inside": inside,
        "action": action,
        "score_breakdown": score_breakdown,
        "ema20_1h": ema20_1h[-1] if ema20_1h else None,
        "ema50_1h": ema50_1h[-1] if ema50_1h else None,
        "ema20_4h": ema20_4h[-1] if ema20_4h else None,
        "ema50_4h": ema50_4h[-1] if ema50_4h else None,
        "bars_1h_in_direction": bars_1h_in_direction,
        "pullback_context": pullback_context,
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
        reasons = list(r.get("reasons") or [])
        # WATCH: «зона далеко» не как отказ — только косметика отображения
        if smart == "WATCH":
            reasons = [x for x in reasons if x != "zone far"]

        if r["best_zone"] is not None:
            z = r["best_zone"]
            zone_lines = [
                f"- {z['direction']} {_fmt_price(z['low'])}–{_fmt_price(z['high'])}",
                f"- level_score: {int(z.get('score', 0))}  retests: {int(z.get('retests', 0))}",
            ]
        else:
            zone_lines = ["- —"]

        rej = r.get("reject_reasons") or []
        if smart == "WATCH" and r.get("best_zone") is not None:
            chunks = ["<i>Состояние: зона есть, цена далеко — ждём подхода</i>"]
            if rej:
                chunks.append("\n".join(f"- {x}" for x in rej[:8]))
            for x in reasons[:8]:
                if x not in rej:
                    chunks.append(f"- {x}")
            reasons_lines = "\n".join(chunks) if chunks else "- —"
        else:
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

                if r is None:
                    rec = {
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "symbol": coin,
                        "price": None,
                        "smart_status": "IGNORE",
                        "decision_state": "NO_DATA",
                        "reject_reasons": ["no data (candles/indicators)"],
                        "rsi_filter_mode": _effective_rsi_filter_mode(),
                        "rsi_filter_mode_raw": RSI_FILTER_MODE,
                        "strict_tf_alignment": STRICT_TF_ALIGNMENT,
                    }
                else:
                    rec = build_decision_record(r)
                append_decision_jsonl(rec)
                _print_decision(rec)

                if (
                    DEBUG_ZONE_LOG
                    and r
                    and r.get("best_zone") is not None
                ):
                    z_dbg = r["best_zone"]
                    px = r.get("price")
                    atr_h = r.get("atr_1h")
                    prox_dbg, _, _ = _zone_proximity(px, z_dbg, atr_h)
                    if prox_dbg in ("INSIDE", "NEAR"):
                        dbg_now = time.time()
                        with _state_lock:
                            last_dbg = _state["last_debug_zone_at"].get(coin, 0.0)
                        if dbg_now - last_dbg >= DEBUG_ZONE_COOLDOWN_SEC:
                            reasons_dbg = r.get("reasons") or []
                            if reasons_dbg:
                                reasons_txt = "\n".join(f"- {x}" for x in reasons_dbg[:12])
                            else:
                                reasons_txt = "- —"
                            send_telegram(
                                telegram_token,
                                chat_id,
                                (
                                    f"🧪 <b>DEBUG ZONE — НЕ ВХОД</b>\n"
                                    f"{coin} {z_dbg['direction']}\n"
                                    f"Price: {_fmt_price(px)}\n"
                                    f"Zone: {_fmt_price(z_dbg['low'])}–{_fmt_price(z_dbg['high'])}\n"
                                    f"proximity: <b>{prox_dbg}</b>\n"
                                    f"setup_grade: <b>{r.get('setup_grade', 'IGNORE')}</b>\n"
                                    f"smart_status: <b>{r.get('smart_status', 'IGNORE')}</b>\n"
                                    f"reasons:\n{reasons_txt}\n"
                                    f"atr_mode: {r.get('atr_mode', 'UNKNOWN')}\n"
                                    f"rsi_dir: {r.get('rsi_dir', 'FLAT')}\n"
                                    f"funding_risk: {'YES ⚠️' if r.get('funding_risk') else 'NO'}\n"
                                    f"\n<b>Правило</b>: это не торговый сигнал, "
                                    f"только проверка касаний зоны."
                                ),
                            )
                            with _state_lock:
                                _state["last_debug_zone_at"][coin] = dbg_now

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
                    cd = _proximity_alert_cooldown_sec()
                    spam_key_inside = _proximity_alert_spam_key(coin, zone_key, "INSIDE")
                    with _state_lock:
                        already_inside = zone_key in _state.get("inside_zone_keys", set())
                        last_inside_spam = _state["last_proximity_alert_at"].get(spam_key_inside, 0.0)

                    # Сигнал только на ВХОДЕ в зону (FAR/NEAR -> INSIDE). Пока цена внутри — не спамим.
                    # Плюс кулдаун на пару (symbol, zone, INSIDE), чтобы не дублировать чаще 30–60 мин.
                    if (
                        inside
                        and warmup_done
                        and (not already_inside)
                        and (now - last_inside_spam >= cd)
                    ):
                        hit_ts_ms = int(time.time() * 1000)
                        log_signal(self_webhook_url, "ZONE_HIT", r)
                        with _state_lock:
                            _state["last_proximity_alert_at"][spam_key_inside] = now
                        register_reaction_watch_on_zone_hit(coin, z, r, hit_ts_ms)
                        ok = fire_zone_hit(self_webhook_url, r)
                        if ok:
                            print("WEBHOOK SENT:", coin, "ZONE HIT")
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

                # NEAR_ZONE: proximity NEAR (<= 0.5 ATR до границы), не внутри зоны. Только Telegram, без webhook.
                if (
                    r
                    and r.get("smart_status") == "READY"
                    and r.get("setup_grade") in ("A+", "B")
                    and r.get("best_zone") is not None
                    and r.get("proximity") == "NEAR"
                ):
                    z = r["best_zone"]
                    zone_key = _zone_key(coin, z)
                    current_near_zone_keys.add(zone_key)

                    now = time.time()
                    cd = _proximity_alert_cooldown_sec()
                    spam_key_near = _proximity_alert_spam_key(coin, zone_key, "NEAR")
                    with _state_lock:
                        already_near = zone_key in _state.get("near_zone_keys", set())
                        last_near_spam = _state["last_proximity_alert_at"].get(spam_key_near, 0.0)

                    # Сигнал на первом цикле в NEAR после выхода из «near-состояния» + кулдаун (symbol, zone, NEAR).
                    if warmup_done and (not already_near) and (now - last_near_spam >= cd):
                        with _state_lock:
                            _state["last_proximity_alert_at"][spam_key_near] = now
                        price = r.get("price")
                        edge_dist = r.get("edge_dist")
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

            process_reaction_watches(telegram_token, chat_id)

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
                if SEND_HOURLY_REPORTS:
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
