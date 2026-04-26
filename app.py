from flask import Flask, request, jsonify
import requests
import re
import json
import os
import time
import threading
from datetime import date, datetime, timezone, timedelta

app = Flask(__name__)

# ===== КОНФИГ =====
# ВАЖНО: токен и chat_id лучше держать в переменных окружения Railway,
# а не в коде. Если этот файл попадёт в публичный репозиторий — токен
# нужно будет немедленно отозвать через @BotFather (/revoke).
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

if not TELEGRAM_TOKEN:
    raise RuntimeError("TELEGRAM_TOKEN не задан. Добавь его в Railway → Variables.")
if not CHAT_ID:
    raise RuntimeError("CHAT_ID не задан. Добавь его в Railway → Variables.")

DEFAULT_BALANCE = 15000.0

SUPPORTED_ASSETS = {"BTC", "ETH", "SOL"}

MAX_LEV  = {"BTC": 4, "ETH": 4, "SOL": 3}
STOP_MIN = {"BTC": 0.2, "ETH": 0.2, "SOL": 0.3}
STOP_MAX = {"BTC": 2.5, "ETH": 3.0, "SOL": 4.0}

DAY_LOSS_LIM   = -0.02
DAY_R_LIM      =  3.0
MAX_TRADES     =  3
COOLDOWN_MIN   =  15        # минут между алертами по одному активу

# Торговые окна (UTC): Лондон 08:00-11:00, Нью-Йорк 13:30-16:30
SESSIONS_UTC   = [(8*60, 11*60), (13*60+30, 16*60+30)]

# TODO: временное решение — заменить на Upstash Redis или Railway Volume
# ✅ переживает рестарт процесса  ❌ не гарантировано при смене инстанса
STATE_FILE      = "state.json"
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "")

# ═══════════════════════════════════════════════════════════════════
#  ХРАНИЛИЩЕ
# ═══════════════════════════════════════════════════════════════════
USER_STATE: dict = {}
# RLock — чтобы handlers могли захватывать его и потом вызывать save_state(),
# который сам захватывает лок повторно (для снапшота).
STATE_LOCK = threading.RLock()


def load_state_from_disk():
    global USER_STATE
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, encoding="utf-8") as f:
                USER_STATE = json.load(f)
        except Exception as e:
            print("State load error:", e)


def save_state():
    """
    Снимок словаря под локом → запись на диск вне лока.
    Это убирает 'dictionary changed size during iteration' под gunicorn-threads
    и не держит лок на время дискового I/O.
    """
    try:
        with STATE_LOCK:
            snapshot = json.loads(json.dumps(USER_STATE, ensure_ascii=False))
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(snapshot, f, ensure_ascii=False, indent=2)
        os.replace(tmp, STATE_FILE)   # атомарная замена — файл не может стать пустым
    except Exception as e:
        print("State save error:", e)


def ensure_user_state(chat_id: str):
    today = str(date.today())
    if chat_id not in USER_STATE:
        USER_STATE[chat_id] = {}
    u = USER_STATE[chat_id]
    # Сброс дневных счётчиков при смене дня
    # consecutive_stops НЕ сбрасывается — серия переходит через ночь
    # active_trade НЕ сбрасывается — сделка может жить через полночь
    if u.get("day_date") != today:
        u["day_date"]        = today
        u["daily_stops"]     = 0
        u["daily_trades"]    = 0
        u["daily_r"]         = 0.0
        u["daily_pnl_pct"]   = 0.0
        save_state()
    u.setdefault("consecutive_stops", 0)
    u.setdefault("account_balance",   DEFAULT_BALANCE)
    u.setdefault("active_trade",      None)
    u.setdefault("paused",            False)
    u.setdefault("flow_step",         None)
    u.setdefault("last_alert",        None)
    u.setdefault("last_alert_ts",     {})   # {asset: timestamp}
    u.setdefault("reaction",          {})
    u.setdefault("gate",              {})
    u.setdefault("side",              None)
    u.setdefault("asset",             None)
    u.setdefault("grade",             None)
    u.setdefault("journal_draft",     None)
    u.setdefault("trade_history",     [])


def get_user(chat_id: str) -> dict:
    # Лок здесь обязателен: ensure_user_state мутирует USER_STATE
    # (создаёт ключ + setdefault'ы) — без лока возможна гонка с reminder_loop
    # и другими хендлерами.
    with STATE_LOCK:
        ensure_user_state(chat_id)
        return USER_STATE[chat_id]


# ═══════════════════════════════════════════════════════════════════
#  TELEGRAM API
# ═══════════════════════════════════════════════════════════════════
def tg_api(method: str, payload: dict):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/{method}"
    try:
        return requests.post(url, json=payload, timeout=10).json()
    except Exception as e:
        print("Telegram API error:", e)
        return None


def send_message(chat_id: str, text: str, reply_markup: dict = None):
    payload = {"chat_id": chat_id, "text": text}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    return tg_api("sendMessage", payload)


def answer_callback(cb_id: str, text: str = ""):
    return tg_api("answerCallbackQuery", {"callback_query_id": cb_id, "text": text})


def format_price(v) -> str:
    try:
        return f"{float(v):.2f}"
    except Exception:
        return str(v)


# ═══════════════════════════════════════════════════════════════════
#  УТИЛИТЫ: СЕССИЯ, КУЛДАУН, БЛОК ДНЯ
# ═══════════════════════════════════════════════════════════════════
def in_trading_session() -> bool:
    now = datetime.now(timezone.utc)
    t   = now.hour * 60 + now.minute
    return any(start <= t < end for start, end in SESSIONS_UTC)


def session_label() -> str:
    now = datetime.now(timezone.utc)
    return now.strftime("%H:%M UTC")


def cooldown_ok(u: dict, asset: str) -> bool:
    ts = u.get("last_alert_ts", {}).get(asset)
    if not ts:
        return True
    return (time.time() - ts) >= COOLDOWN_MIN * 60


def day_blocked(u: dict):
    if u.get("paused"):
        return "⏸ Пауза активна. /resume чтобы продолжить."
    if u.get("consecutive_stops", 0) >= 3:
        return "⛔ 3 стопа подряд — день закрыт."
    if u.get("daily_pnl_pct", 0) <= DAY_LOSS_LIM:
        return "🚫 Дневной лимит −2% достигнут."
    if u.get("daily_r", 0) >= DAY_R_LIM:
        return f"🏁 +{DAY_R_LIM}R достигнуто. День закрыт."
    if u.get("daily_trades", 0) >= MAX_TRADES:
        return f"⛔ Лимит {MAX_TRADES} сделки в день достигнут."
    return None


def current_risk_pct(grade: str, consecutive_stops: int) -> float:
    if consecutive_stops >= 2:
        return 0.005
    return 0.01 if grade == "A+" else 0.005


def make_trade_id(u: dict, asset: str, side: str) -> str:
    d   = date.today().strftime("%Y%m%d")
    num = u.get("daily_trades", 0) + 1
    return f"{asset}-{side}-{d}-{num:02d}"


# ═══════════════════════════════════════════════════════════════════
#  КАЛЬКУЛЯТОР
# ═══════════════════════════════════════════════════════════════════
def calculate_trade(asset: str, side: str, entry: float, stop: float,
                    take: float, grade: str, consecutive_stops: int,
                    account_balance: float, atr_mode: str = "NORMAL") -> dict:
    side = side.upper()
    if side == "LONG":
        if stop >= entry:
            return {"ok": False, "reason": "LONG: стоп должен быть НИЖЕ входа"}
        if take <= entry:
            return {"ok": False, "reason": "LONG: тейк должен быть ВЫШЕ входа"}
        stop_pct   = (entry - stop)  / entry
        reward_pct = (take  - entry) / entry
    else:
        if stop <= entry:
            return {"ok": False, "reason": "SHORT: стоп должен быть ВЫШЕ входа"}
        if take >= entry:
            return {"ok": False, "reason": "SHORT: тейк должен быть НИЖЕ входа"}
        stop_pct   = (stop  - entry) / entry
        reward_pct = (entry - take)  / entry

    if stop_pct <= 0:
        return {"ok": False, "reason": "Некорректный стоп"}

    warnings = []
    s_min = STOP_MIN.get(asset, 0.2)
    s_max = STOP_MAX.get(asset, 3.0)
    if stop_pct * 100 < s_min:
        warnings.append(f"Стоп очень маленький ({stop_pct*100:.2f}%) — рекомендуется ≥{s_min}%")
    if stop_pct * 100 > s_max:
        warnings.append(f"Стоп большой ({stop_pct*100:.2f}%) — обычно ≤{s_max}% для {asset}")

    rr       = reward_pct / stop_pct
    rp       = current_risk_pct(grade, consecutive_stops)
    risk_usd = account_balance * rp

    # ATR HIGH: позиция уменьшается (стоп эффективно шире)
    atr_mult = 1.5 if atr_mode == "HIGH" else 1.0
    eff_stop = stop_pct * atr_mult
    pos_usd  = risk_usd / eff_stop

    if atr_mode == "HIGH":
        warnings.append(f"ATR HIGH: позиция уменьшена (коэф. ×{atr_mult})")

    # Плечо
    max_lev    = MAX_LEV.get(asset, 3)
    lev_raw    = pos_usd / account_balance
    lev_capped = lev_raw > max_lev

    if lev_capped:
        pos_usd  = account_balance * max_lev
        risk_usd = pos_usd * eff_stop
        warnings.append(
            f"Плечо обрезано до {max_lev}x (расч. {lev_raw:.1f}x)\n"
            f"Фактический риск: ${risk_usd:.0f} ({risk_usd/account_balance*100:.2f}%)"
        )
        lev_display = max_lev
    else:
        lev_display = round(lev_raw, 1)

    return {
        "ok":         True,
        "asset":      asset,
        "side":       side,
        "grade":      grade,
        "entry":      entry,
        "stop":       stop,
        "take":       take,
        "atr_mode":   atr_mode,
        "risk_pct":   risk_usd / account_balance,
        "risk_usd":   risk_usd,
        "stop_pct":   stop_pct   * 100,
        "reward_pct": reward_pct * 100,
        "rr":         rr,
        "pos_usd":    pos_usd,
        "lev":        lev_display,
        "stop_usd":   pos_usd * stop_pct,
        "take_usd":   pos_usd * reward_pct,
        "allowed":    rr >= 2.3,
        "warnings":   warnings,
    }


def build_calc_message(r: dict, trade_id: str = "") -> str:
    if not r["ok"]:
        return f"🚫 ОШИБКА\n\nПричина: {r['reason']}"
    status = (
        "✅ RR OK — сделка разрешена только если реакция подтверждена"
        if r["allowed"]
        else "❌ RR FAIL — вход запрещён"
    )
    warns  = ("\n\n" + "\n".join(f"⚠️ {w}" for w in r["warnings"])) if r["warnings"] else ""
    id_ln  = f"\nID: {trade_id}" if trade_id else ""
    rules = (
        "\n\nПравила:\n"
        "A+ = только reaction 3/3\n"
        "B = максимум reaction 2/3, риск 0.5%\n"
        "Стоп должен быть за зоной или экстремумом реакции\n"
        "Тейк — ближайшая ликвидность или RR ≥ 2.3"
        if r["allowed"]
        else "\n\nНе улучшай сделку эмоциями, не двигай стоп ближе ради красивого RR\n"
             "Либо жди лучший вход, либо пропусти сетап"
    )
    hint   = "\n\nПосле сделки: /sl (стоп) или /win X (профит)" if r["allowed"] else ""
    return (
        f"📊 {r['asset']} {r['side']} | {r['grade']}{id_ln}\n\n"
        f"Вход:    {r['entry']}\n"
        f"Стоп:    {r['stop']}  (−{r['stop_pct']:.2f}% / −${r['stop_usd']:.0f})\n"
        f"Тейк:    {r['take']}  (+{r['reward_pct']:.2f}% / +${r['take_usd']:.0f})\n\n"
        f"Риск:    ${r['risk_usd']:.0f}  ({r['risk_pct']*100:.1f}%)\n"
        f"Позиция: ${r['pos_usd']:.0f}\n"
        f"Плечо:   {r['lev']}x\n"
        f"RR:      1:{r['rr']:.2f}\n\n"
        f"{status}{rules}{warns}{hint}"
    )


# ═══════════════════════════════════════════════════════════════════
#  REACTION SCORING HELPERS
# ═══════════════════════════════════════════════════════════════════
def reaction_questions(side: str):
    if side == "SHORT":
        return [
            "Есть ли хвост сверху?",
            "Свеча закрылась ниже уровня?",
            "Нет продолжения вверх?",
        ]
    return [
        "Есть ли хвост снизу?",
        "Свеча закрылась выше уровня?",
        "Нет продолжения вниз?",
    ]


def yn_keyboard(yes_data: str, no_data: str) -> dict:
    return {"inline_keyboard": [[
        {"text": "✅ Да", "callback_data": yes_data},
        {"text": "❌ Нет", "callback_data": no_data},
    ]]}


def ask_reaction_q(chat_id: str, u: dict, q_num: int):
    """Задаёт вопрос реакции q_num (1, 2, 3)."""
    qs   = reaction_questions(u.get("side", "LONG"))
    text = f"Реакция {q_num}/3\n\n{qs[q_num-1]}"
    kb   = yn_keyboard(f"r{q_num}_yes", f"r{q_num}_no")
    send_message(chat_id, text, reply_markup=kb)


# ═══════════════════════════════════════════════════════════════════
#  JOURNAL HELPERS
# ═══════════════════════════════════════════════════════════════════
def start_journal(chat_id: str, u: dict, outcome: str, pnl_r: float):
    """Начинает flow журнала после закрытия сделки."""
    at = u.get("active_trade", {}) or {}
    u["journal_draft"] = {
        "trade_id": at.get("id", "?"),
        "outcome":  outcome,
        "pnl_r":    pnl_r,
    }
    u["flow_step"] = "journal_why"
    save_state()
    send_message(chat_id,
        f"📝 Журнал — {at.get('id', '?')}\n\n"
        f"Почему вошёл? (1–2 предложения)"
    )


def save_trade_to_history(chat_id: str, emotion: int):
    with STATE_LOCK:
        u  = get_user(chat_id)
        at = u.get("active_trade") or {}
        jd = u.get("journal_draft") or {}
        gate = u.get("gate", {})
        entry = {
            "id":             at.get("id", "?"),
            "date":           str(date.today()),
            "time":           at.get("session_time", session_label()),   # из момента входа
            "session":        at.get("session", "?"),                    # из момента входа
            "asset":          at.get("asset", "?"),
            "side":           at.get("side",  "?"),
            "grade":          at.get("grade", "?"),
            "entry":          at.get("entry", 0),
            "stop":           at.get("stop",  0),
            "take":           at.get("take",  0),
            "market_mode":    gate.get("market",  "?"),
            "level_quality":  gate.get("level",   "?"),
            "atr_mode":       gate.get("atr",     "NORMAL"),
            "reaction_score": u.get("reaction", {}).get("score", 0),
            "rr_planned":     at.get("rr", 0),
            "rr_actual":      jd.get("pnl_r", 0),
            "outcome":        jd.get("outcome", "?"),
            "risk_pct":       at.get("risk_pct", 0),
            "risk_usd":       round(at.get("risk_pct", 0) * u.get("account_balance", DEFAULT_BALANCE), 2),
            "pnl_r":          jd.get("pnl_r", 0),
            "pnl_pct":        at.get("risk_pct", 0) * jd.get("pnl_r", 0),
            "opened_at":      at.get("open_ts"),
            "journal": {
                "why":           jd.get("why", ""),
                "process_error": jd.get("process_error", False),
                "emotion":       emotion,
            },
        }
        u.setdefault("trade_history", []).append(entry)
        u["journal_draft"] = None
        u["flow_step"]     = None
        save_state()
        process_error_text = 'Да' if entry['journal']['process_error'] else 'Нет'
        msg = (
            f"✅ Журнал сохранён [{entry['id']}]\n"
            f"Эмоции: {emotion}/10 | Ошибка процесса: {process_error_text}"
        )
    send_message(chat_id, msg)


# ═══════════════════════════════════════════════════════════════════
#  КОМАНДЫ
# ═══════════════════════════════════════════════════════════════════
def handle_status(chat_id: str):
    u      = get_user(chat_id)
    cons   = u.get("consecutive_stops", 0)
    pnl    = u.get("daily_pnl_pct", 0) * 100
    r      = u.get("daily_r", 0)
    bal    = u.get("account_balance", DEFAULT_BALANCE)
    trades = u.get("daily_trades", 0)
    rem    = max(0, MAX_TRADES - trades)

    mood = "🔴" if pnl < -1 else "🟡" if pnl < 0 else "🟢"

    if cons >= 3:
        risk_note = "⛔ День закрыт — 3 стопа подряд"
    elif cons >= 2:
        risk_note = "⚠️ Риск 0.5% — 2 стопа подряд"
    else:
        risk_note = "✅ Риск 1% (A+) / 0.5% (B)"

    at = u.get("active_trade")
    active_line = (
        f"\n\n📌 Активная: {at['id']}\n"
        f"{at['asset']} {at['side']} | {at['grade']} | RR 1:{at['rr']:.2f}"
    ) if at and not at.get("closed") else "\n\nАктивных сделок нет."

    session = "✅ В торговом окне" if in_trading_session() else f"⏰ Вне окна ({session_label()})"
    block   = day_blocked(u)

    send_message(chat_id,
        f"{mood} Дневной отчёт — {u.get('day_date','?')}\n\n"
        f"Баланс:         ${bal:,.0f}\n"
        f"Сделок:         {trades}/{MAX_TRADES} (осталось {rem})\n"
        f"Стопов:         {u.get('daily_stops',0)}\n"
        f"Стопов подряд: {cons}\n"
        f"P&L день:       {pnl:+.2f}%\n"
        f"R за день:      {r:+.1f}R\n\n"
        f"{risk_note}\n"
        f"{session}"
        f"{active_line}"
        + (f"\n\n{block}" if block else "") +
        "\n\n0 сделок — это норма. Ничего не нужно догонять."
    )


def handle_sl(chat_id: str):
    with STATE_LOCK:
        u  = get_user(chat_id)
        at = u.get("active_trade")
        if not at or at.get("closed"):
            send_message(chat_id, "⚠️ Нет активной открытой сделки.")
            return
        rp = at.get("risk_pct", 0.01)
        u["consecutive_stops"] = u.get("consecutive_stops", 0) + 1
        u["daily_stops"]       = u.get("daily_stops", 0) + 1
        u["daily_trades"]      = u.get("daily_trades", 0) + 1
        u["daily_r"]           = round(u.get("daily_r", 0) - 1.0, 2)
        u["daily_pnl_pct"]     = round(u.get("daily_pnl_pct", 0) - rp, 4)
        at["closed"]           = True
        u["last_alert"]        = None   # сбрасываем контекст алерта
        cons = u["consecutive_stops"]
        msg  = (
            f"🔴 Стоп записан [{at['id']}]\n"
            f"P&L: {u['daily_pnl_pct']*100:+.2f}% | R: {u['daily_r']:+.1f}R | Стопов подряд: {cons}"
        )
        if cons >= 3:
            msg += "\n\n⛔ 3 стопа подряд — день закрыт."
        elif cons >= 2:
            msg += "\n⚠️ Риск 0.5% — только A+."
        if u["daily_pnl_pct"] <= DAY_LOSS_LIM:
            msg += "\n\n🚫 Лимит −2% достигнут."
    send_message(chat_id, msg)
    start_journal(chat_id, u, "loss", -1.0)


def handle_win(chat_id: str, r_val: float):
    with STATE_LOCK:
        u  = get_user(chat_id)
        at = u.get("active_trade")
        if not at or at.get("closed"):
            send_message(chat_id, "⚠️ Нет активной открытой сделки.")
            return
        rp = at.get("risk_pct", 0.01)
        u["consecutive_stops"] = 0
        u["daily_trades"]      = u.get("daily_trades", 0) + 1
        u["daily_r"]           = round(u.get("daily_r", 0) + r_val, 2)
        u["daily_pnl_pct"]     = round(u.get("daily_pnl_pct", 0) + rp * r_val, 4)
        at["closed"]           = True
        u["last_alert"]        = None   # сбрасываем контекст алерта
        msg = (
            f"✅ Профит +{r_val}R [{at['id']}]\n"
            f"R: {u['daily_r']:+.1f}R | P&L: {u['daily_pnl_pct']*100:+.2f}%\n"
            f"Серия стопов обнулена."
        )
        if u["daily_r"] >= DAY_R_LIM:
            msg += f"\n\n🏁 +{DAY_R_LIM}R — дневная цель. Стоп."
    send_message(chat_id, msg)
    start_journal(chat_id, u, "win", r_val)


def handle_weekly(chat_id: str):
    u       = get_user(chat_id)
    history = u.get("trade_history", [])
    cutoff  = (date.today() - timedelta(days=7)).isoformat()
    recent  = [t for t in history
               if t.get("date","") >= cutoff and t.get("outcome") in ("win","loss")]
    if not recent:
        send_message(chat_id, "Нет закрытых сделок за 7 дней.\n\n0 сделок — это норма.")
        return

    def calc_exp(trades):
        wins   = [t for t in trades if t["outcome"] == "win"]
        losses = [t for t in trades if t["outcome"] == "loss"]
        if not trades:
            return 0, 0, 0, 0
        wr      = len(wins) / len(trades)
        avg_w   = sum(t.get("pnl_r",0) for t in wins)   / len(wins)   if wins   else 0
        avg_l   = abs(sum(t.get("pnl_r",0) for t in losses) / len(losses)) if losses else 1
        exp     = wr * avg_w - (1 - wr) * avg_l
        return wr, avg_w, avg_l, exp

    lines = [f"📊 Статистика — 7 дней ({len(recent)} сделок)\n"]

    for grade in ["A+", "B"]:
        gt = [t for t in recent if t.get("grade") == grade]
        if not gt:
            continue
        wr, avg_w, avg_l, exp = calc_exp(gt)
        wins = [t for t in gt if t["outcome"] == "win"]
        mark = "✅" if exp >= 0 else "❌"
        lines.append(
            f"\n{grade} ({len(gt)} сделок):\n"
            f"Win: {len(wins)}/{len(gt)} ({wr*100:.0f}%)\n"
            f"Avg win: +{avg_w:.1f}R | Avg loss: −{avg_l:.1f}R\n"
            f"Expectancy: {exp:+.2f}R {mark}"
        )

    # Лучший актив
    asset_exp = {}
    for a in set(t.get("asset","?") for t in recent):
        at_list = [t for t in recent if t.get("asset") == a]
        if len(at_list) >= 2:
            _, _, _, exp = calc_exp(at_list)
            asset_exp[a] = exp
    if asset_exp:
        best = max(asset_exp, key=asset_exp.get)
        lines.append(f"\nЛучший актив: {best} (exp {asset_exp[best]:+.2f}R)")

    # Реакция 3/3 vs 2/3
    r3 = [t for t in recent if t.get("reaction_score") == 3]
    r2 = [t for t in recent if t.get("reaction_score") == 2]
    if len(r3) >= 2 and len(r2) >= 2:
        _, _, _, e3 = calc_exp(r3)
        _, _, _, e2 = calc_exp(r2)
        lines.append(f"\nРеакция 3/3: {e3:+.2f}R  vs  2/3: {e2:+.2f}R")

    # Эмоции: среднее
    emotions = [t["journal"]["emotion"] for t in recent
                if t.get("journal",{}).get("emotion")]
    if emotions:
        avg_e = sum(emotions) / len(emotions)
        lines.append(f"\nСредние эмоции: {avg_e:.1f}/10")

    lines.append("\n\n0 сделок — норма. Качество важнее количества.")
    send_message(chat_id, "\n".join(lines))


# ═══════════════════════════════════════════════════════════════════
#  A) TRADINGVIEW WEBHOOK
# ═══════════════════════════════════════════════════════════════════
@app.route("/webhook", methods=["POST"])
def webhook():
    data      = request.get_json(silent=True) or {}
    asset     = str(data.get("asset","UNKNOWN")).upper()
    asset     = asset.replace("USDT", "").replace("USDC", "")  # "BTCUSDT" → "BTC"
    direction = str(data.get("direction","LONG")).upper()
    if direction not in ("LONG", "SHORT"):
        direction = "LONG"
    alert_type = str(data.get("alert_type", "ZONE_HIT")).upper()

    # Валидация актива — режем сразу
    if asset not in SUPPORTED_ASSETS:
        return jsonify({"status": "unsupported asset", "asset": asset}), 400

    # Парсинг зон во float; если мусор — режем
    try:
        zone_low  = float(data.get("zone_low"))
        zone_high = float(data.get("zone_high"))
    except (TypeError, ValueError):
        return jsonify({"status": "bad zone payload"}), 400
    if zone_low >= zone_high:
        return jsonify({"status": "zone_low >= zone_high"}), 400

    entry_tf  = data.get("entry_tf","1H")
    ctx_tf    = data.get("context_tf","4H")
    price_raw = data.get("price", data.get("trigger_price", 0))
    price     = format_price(price_raw)

    # ── Поля от scanner v2 (опциональны для обратной совместимости) ──
    scanner_atr_mode = str(data.get("atr_mode") or "").upper() or None
    if scanner_atr_mode not in ("NORMAL", "HIGH", "EXTREME", None):
        scanner_atr_mode = None
    scanner_grade    = str(data.get("grade") or "").upper() or None
    if scanner_grade not in ("A+", "B", None):
        scanner_grade = None
    funding_risk     = bool(data.get("funding_risk", False))
    rsi_dir          = str(data.get("rsi_dir") or "").upper() or None
    if rsi_dir not in ("UP", "DOWN", "FLAT", None):
        rsi_dir = None

    # EXTREME ATR — сканер сам не должен такое присылать, но на всякий
    if scanner_atr_mode == "EXTREME":
        send_message(CHAT_ID,
            f"⚠️ {asset} {direction} @ {price}\n\n"
            f"🚫 ATR EXTREME (по сканеру) — алерт отброшен."
        )
        return jsonify({"status": "atr extreme"})

    with STATE_LOCK:
        u = get_user(CHAT_ID)   # RLock — захватывается повторно, OK

        # Активная сделка — новый алерт игнорируем
        at = u.get("active_trade")
        if at and not at.get("closed"):
            send_message(CHAT_ID,
                f"⚠️ {asset} {direction} @ {price}\n\n"
                f"⛔ Новый алерт проигнорирован: уже есть активная сделка [{at['id']}].\n"
                f"Закрой текущую (/sl или /win) перед новым входом."
            )
            return jsonify({"status": "active trade exists"})

        # Незавершённый flow — новый алерт перетрёт состояние
        BUSY_STEPS = {
            "reaction_1", "reaction_2", "reaction_3",
            "gate_market", "gate_level", "gate_atr", "gate_news",
            "grade", "calc",
            "journal_why", "journal_error_q", "journal_emotion",
        }
        if u.get("flow_step") in BUSY_STEPS:
            send_message(CHAT_ID,
                f"⚠️ {asset} {direction} @ {price}\n\n"
                f"⛔ Новый алерт проигнорирован: текущий flow ещё не завершён.\n"
                f"Шаг: {u['flow_step']}"
            )
            return jsonify({"status": "flow busy"})

        # Дневной блок
        block = day_blocked(u)
        if block:
            send_message(CHAT_ID, f"⚠️ Алерт: {asset} {direction} @ {price}\n\n{block}")
            return jsonify({"status": "day blocked"})

        # Кулдаун
        if not cooldown_ok(u, asset):
            elapsed = int((time.time() - u["last_alert_ts"].get(asset,0)) / 60)
            send_message(CHAT_ID,
                f"⏱ {asset} {direction} @ {price}\n"
                f"Кулдаун: {elapsed}/{COOLDOWN_MIN} мин. Алерт пропущен."
            )
            return jsonify({"status": "cooldown"})

        # Сохраняем метаданные
        u["last_alert_ts"][asset] = time.time()
        u["last_alert"] = {
            "asset": asset, "direction": direction,
            "zone_low": zone_low, "zone_high": zone_high, "price": price,
            # Подсказки от scanner v2 — нужны для gate / grade
            "atr_mode":     scanner_atr_mode,
            "grade":        scanner_grade,
            "funding_risk": funding_risk,
            "rsi_dir":      rsi_dir,
        }
        u["side"]      = direction
        u["asset"]     = asset
        u["reaction"]  = {}
        u["gate"]      = {}
        u["flow_step"] = None
        save_state()

    # Доп. строки в карточке (только если сканер их прислал)
    extras = []
    if scanner_grade:
        extras.append(f"Scanner grade: {scanner_grade}")
    if scanner_atr_mode:
        extras.append(f"ATR (scanner): {scanner_atr_mode}")
    if rsi_dir:
        extras.append(f"RSI(1H): {rsi_dir}")
    if funding_risk:
        extras.append("⚠️ Funding против сделки")
    extras_block = ("\n" + "\n".join(extras) + "\n") if extras else ""

    text = (
        f"⚠️ {asset} {direction} ZONE HIT\n\n"
        f"Entry TF: {entry_tf} | Context TF: {ctx_tf}\n"
        f"Zone: {format_price(zone_low)} – {format_price(zone_high)}\n"
        f"Price: {price}\n"
        f"{extras_block}\n"
        f"👁 Смотришь на реакцию. Есть сигнал?"
    )
    keyboard = {"inline_keyboard": [[
        {"text": "✅ Есть реакция", "callback_data": "reaction_start"},
        {"text": "❌ Нет реакции",  "callback_data": "reaction_no"},
        {"text": "🚫 Invalidate",   "callback_data": "invalidate"},
    ]]}
    send_message(CHAT_ID, text, reply_markup=keyboard)
    return jsonify({"status": "alert sent"})


# ═══════════════════════════════════════════════════════════════════
#  B) TELEGRAM WEBHOOK
# ═══════════════════════════════════════════════════════════════════
@app.route("/telegram", methods=["POST"])
def telegram_webhook():
    update = request.get_json(silent=True) or {}

    # ── Inline кнопки ─────────────────────────────────────────────
    if "callback_query" in update:
        cb      = update["callback_query"]
        cb_id   = cb["id"]
        data    = cb.get("data","")
        chat_id = str(cb.get("message",{}).get("chat",{}).get("id",""))
        u = get_user(chat_id)   # под STATE_LOCK внутри
        answer_callback(cb_id)

        # ── Начало реакции ─────────────────────────────────────────
        if data == "reaction_start":
            block = day_blocked(u)
            if block:
                send_message(chat_id, block)
                return jsonify({"status": "day blocked"})
            u["reaction"]  = {"q1": None, "q2": None, "q3": None, "score": 0}
            u["flow_step"] = "reaction_1"
            save_state()
            ask_reaction_q(chat_id, u, 1)

        elif data == "reaction_no":
            u["flow_step"] = None
            save_state()
            send_message(chat_id, "❌ Нет реакции — сетап отклонён.")

        elif data == "invalidate":
            # Если есть открытая сделка — invalidate не для неё
            at = u.get("active_trade")
            if at and not at.get("closed"):
                send_message(chat_id,
                    f"⚠️ Есть активная сделка [{at['id']}].\n"
                    f"Используй /sl, /win или /cleartrade."
                )
                return jsonify({"status": "active trade exists"})
            # Проверяем: есть ли реальный контекст (alert или незакрытый flow)
            has_alert  = bool(u.get("last_alert"))
            flow       = u.get("flow_step")
            in_flow    = flow in ("reaction_1","reaction_2","reaction_3",
                                  "gate_market","gate_level","gate_atr","gate_news",
                                  "grade","calc")
            has_context = has_alert or in_flow
            u["flow_step"] = None
            if not has_context:
                save_state()
                send_message(chat_id, "⚠️ Нет активного сетапа для аннулирования.")
            else:
                u.setdefault("trade_history", []).append({
                    "id":      f"{u.get('asset','?')}-{u.get('side','?')}-{date.today().strftime('%Y%m%d')}-INV",
                    "date":    str(date.today()),
                    "asset":   u.get("asset","?"),
                    "side":    u.get("side","?"),
                    "outcome": "invalidated",
                })
                u["last_alert"] = None
                save_state()
                send_message(chat_id, "🚫 Идея аннулирована. Не стоп — отмена сценария.")

        # ── Реакция Q1 ─────────────────────────────────────────────
        elif data in ("r1_yes","r1_no"):
            u["reaction"]["q1"] = (data == "r1_yes")
            u["flow_step"]      = "reaction_2"
            save_state()
            ask_reaction_q(chat_id, u, 2)

        # ── Реакция Q2 ─────────────────────────────────────────────
        elif data in ("r2_yes","r2_no"):
            u["reaction"]["q2"] = (data == "r2_yes")
            u["flow_step"]      = "reaction_3"
            save_state()
            ask_reaction_q(chat_id, u, 3)

        # ── Реакция Q3 — финал scoring ────────────────────────────
        elif data in ("r3_yes","r3_no"):
            u["reaction"]["q3"] = (data == "r3_yes")
            score = sum([
                bool(u["reaction"].get("q1")),
                bool(u["reaction"].get("q2")),
                u["reaction"]["q3"],
            ])
            u["reaction"]["score"] = score
            if score < 2:
                u["flow_step"] = None
                save_state()
                send_message(chat_id,
                    f"🚫 Реакция {score}/3 — сетап отклонён.\n"
                    f"Минимум 2/3 для продолжения."
                )
            else:
                label = "✅ Сильная" if score == 3 else "⚠️ Допустимая"
                u["flow_step"] = "gate_market"
                save_state()
                send_message(chat_id,
                    f"{label} реакция {score}/3\n\nРежим рынка:",
                    reply_markup={"inline_keyboard": [[
                        {"text": "📈 TREND", "callback_data": "market_TREND"},
                        {"text": "↔️ RANGE", "callback_data": "market_RANGE"},
                        {"text": "🌪 CHAOS", "callback_data": "market_CHAOS"},
                    ]]}
                )

        # ── Gate Q1: режим рынка ────────────────────────────────────
        elif data.startswith("market_"):
            market = data.split("_",1)[1]
            if market == "CHAOS":
                u["flow_step"] = None
                save_state()
                send_message(chat_id, "🚫 CHAOS — сделок нет. Ждёшь структуры.")
            else:
                u["gate"]["market"] = market
                u["flow_step"]      = "gate_level"
                save_state()
                send_message(chat_id,
                    f"Рынок: {market}\n\nКачество уровня:",
                    reply_markup={"inline_keyboard": [[
                        {"text": "🎯 4/4 (все критерии)", "callback_data": "level_4"},
                        {"text": "✅ 3/4",                 "callback_data": "level_3"},
                    ]]}
                )

        # ── Gate Q2: уровень ────────────────────────────────────────
        elif data in ("level_3","level_4"):
            level = "4/4" if data == "level_4" else "3/4"
            u["gate"]["level"] = level
            # Если scanner v2 уже определил ATR mode — используем без вопроса.
            scanner_atr = (u.get("last_alert") or {}).get("atr_mode")
            if scanner_atr in ("NORMAL", "HIGH"):
                u["gate"]["atr"] = scanner_atr
                u["flow_step"]   = "gate_news"
                save_state()
                tag = "⚠️" if scanner_atr == "HIGH" else "✅"
                send_message(chat_id,
                    f"Уровень: {level}\n"
                    f"ATR (от сканера): {tag} {scanner_atr}\n\n"
                    f"Новости в ближайшие 30 мин?",
                    reply_markup={"inline_keyboard": [[
                        {"text": "✅ Нет новостей", "callback_data": "news_no"},
                        {"text": "🚫 Есть новости", "callback_data": "news_yes"},
                    ]]}
                )
            else:
                u["flow_step"] = "gate_atr"
                save_state()
                send_message(chat_id,
                    f"Уровень: {level}\n\nATR сейчас:",
                    reply_markup={"inline_keyboard": [[
                        {"text": "✅ NORMAL", "callback_data": "atr_NORMAL"},
                        {"text": "⚠️ HIGH",   "callback_data": "atr_HIGH"},
                        {"text": "🚫 EXTREME","callback_data": "atr_EXTREME"},
                    ]]}
                )

        # ── Gate Q3: ATR ───────────────────────────────────────────
        elif data.startswith("atr_"):
            atr = data.split("_",1)[1]
            if atr == "EXTREME":
                u["flow_step"] = None
                save_state()
                send_message(chat_id, "🚫 ATR EXTREME — не торгуешь.")
            else:
                u["gate"]["atr"] = atr
                u["flow_step"]   = "gate_news"
                save_state()
                send_message(chat_id,
                    f"ATR: {atr}\n\nНовости в ближайшие 30 мин?",
                    reply_markup={"inline_keyboard": [[
                        {"text": "✅ Нет новостей", "callback_data": "news_no"},
                        {"text": "🚫 Есть новости", "callback_data": "news_yes"},
                    ]]}
                )

        # ── Gate Q4: новости ────────────────────────────────────────
        elif data in ("news_yes","news_no"):
            if data == "news_yes":
                u["flow_step"] = None
                save_state()
                send_message(chat_id, "🚫 Новости в 30 мин — не торгуешь.")
            else:
                u["gate"]["news"] = False
                # Проверка сессии
                in_session = in_trading_session()
                atr        = u["gate"].get("atr","NORMAL")
                cons       = u.get("consecutive_stops",0)
                # Итог gate — показываем сводку и предлагаем grade
                gate_summary = (
                    f"✅ Pre-trade gate пройден\n\n"
                    f"Рынок: {u['gate'].get('market','?')} | "
                    f"Уровень: {u['gate'].get('level','?')} | "
                    f"ATR: {atr} | "
                    f"Реакция: {u['reaction'].get('score',0)}/3\n"
                    f"Сессия: {'✅' if in_session else '⚠️ Вне окна'} ({session_label()})\n\n"
                    f"Grade:"
                )
                # B ограничения
                b_blocked = (
                    not in_session or
                    atr == "HIGH" or
                    cons >= 2
                )
                b_label = "🟡 B" if not b_blocked else "🟡 B ⛔"
                b_data  = "grade_B" if not b_blocked else "grade_B_blocked"
                u["flow_step"] = "grade"
                save_state()
                send_message(chat_id, gate_summary,
                    reply_markup={"inline_keyboard": [[
                        {"text": "🟢 A+", "callback_data": "grade_A+"},
                        {"text": b_label, "callback_data": b_data},
                    ]]}
                )

        # ── /cleartrade подтверждение ───────────────────────────────
        elif data == "cleartrade_confirm":
            at = u.get("active_trade")
            if at and not at.get("closed"):
                trade_id = at.get("id","?")
                u["active_trade"] = None
                u["flow_step"]    = None
                u["last_alert"]   = None
                u["last_alert_ts"] = {}   # сбрасываем кулдауны — даём шанс перезайти
                u["reaction"]     = {}
                u["gate"]         = {}
                u["grade"]        = None
                u["asset"]        = None
                u["side"]         = None
                save_state()
                send_message(chat_id,
                    f"🗑 Сделка [{trade_id}] сброшена без записи в журнал.\n"
                    f"Убедись что позиция закрыта на бирже."
                )
            else:
                send_message(chat_id, "Нет активной сделки для сброса.")
        elif data == "cleartrade_cancel":
            send_message(chat_id, "Отмена — сделка сохранена.")

        # ── /hardreset подтверждение ────────────────────────────────
        elif data == "hardreset_yes":
            cons = u.get("consecutive_stops", 0)
            u["consecutive_stops"] = 0
            save_state()
            send_message(chat_id,
                f"✅ Серия стопов {cons} → 0 сброшена.\n"
                f"Используй только если серия была записана ошибочно."
            )
        elif data == "hardreset_no":
            send_message(chat_id, "Отмена — серия стопов сохранена.")

        # ── Grade blocked ───────────────────────────────────────────
        elif data == "grade_B_blocked":
            reasons = []
            if not in_trading_session():
                reasons.append("вне торгового окна")
            if u["gate"].get("atr") == "HIGH":
                reasons.append("ATR HIGH")
            if u.get("consecutive_stops",0) >= 2:
                reasons.append(f"{u['consecutive_stops']} стопа подряд")
            # flow_step остаётся "grade" — пользователь жмёт A+ или /cleartrade.
            # Но даём ему явный выход: показываем кнопку отмены.
            send_message(chat_id,
                f"⛔ B заблокирован:\n" + "\n".join(f"• {r}" for r in reasons) +
                "\n\nВыбери A+ или отменяй (/cleartrade ничего не запишет — "
                "сделки ведь ещё нет; используй /reset чтобы выйти из flow)."
            )

        # ── Grade выбран ────────────────────────────────────────────
        elif data in ("grade_A+","grade_B"):
            grade = "A+" if data == "grade_A+" else "B"
            reaction_score = u.get("reaction", {}).get("score", 0)
            # Протокол: A+ требует все 3 условия входа, паттерн = reaction 3/3.
            if grade == "A+" and reaction_score < 3:
                send_message(chat_id,
                    f"⚠️ A+ требует реакцию 3/3 — сейчас {reaction_score}/3.\n"
                    f"Доступен только B."
                )
                return jsonify({"status": "A+ blocked: weak reaction"})
            if grade == "B" and u.get("consecutive_stops",0) >= 2:
                send_message(chat_id,
                    f"⚠️ {u['consecutive_stops']} стопа подряд — только A+."
                )
                return jsonify({"status": "grade blocked"})
            u["grade"]     = grade
            u["flow_step"] = "calc"
            save_state()
            emoji = "🟢" if grade == "A+" else "🟡"
            atr   = u["gate"].get("atr","NORMAL")
            atr_note = " | ⚠️ позиция уменьшена (ATR HIGH)" if atr == "HIGH" else ""
            # Если scanner v2 прислал funding_risk — напоминаем перед калькулятором
            f_risk = (u.get("last_alert") or {}).get("funding_risk")
            funding_note = "\n⚠️ Funding против сделки (контекст, не блок)." if f_risk else ""
            send_message(chat_id,
                f"{emoji} {u.get('asset','?')} {u.get('side','?')} | Grade {grade}{atr_note}{funding_note}\n\n"
                f"Отправь одним сообщением:\n"
                f"entry 75620 stop 74980 take 77200"
            )

        # ── Journal: ошибка процесса ────────────────────────────────
        elif data in ("journal_error_yes","journal_error_no"):
            has_error = (data == "journal_error_yes")
            jd = u.get("journal_draft") or {}
            jd["process_error"] = has_error
            u["journal_draft"]  = jd
            u["flow_step"]      = "journal_emotion"
            save_state()
            send_message(chat_id, "Эмоции на входе (1–10):")

        return jsonify({"status": "callback handled"})

    # ── Обычные сообщения ──────────────────────────────────────────
    if "message" in update:
        msg     = update["message"]
        chat_id = str(msg.get("chat",{}).get("id",""))
        text    = msg.get("text","").strip()
        u = get_user(chat_id)   # под STATE_LOCK внутри

        # ── Команды (всегда первыми) ────────────────────────────────
        if text == "/start":
            send_message(chat_id,
                "✅ Бот активен v4.4\n\n"
                "Команды:\n"
                "/status        — дневной отчёт\n"
                "/sl            — стоп-лосс\n"
                "/win 2.3       — профит\n"
                "/weekly        — статистика за 7 дней\n"
                "/balance 14500 — обновить баланс\n"
                "/pause         — пауза\n"
                "/resume        — снять паузу\n"
                "/reset         — сбросить дневные счётчики\n"
                "/cleartrade    — сбросить активную сделку (с подтверждением)\n"
                "/hardreset     — обнулить серию стопов (осознанно!)\n"
                "/remind        — статус таймера (авто через 3ч после входа)\n"
                "/help          — справка"
            )
            return jsonify({"status": "start"})

        if text == "/help":
            send_message(chat_id,
                "Поток:\n"
                "1. Сканер 4H → ZONE HIT\n"
                "2. Проверяешь реакцию (3 вопроса)\n"
                "3. Pre-trade gate (рынок / уровень / ATR / новости)\n"
                "4. Grade A+ / B\n"
                "5. entry stop take → расчёт\n\n"
                "После сделки:\n"
                "/sl          — стоп\n"
                "/win 2.3     — профит\n"
                "→ журнал (3 вопроса)\n\n"
                "Баланс раз в неделю:\n"
                "/balance 14800"
            )
            return jsonify({"status": "help"})

        if text == "/status":
            handle_status(chat_id)
            return jsonify({"status": "ok"})

        if text == "/sl":
            handle_sl(chat_id)
            return jsonify({"status": "ok"})

        if text.startswith("/win"):
            parts = text.split()
            try:
                r_val = float(parts[1]) if len(parts) > 1 else 1.0
                assert 0 < r_val < 20
            except Exception:
                send_message(chat_id, "Формат: /win 2.3")
                return jsonify({"status": "bad win"})
            handle_win(chat_id, r_val)
            return jsonify({"status": "ok"})

        if text == "/weekly":
            handle_weekly(chat_id)
            return jsonify({"status": "ok"})

        if text.startswith("/balance"):
            parts = text.split()
            try:
                bal = float(parts[1])
                assert 100 < bal < 10_000_000
            except Exception:
                send_message(chat_id, "Формат: /balance 14800")
                return jsonify({"status": "bad balance"})
            u["account_balance"] = bal
            save_state()
            send_message(chat_id, f"✅ Баланс обновлён: ${bal:,.0f}")
            return jsonify({"status": "ok"})

        if text == "/pause":
            u["paused"] = True
            save_state()
            send_message(chat_id, "⏸ Пауза. /resume чтобы продолжить.")
            return jsonify({"status": "ok"})

        if text == "/resume":
            u["paused"] = False
            save_state()
            send_message(chat_id, "▶️ Пауза снята.")
            return jsonify({"status": "ok"})

        if text == "/reset":
            cons = u.get("consecutive_stops", 0)
            at   = u.get("active_trade")
            has_open_trade = at and not at.get("closed")
            u.update({
                "day_date":      str(date.today()),
                "daily_stops":   0,
                "daily_trades":  0,
                "daily_r":       0.0,
                "daily_pnl_pct": 0.0,
                # consecutive_stops НЕ сбрасывается — только /win или /hardreset
                # active_trade НЕ сбрасывается — используй /cleartrade
                "flow_step":     None,
                "paused":        False,
            })
            save_state()
            cons_note  = f"\n⚠️ Серия стопов {cons} сохранена." if cons > 0 else ""
            trade_note = (
                f"\n📌 Активная сделка [{at['id']}] сохранена. /cleartrade — если нужно сбросить."
            ) if has_open_trade else ""
            send_message(chat_id,
                f"🔄 Дневные счётчики сброшены.{cons_note}{trade_note}"
            )
            return jsonify({"status": "ok"})

        if text == "/hardreset":
            cons = u.get("consecutive_stops", 0)
            if cons == 0:
                send_message(chat_id, "Серия стопов и так 0 — ничего не нужно сбрасывать.")
                return jsonify({"status": "ok"})
            send_message(chat_id,
                f"⚠️ Сбросить серию стопов?\n\n"
                f"Сейчас: {cons} стопа подряд\n\n"
                f"Используй только если серия записана ошибочно.",
                reply_markup={"inline_keyboard": [[
                    {"text": "✅ Да, сбросить", "callback_data": "hardreset_yes"},
                    {"text": "❌ Отмена",        "callback_data": "hardreset_no"},
                ]]}
            )
            return jsonify({"status": "ok"})

        if text == "/cleartrade":
            at = u.get("active_trade")
            if not at or at.get("closed"):
                send_message(chat_id, "Нет активной открытой сделки.")
                return jsonify({"status": "ok"})
            send_message(chat_id,
                f"⚠️ Сбросить активную сделку?\n\n"
                f"ID: {at['id']}\n"
                f"{at['asset']} {at['side']} | Grade {at['grade']}\n"
                f"Вход: {at['entry']} | Стоп: {at['stop']} | Тейк: {at['take']}\n\n"
                f"Это НЕ запишет результат в журнал.\n"
                f"Используй только если сделка была введена ошибочно.",
                reply_markup={"inline_keyboard": [[
                    {"text": "🗑 Да, сбросить", "callback_data": "cleartrade_confirm"},
                    {"text": "❌ Отмена",        "callback_data": "cleartrade_cancel"},
                ]]}
            )
            return jsonify({"status": "ok"})

        if text == "/remind":
            at = u.get("active_trade")
            if not at or at.get("closed"):
                send_message(chat_id, "Нет активной сделки — напоминание не нужно.")
                return jsonify({"status": "ok"})
            open_ts = at.get("open_ts")
            if not open_ts:
                send_message(chat_id, "⚠️ Нет метки времени входа — обнови баланс или переоткрой сделку.")
                return jsonify({"status": "ok"})
            elapsed_min = int((time.time() - open_ts) / 60)
            remain_min  = max(0, 180 - elapsed_min)
            if remain_min == 0:
                send_message(chat_id,
                    f"⏰ Сделка открыта {elapsed_min} мин назад.\n"
                    f"Ты уже должен был получить напоминание. Всё ещё в рынке?"
                )
            else:
                h, m = divmod(remain_min, 60)
                send_message(chat_id,
                    f"⏱ Сделка открыта {elapsed_min} мин назад.\n"
                    f"Напоминание через {h}ч {m}мин (3-часовая метка)."
                )
            return jsonify({"status": "ok"})

        # ── Flow: journal_why ──────────────────────────────────────
        step = u.get("flow_step")
        if step == "journal_why":
            if len(text.strip()) < 3:
                send_message(chat_id, "Напиши хотя бы несколько слов.")
                return jsonify({"status": "ok"})
            jd = u.get("journal_draft") or {}
            jd["why"]          = text.strip()
            u["journal_draft"] = jd
            u["flow_step"]     = "journal_error_q"
            save_state()
            send_message(chat_id,
                "Была ли ошибка процесса?",
                reply_markup={"inline_keyboard": [[
                    {"text": "✅ Нет", "callback_data": "journal_error_no"},
                    {"text": "⚠️ Да", "callback_data": "journal_error_yes"},
                ]]}
            )
            return jsonify({"status": "ok"})

        # ── Flow: journal_emotion ──────────────────────────────────
        if step == "journal_emotion":
            try:
                emotion = int(text.strip())
                assert 1 <= emotion <= 10
            except Exception:
                send_message(chat_id, "Введи число от 1 до 10.")
                return jsonify({"status": "ok"})
            save_trade_to_history(chat_id, emotion)
            return jsonify({"status": "ok"})

        # ── Flow: calc (entry/stop/take) ───────────────────────────
        if step == "calc":
            block = day_blocked(u)
            if block:
                send_message(chat_id, f"Расчёт заблокирован:\n{block}")
                u["flow_step"] = None
                save_state()
                return jsonify({"status": "day blocked"})
            match = re.search(
                r"entry\s+([0-9]*\.?[0-9]+)\s+stop\s+([0-9]*\.?[0-9]+)\s+take\s+([0-9]*\.?[0-9]+)",
                text, re.IGNORECASE
            )
            if not match:
                send_message(chat_id, "❗ Формат:\nentry 75620 stop 74980 take 77200")
                return jsonify({"status": "bad format"})
            entry = float(match.group(1))
            stop  = float(match.group(2))
            take  = float(match.group(3))
            atr   = u.get("gate", {}).get("atr", "NORMAL")
            result = calculate_trade(
                asset             = u.get("asset","BTC"),
                side              = u.get("side","LONG"),
                entry             = entry,
                stop              = stop,
                take              = take,
                grade             = u.get("grade","B"),
                consecutive_stops = u.get("consecutive_stops",0),
                account_balance   = u.get("account_balance", DEFAULT_BALANCE),
                atr_mode          = atr,
            )
            trade_id = ""
            if result["ok"] and result["allowed"]:
                trade_id = make_trade_id(u, result["asset"], result["side"])
                u["active_trade"] = {
                    "id":           trade_id,
                    "asset":        result["asset"],
                    "side":         result["side"],
                    "grade":        result["grade"],
                    "entry":        entry,
                    "stop":         stop,
                    "take":         take,
                    "atr_mode":     atr,
                    "risk_pct":     result["risk_pct"],
                    "rr":           round(result["rr"],2),
                    "open_ts":      time.time(),
                    "session":      "IN" if in_trading_session() else "OUT",
                    "session_time": session_label(),
                    "remind_sent":  False,
                    "closed":       False,
                }
            send_message(chat_id, build_calc_message(result, trade_id))
            u["flow_step"] = None
            save_state()
            return jsonify({"status": "calc sent"})

    return jsonify({"status": "ignored"})


# ═══════════════════════════════════════════════════════════════════
#  SETUP / HOME
# ═══════════════════════════════════════════════════════════════════
@app.route("/setup", methods=["GET"])
def setup():
    base = (PUBLIC_BASE_URL or "https://helptreating-bot-production.up.railway.app").rstrip("/")
    webhook_url = f"{base}/telegram"
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/setWebhook"
    r = requests.get(url, params={"url": webhook_url}, timeout=15)
    return jsonify({"target": webhook_url, "telegram_response": r.json()})


@app.route("/", methods=["GET"])
def home():
    return jsonify({"status": "ok", "version": "4.4"})


# ═══════════════════════════════════════════════════════════════════
#  ФОНОВЫЙ ПОТОК — 3-ЧАСОВЫЕ НАПОМИНАНИЯ
# ═══════════════════════════════════════════════════════════════════
REMIND_AFTER_SEC = 3 * 3600   # 3 часа


def reminder_loop():
    """Проверяет раз в минуту все открытые сделки. Если прошло 3 ч — шлёт напоминание."""
    while True:
        time.sleep(60)
        try:
            # Снапшот списка под локом — иначе можно поймать
            # 'dictionary changed size during iteration', если хендлер
            # параллельно создаёт нового пользователя.
            with STATE_LOCK:
                items = list(USER_STATE.items())

            for cid, u in items:
                # Чтение под локом + сборка текста под локом, отправка — вне.
                with STATE_LOCK:
                    at = u.get("active_trade")
                    if not at or at.get("closed") or at.get("remind_sent"):
                        continue
                    open_ts = at.get("open_ts")
                    if not open_ts:
                        continue
                    elapsed = time.time() - open_ts
                    if elapsed < REMIND_AFTER_SEC:
                        continue
                    h = int(elapsed // 3600)
                    m = int((elapsed % 3600) // 60)
                    msg_text = (
                        f"⏰ Напоминание — сделка [{at['id']}] открыта уже {h}ч {m}мин\n\n"
                        f"{at['asset']} {at['side']} | Вход: {at['entry']}\n"
                        f"Стоп: {at['stop']} | Тейк: {at['take']}\n\n"
                        f"Проверь позицию. Всё ещё актуально?"
                    )
                    at["remind_sent"] = True
                    save_state()
                # Telegram I/O — уже без лока, чтобы не блокировать хендлеры
                send_message(cid, msg_text)
        except Exception as e:
            print("Reminder loop error:", e)


# ==========================================
# СТАРТ
# ==========================================
load_state_from_disk()

# Запускаем фоновый поток напоминаний (daemon — умирает вместе с процессом)
_reminder_thread = threading.Thread(target=reminder_loop, daemon=True)
_reminder_thread.start()

# Запускаем сканер рынка (Hyperliquid → BTC/ETH/SOL → отчёт + ZONE HIT)
from scanner import start_scanner_thread

# Локальный POST вместо публичного URL — нет лишнего сетевого хопа,
# не зависит от смены Railway-домена. Можно перебить через SCANNER_WEBHOOK_URL.
_self_webhook_url = (
    os.getenv("SCANNER_WEBHOOK_URL")
    or f"http://127.0.0.1:{os.getenv('PORT', '8080')}/webhook"
)

start_scanner_thread(
    telegram_token=TELEGRAM_TOKEN,
    chat_id=CHAT_ID,
    self_webhook_url=_self_webhook_url,
)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
