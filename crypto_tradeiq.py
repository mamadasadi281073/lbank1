import os
import re
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import pandas as pd
import requests

STATE_FILE = "kcex_signal_state.json"

# =========================
# BINANCE SPOT SOURCE
# =========================
# Public market-data only. No Binance account/API key is required.
# GitHub Actions is used as the execution environment so the scanner does
# not depend on opening Binance in the user's local browser/network.
COINS_FILE = os.getenv("COINS_FILE", "coins.txt")

FALLBACK_SOURCE_SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    "ADAUSDT", "DOGEUSDT", "AVAXUSDT", "LINKUSDT", "DOTUSDT",
]

BINANCE_SPOT_BASE = os.getenv(
    "BINANCE_SPOT_BASE",
    "https://data-api.binance.vision",
).rstrip("/")

BINANCE_TIMEOUT = int(os.getenv("BINANCE_TIMEOUT", "15"))
BINANCE_4H_BARS = int(os.getenv("BINANCE_4H_BARS", "400"))
BINANCE_1D_BARS = int(os.getenv("BINANCE_1D_BARS", "800"))
BINANCE_SCAN_WORKERS = int(os.getenv("BINANCE_SCAN_WORKERS", "16"))

SENS = 0.28
MIN_EVENT_SEPARATION = 5
CANDIDATE_START = 4
CANDIDATE_END = 15


# =========================
# TAKE PROFIT SETTINGS
# =========================

TP1_PERCENT = 1.0
TP2_PERCENT = 2.0
TP3_PERCENT = 3.0
TP4_PERCENT = 4.0
TP5_PERCENT = 5.0
TP6_PERCENT = 8.0
TP7_PERCENT = 10.0
TP8_PERCENT = 20.0
# TP9 to TP33 increase by 10 percentage points each time.
# TP34 to TP40 are 50% higher than the previous percentage.
# For SELL, TP33..TP39 are invalid (>=100% below entry) and TP40 is capped at 99%.
# TP40 is the final / FULL TP.

STARTING_BALANCE = 200.0
FIXED_MARGIN = 10.0
LEVERAGE = 10.0

# Maximum allowed distance between the signal event close and the
# selected Order Block boundary. Kept identical to the original filter.
MAX_OB_DISTANCE_PERCENT = 4.0

# =========================
# DAILY / MONTHLY REPORT
# =========================

# گزارش روزانه در ساعت 20:00 UTC
DAILY_REPORT_HOUR_UTC = int(
    os.getenv("DAILY_REPORT_HOUR_UTC", "20")
)

DAILY_REPORT_MINUTE_UTC = int(
    os.getenv("DAILY_REPORT_MINUTE_UTC", "0")
)

# گزارش ماهانه در روز اول هر ماه
# و بعد از اجرای ساعت گزارش روزانه ارسال می‌شود.
MONTHLY_REPORT_HOUR_UTC = int(
    os.getenv("MONTHLY_REPORT_HOUR_UTC", "20")
)

MONTHLY_REPORT_MINUTE_UTC = int(
    os.getenv("MONTHLY_REPORT_MINUTE_UTC", "0")
)

# گزارش هفتگی در اولین روز هفته (دوشنبه) ساعت 20:00 UTC
WEEKLY_REPORT_HOUR_UTC = int(
    os.getenv("WEEKLY_REPORT_HOUR_UTC", "20")
)

WEEKLY_REPORT_MINUTE_UTC = int(
    os.getenv("WEEKLY_REPORT_MINUTE_UTC", "0")
)

TELEGRAM_SIGNATURE = "@Cryptososhiant"


# =========================================================
# TIME
# =========================================================

def utc_now():
    return datetime.now(
        timezone.utc
    ).strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )


def utc_datetime():
    return datetime.now(
        timezone.utc
    )


def utc_date():
    return datetime.now(
        timezone.utc
    ).strftime(
        "%Y-%m-%d"
    )


def utc_month():
    return datetime.now(
        timezone.utc
    ).strftime(
        "%Y-%m"
    )


# =========================================================
# STATE
# =========================================================

def load_state():

    if not os.path.exists(STATE_FILE):

        return {
            "initialized": False,
            "last_event": {},
            "last_signal": {},
            "active": {},
            "closed_trades": [],
            "deliveries": {},
            "daily_stats": {},
            "monthly_stats": {},
            "weekly_stats": {},
            "portfolio": {
                "initial_balance": STARTING_BALANCE,
                "balance": STARTING_BALANCE,
                "next_trade_number": 1,
                "total_realized_pnl": 0.0,
                "period_opening_balances": {},
                "equity_curve": [],
            },
            "telegram_update_offset": 0,
            "last_daily_report": "",
            "last_monthly_report": "",
            "last_weekly_report": "",
        }

    try:

        with open(
            STATE_FILE,
            "r",
            encoding="utf-8",
        ) as f:

            state = json.load(f)

    except Exception:

        state = {}

    state.setdefault(
        "initialized",
        False,
    )

    state.setdefault(
        "last_event",
        {},
    )

    state.setdefault(
        "last_signal",
        {},
    )

    # Per-symbol bootstrap marker.  A missing marker means we have never
    # established a baseline for that symbol, so historical signals are
    # recorded but never sent as new Telegram signals.
    state.setdefault(
        "signal_baseline",
        {},
    )

    state.setdefault(
        "active",
        {},
    )

    state.setdefault(
        "deliveries",
        {},
    )

    state.setdefault(
        "daily_stats",
        {},
    )

    state.setdefault(
        "monthly_stats",
        {},
    )

    state.setdefault(
        "weekly_stats",
        {},
    )

    state.setdefault(
        "last_daily_report",
        "",
    )

    state.setdefault(
        "last_monthly_report",
        "",
    )

    state.setdefault("closed_trades", [])
    portfolio = state.setdefault("portfolio", {})
    portfolio.setdefault("initial_balance", STARTING_BALANCE)
    portfolio.setdefault("balance", STARTING_BALANCE)
    portfolio.setdefault("next_trade_number", 1)
    portfolio.setdefault("total_realized_pnl", 0.0)
    portfolio.setdefault("period_opening_balances", {})
    portfolio.setdefault("equity_curve", [])
    state.setdefault("telegram_update_offset", 0)
    state.setdefault(
        "last_weekly_report",
        "",
    )

    # Migration for older state files.
    # Older trades may have trade_number=None. Assign real unique numbers
    # before any new signal is processed so "None" can never be sent.
    used_numbers = []
    for item in state.get("active", {}).values():
        try:
            if item.get("trade_number") is not None:
                used_numbers.append(int(item.get("trade_number")))
        except (TypeError, ValueError):
            pass
    for item in state.get("closed_trades", []):
        try:
            if item.get("trade_number") is not None:
                used_numbers.append(int(item.get("trade_number")))
        except (TypeError, ValueError):
            pass

    next_number = max(used_numbers, default=0) + 1
    for active in state["active"].values():
        if active.get("trade_number") is None:
            active["trade_number"] = next_number
            used_numbers.append(next_number)
            next_number += 1
        # Migrate older active-trade schemas so a missing entry_price can
        # never crash the hourly scan.
        if active.get("entry_price") is None:
            for key in ("entry", "zone_low", "zone_high"):
                if active.get(key) is not None:
                    try:
                        active["entry_price"] = float(active[key])
                        break
                    except (TypeError, ValueError):
                        pass
        active.setdefault("initial_sl", active.get("sl"))
        active.setdefault("current_sl", active.get("sl"))
        active.setdefault("margin", 0.0)
        active.setdefault("notional", 0.0)
        active.setdefault("tp4_hit", False)
        active.setdefault("tp5_hit", False)
        active.setdefault("tp6_hit", False)
        active.setdefault("tp7_hit", False)
        active.setdefault("tp8_hit", False)
        for n in range(9, 41):
            active.setdefault(f"tp{n}_hit", False)
        active.setdefault("tp_hits", [])
        active.setdefault("balance_at_entry", float(portfolio.get("balance", STARTING_BALANCE)))

    try:
        saved_next = int(portfolio.get("next_trade_number", 1))
    except (TypeError, ValueError):
        saved_next = 1
    portfolio["next_trade_number"] = max(saved_next, max(used_numbers, default=0) + 1)

    return state


def save_state(state):

    tmp = STATE_FILE + ".tmp"

    with open(
        tmp,
        "w",
        encoding="utf-8",
    ) as f:

        json.dump(
            state,
            f,
            ensure_ascii=False,
            indent=2,
        )

    os.replace(
        tmp,
        STATE_FILE,
    )


# =========================================================
# STATISTICS HELPERS
# =========================================================

def empty_stats():

    return {
        "signals": 0,
        "tp_wins": 0,
        "sl_losses": 0,
        "signal_ids": [],
        "tp_signal_ids": [],
        "sl_signal_ids": [],
    }


def get_daily_stats(
    state,
    date_string=None,
):

    if date_string is None:
        date_string = utc_date()

    stats = state[
        "daily_stats"
    ].setdefault(
        date_string,
        empty_stats(),
    )

    stats.setdefault(
        "signals",
        0,
    )

    stats.setdefault(
        "tp_wins",
        0,
    )

    stats.setdefault(
        "sl_losses",
        0,
    )

    stats.setdefault(
        "signal_ids",
        [],
    )

    stats.setdefault(
        "tp_signal_ids",
        [],
    )

    stats.setdefault(
        "sl_signal_ids",
        [],
    )

    return stats


def get_monthly_stats(
    state,
    month_string=None,
):

    if month_string is None:
        month_string = utc_month()

    stats = state[
        "monthly_stats"
    ].setdefault(
        month_string,
        empty_stats(),
    )

    stats.setdefault(
        "signals",
        0,
    )

    stats.setdefault(
        "tp_wins",
        0,
    )

    stats.setdefault(
        "sl_losses",
        0,
    )

    stats.setdefault(
        "signal_ids",
        [],
    )

    stats.setdefault(
        "tp_signal_ids",
        [],
    )

    stats.setdefault(
        "sl_signal_ids",
        [],
    )

    return stats


def get_week_start(dt):
    # هفته از دوشنبه شروع می‌شود.
    monday = dt - pd.Timedelta(days=int(dt.weekday()))
    return monday.strftime("%Y-%m-%d")


def get_weekly_stats(
    state,
    week_string=None,
):

    if week_string is None:
        week_string = get_week_start(
            utc_datetime()
        )

    stats = state[
        "weekly_stats"
    ].setdefault(
        week_string,
        empty_stats(),
    )

    stats.setdefault("signals", 0)
    stats.setdefault("tp_wins", 0)
    stats.setdefault("sl_losses", 0)
    stats.setdefault("signal_ids", [])
    stats.setdefault("tp_signal_ids", [])
    stats.setdefault("sl_signal_ids", [])

    return stats


def register_weekly_signal(
    state,
    signal_id,
    signal_time,
):

    try:
        dt = datetime.fromisoformat(
            signal_time
        )
        week_string = get_week_start(dt)
    except Exception:
        week_string = get_week_start(
            utc_datetime()
        )

    stats = get_weekly_stats(
        state,
        week_string,
    )

    if signal_id not in stats["signal_ids"]:
        stats["signal_ids"].append(
            signal_id
        )
        stats["signals"] += 1


def register_weekly_tp(
    state,
    signal_id,
    signal_time,
):

    try:
        dt = datetime.fromisoformat(
            signal_time
        )
        week_string = get_week_start(dt)
    except Exception:
        week_string = get_week_start(
            utc_datetime()
        )

    stats = get_weekly_stats(
        state,
        week_string,
    )

    if signal_id not in stats["tp_signal_ids"]:
        stats["tp_signal_ids"].append(
            signal_id
        )
        stats["tp_wins"] += 1


def register_weekly_sl(
    state,
    signal_id,
    signal_time,
):

    try:
        dt = datetime.fromisoformat(
            signal_time
        )
        week_string = get_week_start(dt)
    except Exception:
        week_string = get_week_start(
            utc_datetime()
        )

    stats = get_weekly_stats(
        state,
        week_string,
    )

    if signal_id not in stats["sl_signal_ids"]:
        stats["sl_signal_ids"].append(
            signal_id
        )
        stats["sl_losses"] += 1


def count_open_weekly_trades(
    state,
    week_string,
):

    count = 0

    for active in state[
        "active"
    ].values():

        signal_time = active.get(
            "signal_time",
            "",
        )

        try:
            dt = datetime.fromisoformat(
                signal_time
            )
            active_week = get_week_start(dt)
        except Exception:
            continue

        if active_week == week_string:
            count += 1

    return count


def register_daily_signal(
    state,
    signal_id,
    signal_time,
):

    try:

        dt = datetime.fromisoformat(
            signal_time
        )

        date_string = dt.strftime(
            "%Y-%m-%d"
        )

    except Exception:

        date_string = utc_date()

    stats = get_daily_stats(
        state,
        date_string,
    )

    if signal_id not in stats[
        "signal_ids"
    ]:

        stats[
            "signal_ids"
        ].append(
            signal_id
        )

        stats["signals"] += 1


def register_monthly_signal(
    state,
    signal_id,
    signal_time,
):

    try:

        dt = datetime.fromisoformat(
            signal_time
        )

        month_string = dt.strftime(
            "%Y-%m"
        )

    except Exception:

        month_string = utc_month()

    stats = get_monthly_stats(
        state,
        month_string,
    )

    if signal_id not in stats[
        "signal_ids"
    ]:

        stats[
            "signal_ids"
        ].append(
            signal_id
        )

        stats["signals"] += 1


def register_daily_tp(
    state,
    signal_id,
    signal_time,
):

    try:

        dt = datetime.fromisoformat(
            signal_time
        )

        date_string = dt.strftime(
            "%Y-%m-%d"
        )

    except Exception:

        date_string = utc_date()

    stats = get_daily_stats(
        state,
        date_string,
    )

    if signal_id not in stats[
        "tp_signal_ids"
    ]:

        stats[
            "tp_signal_ids"
        ].append(
            signal_id
        )

        stats["tp_wins"] += 1


def register_monthly_tp(
    state,
    signal_id,
    signal_time,
):

    try:

        dt = datetime.fromisoformat(
            signal_time
        )

        month_string = dt.strftime(
            "%Y-%m"
        )

    except Exception:

        month_string = utc_month()

    stats = get_monthly_stats(
        state,
        month_string,
    )

    if signal_id not in stats[
        "tp_signal_ids"
    ]:

        stats[
            "tp_signal_ids"
        ].append(
            signal_id
        )

        stats["tp_wins"] += 1


def register_daily_sl(
    state,
    signal_id,
    signal_time,
):

    try:

        dt = datetime.fromisoformat(
            signal_time
        )

        date_string = dt.strftime(
            "%Y-%m-%d"
        )

    except Exception:

        date_string = utc_date()

    stats = get_daily_stats(
        state,
        date_string,
    )

    if signal_id not in stats[
        "sl_signal_ids"
    ]:

        stats[
            "sl_signal_ids"
        ].append(
            signal_id
        )

        stats["sl_losses"] += 1


def register_monthly_sl(
    state,
    signal_id,
    signal_time,
):

    try:

        dt = datetime.fromisoformat(
            signal_time
        )

        month_string = dt.strftime(
            "%Y-%m"
        )

    except Exception:

        month_string = utc_month()

    stats = get_monthly_stats(
        state,
        month_string,
    )

    if signal_id not in stats[
        "sl_signal_ids"
    ]:

        stats[
            "sl_signal_ids"
        ].append(
            signal_id
        )

        stats["sl_losses"] += 1


def count_open_daily_trades(
    state,
    date_string,
):

    count = 0

    for active in state[
        "active"
    ].values():

        signal_time = active.get(
            "signal_time",
            "",
        )

        try:

            dt = datetime.fromisoformat(
                signal_time
            )

            active_date = dt.strftime(
                "%Y-%m-%d"
            )

        except Exception:

            continue

        if active_date == date_string:
            count += 1

    return count


def count_open_monthly_trades(
    state,
    month_string,
):

    count = 0

    for active in state[
        "active"
    ].values():

        signal_time = active.get(
            "signal_time",
            "",
        )

        try:

            dt = datetime.fromisoformat(
                signal_time
            )

            active_month = dt.strftime(
                "%Y-%m"
            )

        except Exception:

            continue

        if active_month == month_string:
            count += 1

    return count


# =========================================================
# BINANCE SYMBOL RESOLUTION
# =========================================================

def normalize_requested_symbol(value):
    value = str(value or "").strip().upper()
    if not value:
        return ""
    value = value.replace("/", "_").replace("-", "_").replace(" ", "")
    if value.endswith("_USDT"):
        value = value[:-5]
    elif value.endswith("USDT"):
        value = value[:-4]
    return value


def read_source_symbols():
    path = COINS_FILE
    if os.path.exists(path):
        rows = []
        with open(path, "r", encoding="utf-8") as f:
            for raw in f:
                raw = raw.strip()
                if not raw or raw.startswith("#"):
                    continue
                rows.append(raw)
        if rows:
            print(f"Binance source file: {path} ({len(rows)} requested symbols)")
            return rows

    print(f"{path} was not found or empty; using embedded fallback list ({len(FALLBACK_SOURCE_SYMBOLS)} symbols).")
    return list(FALLBACK_SOURCE_SYMBOLS)


def binance_json_get(path, params=None):
    url = BINANCE_SPOT_BASE.rstrip("/") + path
    headers = {
        "Accept": "application/json",
        "User-Agent": "CryptoTradeIQ/1.0",
    }
    last_error = None
    for attempt in range(3):
        try:
            response = requests.get(
                url,
                params=params or {},
                headers=headers,
                timeout=BINANCE_TIMEOUT,
            )
            response.raise_for_status()
            payload = response.json()
            if isinstance(payload, dict) and payload.get("code") not in (None, 0):
                raise RuntimeError(str(payload))
            return payload
        except Exception as error:
            last_error = error
            if attempt < 2:
                import time
                time.sleep(0.7 * (attempt + 1))
    raise RuntimeError(f"Binance request failed: {url} | {last_error}")


def get_binance_spot_symbols():
    """Resolve requested coins against Binance Spot USDT markets."""
    requested = read_source_symbols()
    requested_bases = []
    seen = set()

    for item in requested:
        base = normalize_requested_symbol(item)
        if base and base not in seen:
            seen.add(base)
            requested_bases.append(base)

    payload = binance_json_get("/api/v3/exchangeInfo")
    symbols = payload.get("symbols", []) if isinstance(payload, dict) else []

    symbol_map = {}
    for item in symbols:
        if not isinstance(item, dict):
            continue
        if item.get("status") != "TRADING":
            continue
        if item.get("quoteAsset") != "USDT":
            continue
        symbol = str(item.get("symbol") or "").upper()
        base = str(item.get("baseAsset") or "").upper()
        if symbol and base:
            symbol_map.setdefault(base, item)

    rows = []
    missing = []
    for base in requested_bases:
        item = symbol_map.get(base)
        if not item:
            missing.append(base)
            continue
        rows.append({
            "symbol": item["symbol"],
            "name": base,
            "source_symbol": base + "USDT",
            "spot_symbol": item["symbol"],
            "base_currency": base,
            "clear_currency": "USDT",
            "price_tick": next((x.get("tickSize") for x in item.get("filters", []) if x.get("filterType") == "PRICE_FILTER"), None),
            "volume_tick": next((x.get("stepSize") for x in item.get("filters", []) if x.get("filterType") == "LOT_SIZE"), None),
            "min_order_volume": next((x.get("minQty") for x in item.get("filters", []) if x.get("filterType") == "LOT_SIZE"), None),
            "min_order_cost": None,
            "default_leverage": None,
        })

    order = {base: i for i, base in enumerate(requested_bases)}
    rows.sort(key=lambda row: order.get(normalize_requested_symbol(row["source_symbol"]), 999999))

    print(f"Binance requested symbols : {len(requested_bases)}")
    print(f"Binance Spot matched   : {len(rows)}")
    print(f"Binance Spot missing   : {len(missing)}")
    if missing:
        print("Not listed on Binance Spot: " + ", ".join(missing))

    if not rows:
        raise RuntimeError("None of the requested coins are currently available on Binance Spot.")
    return rows


# =========================================================
# BINANCE KLINE DATA
# =========================================================

def _binance_kline(symbol, size, interval):
    requested_size = min(int(size), 1500)
    try:
        payload = binance_json_get(
            "/api/v3/klines",
            {
                "symbol": symbol,
                "interval": interval,
                "limit": requested_size,
            },
        )
        if not isinstance(payload, list) or not payload:
            raise RuntimeError(f"Empty Kline response for {symbol} {interval}")

        rows = []
        for item in payload:
            if not isinstance(item, (list, tuple)) or len(item) < 6:
                continue
            try:
                rows.append({
                    "Time": pd.to_datetime(int(item[0]), unit="ms", utc=True),
                    "Open": float(item[1]),
                    "High": float(item[2]),
                    "Low": float(item[3]),
                    "Close": float(item[4]),
                })
            except (TypeError, ValueError):
                continue

        if not rows:
            raise RuntimeError(f"No usable Kline rows for {symbol} {interval}")

        df = (
            pd.DataFrame(rows)
            .drop_duplicates(subset=["Time"])
            .sort_values("Time")
            .set_index("Time")
        )
        df = df[["Open", "High", "Low", "Close"]].dropna().copy()

        # Ignore the currently forming candle.
        if len(df) > 1:
            df = df.iloc[:-1].copy()
        return df
    except Exception as error:
        print(f"Binance Kline request failed: {symbol} {interval}: {error}")
        return None


_RUN_DATA_CACHE = {}
_RUN_DATA_CACHE_LOCK = __import__("threading").Lock()

def _cached_download(key, loader):
    with _RUN_DATA_CACHE_LOCK:
        if key in _RUN_DATA_CACHE:
            return _RUN_DATA_CACHE[key]
    value = loader()
    with _RUN_DATA_CACHE_LOCK:
        _RUN_DATA_CACHE[key] = value
    return value


def download_4h(symbol):
    return _cached_download(("4h", symbol), lambda: _binance_kline(symbol, BINANCE_4H_BARS, "4h"))


def download_1d(symbol):
    return _cached_download(("1d", symbol), lambda: _binance_kline(symbol, BINANCE_1D_BARS, "1d"))


def latest_daily_order(df):
    if df is None or len(df) < 30:
        return None
    signals = latest_signals(df)
    if not signals:
        return None
    return signals[-1]


def daily_order_matches(
    four_hour_signal,
    daily_signal,
):

    if daily_signal is None:
        return False

    return (
        four_hour_signal["side"]
        == daily_signal["side"]
    )


# =========================================================
# SIGNAL LOGIC
# =========================================================

def canonical_side_from_event(direction):
    """Canonical mapping used by the strategy: bull -> BUY, bear -> SELL."""
    if direction == "bull":
        return "BUY"
    if direction == "bear":
        return "SELL"
    return None


def canonical_side_from_candidate(direction, candidate_candle):
    """Validate the event/candidate combination before a signal can be created."""
    if direction == "bull" and candidate_candle == "BEARISH":
        return "BUY"
    if direction == "bear" and candidate_candle == "BULLISH":
        return "SELL"
    return None


def raw_events(df):

    events = []

    pc = (
        (
            df["Open"]
            - df["Open"].shift(4)
        )
        / df["Open"].shift(4)
        * 100
    )

    last_bull_event = None
    last_bear_event = None

    for i in range(
        5,
        len(df),
    ):

        previous = pc.iloc[i - 1]
        current = pc.iloc[i]

        if (
            pd.isna(previous)
            or pd.isna(current)
        ):

            continue

        bullish = (
            previous <= SENS
            and current > SENS
        )

        bearish = (
            previous >= -SENS
            and current < -SENS
        )

        if bullish:

            if (
                last_bull_event is None
                or (
                    i - last_bull_event
                    >= MIN_EVENT_SEPARATION
                )
            ):

                events.append(
                    ("bull", i)
                )

                last_bull_event = i

        if bearish:

            if (
                last_bear_event is None
                or (
                    i - last_bear_event
                    >= MIN_EVENT_SEPARATION
                )
            ):

                events.append(
                    ("bear", i)
                )

                last_bear_event = i

    return sorted(
        events,
        key=lambda x: x[1],
    )


def build_signal(
    df,
    direction,
    event_idx,
):

    canonical_side = canonical_side_from_event(direction)
    if canonical_side is None:
        return None

    if direction == "bull":

        for offset in range(
            CANDIDATE_START,
            CANDIDATE_END + 1,
        ):

            idx = (
                event_idx
                - offset
            )

            if idx < 0:
                continue

            o = float(
                df["Open"].iloc[idx]
            )

            c = float(
                df["Close"].iloc[idx]
            )

            if c < o:

                low = float(
                    df["Low"].iloc[idx]
                )

                event_time = (
                    df.index[event_idx]
                )

                return {
                    "id": (
                        f"BUY|"
                        f"{event_time.isoformat()}|"
                        f"{idx}|"
                        f"{low:.12f}"
                    ),
                    "side": canonical_side,
                    "event_direction": direction,
                    "candidate_index": idx,
                    "candidate_candle": "BEARISH",
                    "zone_low": low,
                    "sl": low,
                    "event_index": event_idx,
                    "event_time": event_time.isoformat(),
                }

    else:

        for offset in range(
            CANDIDATE_START,
            CANDIDATE_END + 1,
        ):

            idx = (
                event_idx
                - offset
            )

            if idx < 0:
                continue

            o = float(
                df["Open"].iloc[idx]
            )

            c = float(
                df["Close"].iloc[idx]
            )

            if c > o:

                high = float(
                    df["High"].iloc[idx]
                )

                event_time = (
                    df.index[event_idx]
                )

                return {
                    "id": (
                        f"SELL|"
                        f"{event_time.isoformat()}|"
                        f"{idx}|"
                        f"{high:.12f}"
                    ),
                    "side": canonical_side,
                    "event_direction": direction,
                    "candidate_index": idx,
                    "candidate_candle": "BULLISH",
                    "zone_high": high,
                    "sl": high,
                    "event_index": event_idx,
                    "event_time": event_time.isoformat(),
                }

    return None


def latest_signals(df):

    signals = []

    for direction, event_idx in raw_events(df):

        signal = build_signal(
            df,
            direction,
            event_idx,
        )

        if signal:
            signals.append(signal)

    return sorted(
        signals,
        key=lambda signal: (
            signal["event_time"],
            signal["event_index"],
        ),
    )


# =========================================================
# TELEGRAM
# =========================================================

def telegram_targets():

    targets = []

    for key in (
        "TELEGRAM_CHAT_ID",
        "TELEGRAM_CHANNEL_ID",
    ):

        value = os.getenv(
            key,
            "",
        ).strip()

        if (
            value
            and value not in targets
        ):

            targets.append(value)

    return targets


def send_telegram_to_target(
    chat_id,
    text,
    reply_markup=None,
):

    token = os.getenv(
        "TELEGRAM_BOT_TOKEN",
        "",
    ).strip()

    if not token:

        raise RuntimeError(
            "TELEGRAM_BOT_TOKEN is missing."
        )

    url = (
        "https://api.telegram.org/"
        f"bot{token}/sendMessage"
    )

    response = requests.post(
        url,
        json={
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": True,
            **({"reply_markup": reply_markup} if reply_markup else {}),
        },
        timeout=30,
    )

    response.raise_for_status()

    data = response.json()

    if not data.get("ok"):

        raise RuntimeError(
            str(data)
        )

    return True


def deliver_once(
    state,
    delivery_key,
    text,
    reply_markup=None,
):

    targets = telegram_targets()

    if not targets:

        print(
            "No Telegram targets configured."
        )

        return False

    delivery_state = (
        state["deliveries"]
        .setdefault(
            delivery_key,
            {},
        )
    )

    for chat_id in targets:

        if (
            delivery_state.get(
                chat_id
            )
            is True
        ):

            continue

        try:

            send_telegram_to_target(
                chat_id,
                text,
                reply_markup=reply_markup,
            )

            delivery_state[
                chat_id
            ] = True

            print(
                f"Delivered "
                f"{delivery_key} -> "
                f"{chat_id}"
            )

        except Exception as error:

            print(
                "Telegram delivery "
                "failed for "
                f"{chat_id}: {error}"
            )

    return all(
        delivery_state.get(
            chat_id
        )
        is True
        for chat_id in targets
    )


# =========================================================
# SIMULATION / TRADE HELPERS
# =========================================================

def tp_levels():
    levels = [
        ("TP1", TP1_PERCENT),
        ("TP2", TP2_PERCENT),
        ("TP3", TP3_PERCENT),
        ("TP4", TP4_PERCENT),
        ("TP5", TP5_PERCENT),
        ("TP6", TP6_PERCENT),
        ("TP7", TP7_PERCENT),
        ("TP8", TP8_PERCENT),
    ]

    # TP9 to TP33: each level is 10 percentage points higher than the previous TP
    last = TP8_PERCENT
    for n in range(9, 34):
        last += 10.0
        levels.append((f"TP{n}", last))

    # TP34 to TP40: each level is 50% higher than the previous TP
    for n in range(34, 41):
        last *= 1.50
        levels.append((f"TP{n}", last))

    return levels


def tp_levels_for_side(side):
    """Return usable TP levels for a trade side.

    BUY uses the full TP1..TP40 ladder.
    SELL cannot have a target at or below zero, so TP percentages that
    would imply a non-positive price are not used. TP40 is explicitly
    capped at 99% below entry as requested.
    """
    levels = tp_levels()
    if side != "SELL":
        return levels

    result = []
    for name, percent in levels:
        if name == "TP40":
            result.append((name, 99.0))
        elif percent < 100.0:
            result.append((name, percent))
        else:
            # TP33+ would imply a non-positive SELL target.
            # Keep the names available for display, but mark them invalid.
            result.append((name, None))
    return result


def tp_hit_key(tp_name):
    return tp_name.lower().replace(" ", "_") + "_hit"


def trade_pnl(entry, exit_price, side, notional):
    if entry <= 0 or notional <= 0:
        return 0.0
    if side == "BUY":
        return notional * ((exit_price - entry) / entry)
    return notional * ((entry - exit_price) / entry)


def ensure_period_opening_balance(state, period_key):
    portfolio = state["portfolio"]
    openings = portfolio.setdefault("period_opening_balances", {})
    if period_key not in openings:
        openings[period_key] = float(portfolio["balance"])
    return float(openings[period_key])


def update_period_openings(state):
    ensure_period_opening_balance(state, f"DAY|{utc_date()}")
    ensure_period_opening_balance(state, f"MONTH|{utc_month()}")
    ensure_period_opening_balance(state, f"WEEK|{get_week_start(utc_datetime())}")


def record_equity_point(state, reason=""):
    portfolio = state["portfolio"]
    curve = portfolio.setdefault("equity_curve", [])
    point = {
        "time": utc_now(),
        "balance": round(float(portfolio.get("balance", STARTING_BALANCE)), 8),
        "reason": reason,
    }
    # Avoid duplicate points in the same run/time with unchanged balance.
    if curve and curve[-1].get("balance") == point["balance"] and curve[-1].get("reason") == reason:
        return
    curve.append(point)
    if len(curve) > 5000:
        del curve[:-5000]


def allocate_trade(state):
    portfolio = state["portfolio"]
    balance = float(portfolio["balance"])
    margin = FIXED_MARGIN
    notional = margin * LEVERAGE

    # Never exceed the configured maximum number of simulated open positions.
    # Always recover a valid unique number, even if an old state file was
    # manually edited or contains a missing/invalid next_trade_number.
    used = []
    for item in state.get("active", {}).values():
        try:
            if item.get("trade_number") is not None:
                used.append(int(item.get("trade_number")))
        except (TypeError, ValueError):
            pass
    for item in state.get("closed_trades", []):
        try:
            if item.get("trade_number") is not None:
                used.append(int(item.get("trade_number")))
        except (TypeError, ValueError):
            pass

    try:
        candidate = int(portfolio.get("next_trade_number", 1))
    except (TypeError, ValueError):
        candidate = 1
    number = max(candidate, max(used, default=0) + 1)
    portfolio["next_trade_number"] = number + 1
    return number, margin, notional


def close_trade(state, info, active, exit_price, reason, candle_time=None):
    entry = float(active["entry_price"])
    notional = float(active.get("notional", 0.0))
    pnl = trade_pnl(entry, float(exit_price), active["side"], notional)
    portfolio = state["portfolio"]
    balance_before = float(portfolio["balance"])
    balance_after = balance_before + pnl
    portfolio["balance"] = balance_after
    portfolio["total_realized_pnl"] = float(portfolio.get("total_realized_pnl", 0.0)) + pnl
    record_equity_point(state, f"trade #{active.get('trade_number')} {reason}")

    closed = {
        "trade_number": active.get("trade_number"),
        "symbol": info["symbol"],
        "name": info.get("name", info["symbol"]),
        "side": active["side"],
        "signal_id": active["signal_id"],
        "signal_time": active["signal_time"],
        "entry_price": entry,
        "exit_price": float(exit_price),
        "initial_sl": float(active.get("initial_sl", active.get("sl", 0.0))),
        "margin": float(active.get("margin", 0.0)),
        "notional": notional,
        "leverage": LEVERAGE,
        "pnl": pnl,
        "balance_before": balance_before,
        "balance_after": balance_after,
        "reason": reason,
        "exit_time": utc_now(),
        "candle_time": candle_time or "",
        "last_tp_hit": active.get("last_tp_hit", ""),
        "tp_hits": active.get("tp_hits", []),
        "current_sl": float(active.get("current_sl", active.get("sl", 0.0))),
        "initial_balance": float(active.get("balance_at_entry", balance_before)),
        "balance_at_entry": float(active.get("balance_at_entry", balance_before)),
    }
    state["closed_trades"].append(closed)

    signal_time = active["signal_time"]
    if pnl > 1e-12:
        register_daily_tp(state, active["signal_id"], signal_time)
        register_monthly_tp(state, active["signal_id"], signal_time)
        register_weekly_tp(state, active["signal_id"], signal_time)
    elif pnl < -1e-12:
        register_daily_sl(state, active["signal_id"], signal_time)
        register_monthly_sl(state, active["signal_id"], signal_time)
        register_weekly_sl(state, active["signal_id"], signal_time)

    state["active"].pop(info["symbol"], None)
    save_state(state)
    return closed


def trade_details_text(trade, title="📋 جزئیات معامله"):
    pnl = float(trade.get("pnl", 0.0))
    balance = float(trade.get("balance_after", 0.0))
    sign = "+" if pnl >= 0 else ""
    return (
        f"{title}\n\n"
        f"🔢 شماره معامله: #{trade.get('trade_number')}\n"
        f"نماد: {trade.get('symbol')}\n"
        f"جهت: {trade.get('side')}\n"
        f"ورود: {float(trade.get('entry_price', 0)):.12g}\n"
        f"خروج: {float(trade.get('exit_price', 0)):.12g}\n"
        f"مارجین: ${float(trade.get('margin', 0)):.2f}\n"
        f"ارزش پوزیشن: ${float(trade.get('notional', 0)):.2f}\n"
        f"اهرم: {float(trade.get('leverage', LEVERAGE)):g}x\n"
        f"SL اولیه: {float(trade.get('initial_sl', 0)):.12g}\n"
        f"SL فعلی: {float(trade.get('current_sl', trade.get('initial_sl', 0))):.12g}\n"
        f"آخرین TP: {trade.get('last_tp_hit') or 'هیچ‌کدام'}\n"
        f"نتیجه: {trade.get('reason')}\n"
        f"PnL: {sign}${pnl:.2f}\n"
        f"موجودی بعد معامله: ${balance:.2f}\n\n"
        f"{TELEGRAM_SIGNATURE}"
    )


def active_trade_details_text(active, info):
    lines = [
        "📋 جزئیات معامله فعال",
        "",
        f"🔢 شماره معامله: #{active.get('trade_number')}",
        f"نماد: {info.get('symbol')}",
        f"نام: {info.get('name', info.get('symbol'))}",
        f"جهت: {active.get('side')}",
        f"ورود: {float(active.get('entry_price', 0)):.12g}",
        f"SL اولیه: {float(active.get('initial_sl', active.get('sl', 0))):.12g}",
        f"SL فعلی: {float(active.get('current_sl', active.get('sl', 0))):.12g}",
        f"مارجین: ${float(active.get('margin', 0)):.2f}",
        f"ارزش پوزیشن: ${float(active.get('notional', 0)):.2f}",
        f"اهرم: {LEVERAGE:g}x",
        f"موجودی هنگام ورود: ${float(active.get('balance_at_entry', 0)):.2f}",
        f"آخرین TP: {active.get('last_tp_hit') or 'هنوز هیچ‌کدام'}",
        "",
        "وضعیت TP:",
    ]
    for name, percent in tp_levels_for_side(active["side"]):
        key = tp_hit_key(name)
        if percent is None:
            lines.append(f"⬜ {name}: نامعتبر برای SELL")
            continue
        price = calculate_tp_price(float(active["entry_price"]), active["side"], percent)
        mark = "✅" if active.get(key, False) else "⬜"
        lines.append(f"{mark} {name}: {price:.12g} ({percent:g}%)")
    lines.append("")
    lines.append(f"{TELEGRAM_SIGNATURE}")
    return "\n".join(lines)



# =========================================================
# SIGNAL MESSAGE
# =========================================================

def signal_text(info, signal, trade_number=None, margin=None, notional=None):
    side = canonical_side_from_event(signal.get("event_direction")) or signal.get("side")
    if side not in ("BUY", "SELL"):
        raise ValueError(f"Invalid signal side: {side}")
    if signal.get("side") != side:
        raise ValueError(f"Signal side mismatch: event={signal.get('event_direction')} side={signal.get('side')}")
    entry = float(signal["zone_low"] if side == "BUY" else signal["zone_high"])
    lines = [
        "🟢 سیگنال خرید" if side == "BUY" else "🔴 سیگنال فروش",
        "",
        f"🔢 شماره معامله: #{trade_number}" if trade_number is not None else "🔢 شماره معامله: در حال ثبت",
        f"نماد: {info['symbol']}",
        f"نام: {info['name']}",
        "تایم‌فریم: 4H",
        "",
        f"📍 ورود: {entry:.12g}",
        f"🛑 SL اولیه: {float(signal['sl']):.12g}",
    ]
    if margin is not None and notional is not None:
        lines += [f"💵 مارجین: ${margin:.2f}", f"📊 ارزش پوزیشن: ${notional:.2f}", f"⚡ اهرم: {LEVERAGE:g}x"]
    lines += ["", "🎯 اهداف:"]
    # فقط TP1 و TP2 در پیام اصلی نمایش داده می‌شوند.
    levels = tp_levels()
    for name, percent in levels[:2]:
        lines.append(f"{name}: {calculate_tp_price(entry, signal['side'], percent):.12g} (+{percent:g}%)")
    lines += [
        "",
        f"💰 مارجین ثابت: ${float(margin):.2f}" if margin is not None else "",
        f"زمان: {utc_now()}",
        "",
        TELEGRAM_SIGNATURE,
    ]
    return "\n".join(lines)



# =========================================================
# STOP LOSS MESSAGE
# =========================================================

def stop_text(info, active, exit_price=None, pnl=None):
    sign = "+" if pnl is not None and pnl >= 0 else ""
    pnl_line = f"PnL: {sign}${pnl:.2f}" if pnl is not None else ""
    return (
        "🛑 معامله بسته شد روی SL\n\n"
        f"🔢 شماره معامله: #{active.get('trade_number')}\n"
        f"نماد: {info['symbol']}\n"
        f"جهت: {active['side']}\n"
        f"ورود: {float(active['entry_price']):.12g}\n"
        f"SL فعال: {float(active.get('current_sl', active.get('sl'))):.12g}\n"
        + (f"خروج: {float(exit_price):.12g}\n" if exit_price is not None else "")
        + (pnl_line + "\n" if pnl is not None else "")
        + f"\n{TELEGRAM_SIGNATURE}"
    )


def tp_text(info, active, tp_name, tp_percent, tp_price, new_sl):
    return (
        f"🎯 {tp_name} زده شد\n\n"
        f"🔢 شماره معامله: #{active.get('trade_number')}\n"
        f"نماد: {info['symbol']}\n"
        f"جهت: {active['side']}\n"
        f"قیمت هدف: {tp_price:.12g}\n"
        f"هدف: +{tp_percent:g}%\n"
        f"🛡 SL جدید: {new_sl:.12g}\n\n"
        f"{TELEGRAM_SIGNATURE}"
    )


def calculate_tp_price(
    entry_price,
    side,
    percent,
):

    multiplier = (
        percent / 100.0
    )

    if side == "BUY":

        return entry_price * (
            1.0 + multiplier
        )

    return entry_price * (
        1.0 - multiplier
    )


# =========================================================
# TAKE PROFIT CHECK
# =========================================================

def check_take_profits(state, info, df):
    active = state["active"].get(info["symbol"])
    if not active or len(df) < 2:
        return
    # TP is monitored from the newest available candle so the 5-minute
    # GitHub Actions worker can report an intrabar TP hit without waiting
    # for the 4H candle to close. Signal generation itself still uses only
    # completed 4H candles.
    monitor_idx = -1
    high = float(df["High"].iloc[monitor_idx])
    low = float(df["Low"].iloc[monitor_idx])
    try:
        entry = float(active["entry_price"])
        side = active["side"]
    except (KeyError, TypeError, ValueError):
        print(f"{info['symbol']}: active trade has no valid entry_price; skipping TP check.")
        return
    levels = tp_levels_for_side(side)

    for index, (tp_name, tp_percent) in enumerate(levels):
        hit_key = tp_hit_key(tp_name)
        if active.get(hit_key, False):
            continue
        if tp_percent is None:
            continue
        tp_price = calculate_tp_price(entry, side, tp_percent)
        hit = high >= tp_price if side == "BUY" else low <= tp_price
        if not hit:
            continue

        # Trailing rule:
        # TP1 keeps original SL.
        # TP2 moves SL to entry.
        # TP3-TP8 move SL two TP levels back (old behavior).
        # From TP9 onward SL moves to the immediately previous TP.
        # TP40 is the final / FULL TP and closes the trade.
        if index == 0:
            new_sl = float(active.get("current_sl", active.get("initial_sl", active["sl"])))
        elif index == 1:
            new_sl = entry
        elif index >= 8:
            previous_name, previous_percent = levels[index - 1]
            new_sl = calculate_tp_price(entry, side, previous_percent)
        else:
            previous_name, previous_percent = levels[index - 2]
            new_sl = calculate_tp_price(entry, side, previous_percent)

        delivery_key = f"TP|{info['symbol']}|{active['signal_id']}|{tp_name}"
        text = tp_text(info, active, tp_name, tp_percent, tp_price, new_sl)
        if not deliver_once(state, delivery_key, text):
            save_state(state)
            return

        active[hit_key] = True
        active["last_tp_hit"] = tp_name
        active.setdefault("tp_hits", []).append({"name": tp_name, "percent": tp_percent, "price": float(tp_price), "time": utc_now(), "new_sl": float(new_sl)})
        active["current_sl"] = float(new_sl)
        active["sl"] = float(new_sl)
        save_state(state)

        print(f"{info['symbol']}: {tp_name} hit at {tp_price:.12g}; SL -> {new_sl:.12g}")

        if tp_name == "TP40":
            closed = close_trade(state, info, active, tp_price, "FULL TP (TP40)", df.index[monitor_idx].isoformat())
            print(f"{info['symbol']}: TP40 / Full TP closed trade #{closed.get('trade_number')}")
            return


# =========================================================
# STOP LOSS CHECK
# =========================================================

def check_stop(state, info, df):
    active = state["active"].get(info["symbol"])
    if not active or len(df) < 2:
        return

    # User rule: once TP1 has been reached, never send a later SL message
    # for that signal. Higher TP levels continue to be monitored.
    if active.get("tp1_hit", False):
        return

    try:
        # SL is based on the close of a fully completed 4H candle.
        close = float(df["Close"].iloc[-2])
        sl = float(active.get("current_sl", active.get("sl")))
        entry = float(active["entry_price"])
    except (KeyError, TypeError, ValueError):
        print(f"{info['symbol']}: active trade has incomplete pricing data; skipping SL check.")
        return
    hit = close < sl if active["side"] == "BUY" else close > sl
    if not hit:
        return
    candle_time = df.index[-2].isoformat()
    delivery_key = f"STOP|{info['symbol']}|{active['signal_id']}|{candle_time}|{sl:.12g}"
    # PnL is calculated once, after the stop notification is successfully delivered.
    pnl = trade_pnl(float(active["entry_price"]), sl, active["side"], float(active.get("notional", 0.0)))
    text = stop_text(info, active, sl, pnl)
    if not deliver_once(state, delivery_key, text):
        save_state(state)
        return
    close_trade(state, info, active, sl, "TRAILING SL" if active.get("last_tp_hit") else "INITIAL SL", candle_time)
    print(f"Stop loss sent for {info['symbol']} trade #{active.get('trade_number')}")


def portfolio_metrics(state):
    curve = state["portfolio"].get("equity_curve", [])
    balances = [float(x.get("balance", 0.0)) for x in curve if x.get("balance") is not None]
    if not balances:
        balances = [float(state["portfolio"].get("balance", STARTING_BALANCE))]
    peak = balances[0]
    max_drawdown = 0.0
    for b in balances:
        peak = max(peak, b)
        if peak > 0:
            max_drawdown = max(max_drawdown, (peak - b) / peak * 100.0)
    return max(balances), max_drawdown


# =========================================================
# DAILY REPORT
# =========================================================

def daily_report_text(state, report_date):
    stats = get_daily_stats(state, report_date)
    opening = float(state["portfolio"].get("period_opening_balances", {}).get(f"DAY|{report_date}", state["portfolio"]["initial_balance"]))
    closing = float(state["portfolio"]["balance"])
    pnl = closing - opening
    closed = [t for t in state.get("closed_trades", []) if str(t.get("exit_time", "")).startswith(report_date)]
    wins = sum(1 for t in closed if float(t.get("pnl", 0)) > 0)
    losses = sum(1 for t in closed if float(t.get("pnl", 0)) < 0)
    best = max((float(t.get("pnl", 0)) for t in closed), default=0.0)
    worst = min((float(t.get("pnl", 0)) for t in closed), default=0.0)
    peak, drawdown = portfolio_metrics(state)
    return (f"📊 گزارش روزانه\n\n📅 تاریخ: {report_date}\n\n"
            f"💰 موجودی ابتدای روز: ${opening:.2f}\n"
            f"💰 موجودی فعلی: ${closing:.2f}\n"
            f"📈 سود/ضرر روز: {'+' if pnl >= 0 else ''}${pnl:.2f}\n"
            f"📊 بازده روز: {(pnl/opening*100 if opening else 0):.2f}%\n\n"
            f"📨 سیگنال‌های امروز: {int(stats.get('signals',0))}\n"
            f"✅ معاملات بسته‌شده سودده: {wins}\n"
            f"❌ معاملات بسته‌شده زیان‌ده: {losses}\n"
            f"🏆 بهترین معامله: ${best:+.2f}\n"
            f"💥 بدترین معامله: ${worst:+.2f}\n"
            f"📈 بیشترین موجودی ثبت‌شده: ${peak:.2f}\n"
            f"📉 Max Drawdown: {drawdown:.2f}%\n"
            f"📂 معاملات باز: {len(state.get('active',{}))}\n\n{TELEGRAM_SIGNATURE}")


def should_send_daily_report():

    now = utc_datetime()

    if now.hour != DAILY_REPORT_HOUR_UTC:
        return False

    if now.minute < DAILY_REPORT_MINUTE_UTC:
        return False

    return True


def send_daily_report(
    state,
):

    if not should_send_daily_report():
        return

    today = utc_date()

    if state.get(
        "last_daily_report",
        "",
    ) == today:

        return

    text = daily_report_text(
        state,
        today,
    )

    delivery_key = (
        f"DAILY_REPORT|{today}"
    )

    if deliver_once(
        state,
        delivery_key,
        text,
    ):

        state[
            "last_daily_report"
        ] = today

        save_state(state)

        print(
            f"Daily report sent for "
            f"{today}"
        )


# =========================================================
# MONTHLY REPORT
# =========================================================

def monthly_report_text(state, month_string):
    opening = float(state["portfolio"].get("period_opening_balances", {}).get(f"MONTH|{month_string}", state["portfolio"]["initial_balance"]))
    trades = [t for t in state.get("closed_trades", []) if str(t.get("exit_time", "")).startswith(month_string)]
    pnl = sum(float(t.get("pnl",0)) for t in trades)
    closing = opening + pnl
    wins = sum(1 for t in trades if float(t.get("pnl",0)) > 0)
    losses = sum(1 for t in trades if float(t.get("pnl",0)) < 0)
    best = max((float(t.get("pnl", 0)) for t in trades), default=0.0)
    worst = min((float(t.get("pnl", 0)) for t in trades), default=0.0)
    peak, drawdown = portfolio_metrics(state)
    return (f"📊 گزارش ماهانه\n\n📅 ماه: {month_string}\n\n"
            f"💰 موجودی ابتدای ماه: ${opening:.2f}\n"
            f"💰 موجودی پایان ماه: ${closing:.2f}\n"
            f"📈 سود/ضرر ماه: {'+' if pnl >= 0 else ''}${pnl:.2f}\n"
            f"📊 بازده ماه: {(pnl/opening*100 if opening else 0):.2f}%\n\n"
            f"📊 معاملات بسته‌شده: {len(trades)}\n"
            f"✅ سودده: {wins}\n❌ زیان‌ده: {losses}\n"
            f"🏆 بهترین معامله: ${best:+.2f}\n💥 بدترین معامله: ${worst:+.2f}\n"
            f"📈 بیشترین موجودی: ${peak:.2f}\n📉 Max Drawdown: {drawdown:.2f}%\n\n{TELEGRAM_SIGNATURE}")


def should_send_monthly_report():

    now = utc_datetime()

    # گزارش ماه قبل در روز اول ماه جدید
    if now.day != 1:
        return False

    if now.hour != MONTHLY_REPORT_HOUR_UTC:
        return False

    if now.minute < MONTHLY_REPORT_MINUTE_UTC:
        return False

    return True


def get_previous_month():

    now = utc_datetime()

    first_day_current = now.replace(
        day=1,
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )

    previous_month_last_day = (
        first_day_current
        - pd.Timedelta(days=1)
    )

    return previous_month_last_day.strftime(
        "%Y-%m"
    )


def send_monthly_report(
    state,
):

    if not should_send_monthly_report():
        return

    # در روز اول ماه، گزارش ماه قبل ارسال می‌شود.
    report_month = get_previous_month()

    if state.get(
        "last_monthly_report",
        "",
    ) == report_month:

        return

    text = monthly_report_text(
        state,
        report_month,
    )

    delivery_key = (
        f"MONTHLY_REPORT|"
        f"{report_month}"
    )

    if deliver_once(
        state,
        delivery_key,
        text,
    ):

        state[
            "last_monthly_report"
        ] = report_month

        save_state(state)

        print(
            f"Monthly report sent for "
            f"{report_month}"
        )


# =========================================================
# WEEKLY REPORT
# =========================================================

def previous_week_start():
    now = utc_datetime()
    current_week_start = (
        now
        - pd.Timedelta(days=int(now.weekday()))
    ).replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )

    previous_week = (
        current_week_start
        - pd.Timedelta(days=7)
    )

    return previous_week.strftime(
        "%Y-%m-%d"
    )


def weekly_report_text(state, week_string):
    try:
        start = datetime.strptime(week_string, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        end = start + pd.Timedelta(days=7)
    except Exception:
        start = utc_datetime() - pd.Timedelta(days=7)
        end = utc_datetime()
    trades = []
    for t in state.get("closed_trades", []):
        try:
            dt = datetime.fromisoformat(str(t.get("exit_time", "")).replace(" UTC", "+00:00"))
            if start <= dt < end:
                trades.append(t)
        except Exception:
            pass
    opening = float(state["portfolio"].get("period_opening_balances", {}).get(f"WEEK|{week_string}", state["portfolio"]["initial_balance"]))
    pnl = sum(float(t.get("pnl",0)) for t in trades)
    closing = opening + pnl
    wins = sum(1 for t in trades if float(t.get("pnl",0)) > 0)
    losses = sum(1 for t in trades if float(t.get("pnl",0)) < 0)
    best = max((float(t.get("pnl", 0)) for t in trades), default=0.0)
    worst = min((float(t.get("pnl", 0)) for t in trades), default=0.0)
    peak, drawdown = portfolio_metrics(state)
    return (f"📊 گزارش هفتگی\n\n📅 هفته: {week_string} تا {(start + pd.Timedelta(days=6)).strftime('%Y-%m-%d')}\n\n"
            f"💰 موجودی ابتدای هفته: ${opening:.2f}\n"
            f"💰 موجودی پایان هفته: ${closing:.2f}\n"
            f"📈 سود/ضرر هفته: {'+' if pnl >= 0 else ''}${pnl:.2f}\n"
            f"📊 بازده هفته: {(pnl/opening*100 if opening else 0):.2f}%\n\n"
            f"📊 معاملات بسته‌شده: {len(trades)}\n"
            f"✅ سودده: {wins}\n❌ زیان‌ده: {losses}\n"
            f"🏆 بهترین معامله: ${best:+.2f}\n💥 بدترین معامله: ${worst:+.2f}\n"
            f"📈 بیشترین موجودی: ${peak:.2f}\n📉 Max Drawdown: {drawdown:.2f}%\n\n{TELEGRAM_SIGNATURE}")


def should_send_weekly_report():

    now = utc_datetime()

    # دوشنبه
    if now.weekday() != 0:
        return False

    if now.hour != WEEKLY_REPORT_HOUR_UTC:
        return False

    if now.minute < WEEKLY_REPORT_MINUTE_UTC:
        return False

    return True


def send_weekly_report(
    state,
):

    if not should_send_weekly_report():
        return

    report_week = previous_week_start()

    if state.get(
        "last_weekly_report",
        "",
    ) == report_week:
        return

    text = weekly_report_text(
        state,
        report_week,
    )

    delivery_key = (
        f"WEEKLY_REPORT|"
        f"{report_week}"
    )

    if deliver_once(
        state,
        delivery_key,
        text,
    ):

        state[
            "last_weekly_report"
        ] = report_week

        save_state(state)

        print(
            f"Weekly report sent for "
            f"{report_week}"
        )


def signal_ob_distance_percent(signal):
    """Return the distance from the event close to the selected OB edge.

    build_signal stores the event index but not event_close, so this helper
    also accepts event_close when present and otherwise returns 0.0.
    analyze_symbol supplies the DataFrame value before calling the filter.
    """
    value = signal.get("ob_distance_percent")
    if value is not None:
        try:
            return float(value)
        except (TypeError, ValueError):
            pass
    return 0.0


def signal_ob_distance_allowed(signal):
    return signal_ob_distance_percent(signal) <= MAX_OB_DISTANCE_PERCENT


def add_ob_distance(signal, df):
    """Calculate and attach the original OB-distance filter value."""
    try:
        event_idx = int(signal.get("event_index", -1))
        event_close = float(df["Close"].iloc[event_idx])
        if signal["side"] == "BUY":
            ob = float(signal["zone_low"])
        else:
            ob = float(signal["zone_high"])
        if ob <= 0:
            distance = 999.0
        else:
            distance = abs(event_close - ob) / ob * 100.0
        signal["event_close"] = event_close
        signal["ob_distance_percent"] = distance
    except Exception:
        signal["ob_distance_percent"] = 999.0
    return signal


# =========================================================
# SIGNAL DELIVERY
# =========================================================


# =========================================================
# ANALYZE SYMBOL
# =========================================================

def analyze_symbol(state, info, df):
    check_take_profits(state, info, df)
    check_stop(state, info, df)

    signals = latest_signals(df)
    if not signals:
        return

    symbol = info["symbol"]
    latest = signals[-1]

    # HARD DIRECTION CHECK:
    # bull event + bearish candidate candle = BUY only.
    # bear event + bullish candidate candle = SELL only.
    # Never allow a signal side that contradicts its source event/candle.
    expected_side = canonical_side_from_event(latest.get("event_direction"))
    expected_candidate_side = canonical_side_from_candidate(
        latest.get("event_direction"),
        latest.get("candidate_candle"),
    )
    if expected_side is None or expected_candidate_side is None or expected_side != expected_candidate_side:
        print(
            f"{symbol}: rejected invalid direction combination "
            f"event={latest.get('event_direction')} "
            f"candidate={latest.get('candidate_candle')}"
        )
        return
    if latest.get("side") != expected_side:
        print(
            f"{symbol}: rejected inconsistent signal side={latest.get('side')} "
            f"expected={expected_side}"
        )
        return

    old_event = state["last_event"].get(symbol)
    old_signal = state["last_signal"].get(symbol)

    # FIRST TIME THIS SYMBOL IS SEEN: establish a baseline only.
    # This prevents old/historical signals from being sent after a fresh
    # state file, after adding a new coin, or after the state is missing
    # that symbol.
    if symbol not in state.get("signal_baseline", {}):
        state.setdefault("signal_baseline", {})[symbol] = latest["event_time"]
        state["last_event"][symbol] = latest["event_time"]
        state["last_signal"][symbol] = latest["id"]
        print(f"{symbol}: baseline set to {latest['event_time']} ({latest['side']}); no old signal sent.")
        return

    # Only the latest event after the stored event is eligible.  We never
    # walk through a backlog of historical signals.
    if old_event and latest["event_time"] <= old_event:
        return

    if old_signal and latest["id"] == old_signal:
        return

    signal = latest

    # Advance the per-symbol cursor BEFORE any filters. This guarantees that
    # an old signal which fails a filter cannot be re-sent on every run.
    state["last_event"][symbol] = signal["event_time"]
    state["last_signal"][symbol] = signal["id"]

    # Calculate the OB distance before applying the 4% filter.
    signal = add_ob_distance(signal, df)
    ob_distance = signal_ob_distance_percent(signal)
    if not signal_ob_distance_allowed(signal):
        print(f"{info['symbol']}: signal disabled; OB distance {ob_distance:.2f}% > {MAX_OB_DISTANCE_PERCENT:.2f}%.")
        save_state(state)
        return

    daily_df = download_1d(info["symbol"])
    daily_signal = latest_daily_order(daily_df)
    if not daily_order_matches(signal, daily_signal):
        print(f"{info['symbol']}: {signal['side']} 4H signal disabled because Daily order is missing/opposite.")
        save_state(state)
        return

    # One active trade per symbol, preserving the existing state model.
    if info["symbol"] in state["active"]:
        print(f"{info['symbol']}: active trade already exists; new signal ignored.")
        save_state(state)
        return

    if signal["side"] == "BUY":
        entry_price = float(signal["zone_low"])
    else:
        entry_price = float(signal["zone_high"])

    try:
        trade_number, margin, notional = allocate_trade(state)
    except RuntimeError as error:
        print(f"{info['symbol']}: signal skipped: {error}")
        save_state(state)
        return

    text = signal_text(info, signal, trade_number, margin, notional)
    # TP1 is the button that reveals TP3 through TP40. TP2 remains visible in the main message.
    reply_markup = {
        "inline_keyboard": [
            [{"text": "🎯 TP1 — نمایش TP3 تا TP40", "callback_data": f"SHOW_TPS:{trade_number}"}]
        ]
    }
    delivery_key = f"SIGNAL|{info['symbol']}|{signal['id']}"
    delivered = deliver_once(state, delivery_key, text, reply_markup=reply_markup)

    if not delivered:
        # Keep the allocated trade number consumed. A partial Telegram delivery
        # must never allow the same trade number to be reused later.
        save_state(state)
        return

    active = {
        "trade_number": trade_number,
        "symbol": info["symbol"],
        "name": info.get("name", info["symbol"]),
        "signal_id": signal["id"],
        "side": signal["side"],
        "initial_sl": float(signal["sl"]),
        "current_sl": float(signal["sl"]),
        "sl": float(signal["sl"]),
        "entry_price": entry_price,
        "signal_time": signal["event_time"],
        "margin": margin,
        "notional": notional,
        "leverage": LEVERAGE,
        "tp1_hit": False,
        "tp2_hit": False,
        "tp3_hit": False,
        "tp4_hit": False,
        "tp5_hit": False,
        "tp6_hit": False,
        "tp7_hit": False,
        "tp8_hit": False,
        "full_tp_hit": False,
        "last_tp_hit": "",
        "tp_hits": [],
        "balance_at_entry": float(state["portfolio"]["balance"]),
    }
    for n in range(1, 41):
        active.setdefault(f"tp{n}_hit", False)

    state["last_event"][info["symbol"]] = signal["event_time"]
    state["last_signal"][info["symbol"]] = signal["id"]
    state["active"][info["symbol"]] = active

    register_daily_signal(state, signal["id"], signal["event_time"])
    register_monthly_signal(state, signal["id"], signal["event_time"])
    register_weekly_signal(state, signal["id"], signal["event_time"])
    save_state(state)
    print(f"{info['symbol']}: new {signal['side']} trade #{trade_number} active at {entry_price:.12g}")


# =========================================================
# TELEGRAM TRADE LOOKUP
# =========================================================

def authorized_chat(chat_id):
    return str(chat_id) in set(telegram_targets())


def find_trade(state, number):
    for active in state.get("active", {}).values():
        if int(active.get("trade_number", -1)) == number:
            return ("active", active)
    for trade in reversed(state.get("closed_trades", [])):
        if int(trade.get("trade_number", -1)) == number:
            return ("closed", trade)
    return (None, None)


# =========================================================
# MAIN
# =========================================================

def main():

    state = load_state()
    update_period_openings(state)
    record_equity_point(state, "heartbeat")
    # Telegram commands are handled by the separate command workflow.

    binance_symbols = get_binance_spot_symbols()

    workers = max(1, min(BINANCE_SCAN_WORKERS, 32))
    print(f"Binance parallel scan workers: {workers}")

    results = {}
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_map = {executor.submit(download_4h, info["symbol"]): info for info in binance_symbols}
        for future in as_completed(future_map):
            info = future_map[future]
            symbol = info["symbol"]
            try:
                results[symbol] = future.result()
            except Exception as error:
                results[symbol] = None
                print(f"{symbol}: ERROR: {error}")

    for info in binance_symbols:
        symbol = info["symbol"]
        df = results.get(symbol)
        if df is None or len(df) < 30:
            print(f"{symbol}: insufficient data")
            continue
        print(f"{symbol} rows={len(df)}")
        try:
            analyze_symbol(state, info, df)
        except Exception as error:
            print(f"{symbol}: ERROR: {error}")

    # گزارش روزانه
    if not state[
        "initialized"
    ]:

        state[
            "initialized"
        ] = True

    send_daily_report(state)
    send_monthly_report(state)
    send_weekly_report(state)

    save_state(state)

    print(
        "Crypto scan completed."
    )


if __name__ == "__main__":

    main()
