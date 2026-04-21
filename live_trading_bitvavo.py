"""
live_trading_bitvavo.py  —  DCA Live Trading Dashboard (Bitvavo)
────────────────────────────────────────────────────────────────
Executes the DCA strategy live on Bitvavo using real market orders.
REAL MONEY IS AT RISK. Test thoroughly with paper trading first.

Requires: pip install python-bitvavo-api

State (open trade, closed trades, log) is persisted to
  live_state.json  next to this script, so the bot survives restarts.

Run with:
  streamlit run live_trading_bitvavo.py
"""

import json
import time
import datetime as dt
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import streamlit as st

AMS = ZoneInfo("Europe/Amsterdam")

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────
INTERVAL_MINUTES: dict[str, int] = {
    "1m": 1, "5m": 5, "15m": 15, "30m": 30,
    "1h": 60, "2h": 120, "4h": 240, "6h": 360,
    "8h": 480, "12h": 720, "1d": 1440, "3d": 4320, "1w": 10080,
}
BASE_URL   = "https://api.bitvavo.com/v2"
LOOKBACK   = 500
STATE_FILE = Path(__file__).parent / "live_state.json"


# ─────────────────────────────────────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(layout="wide", page_title="DCA Live Trader — Bitvavo")
st.title("⚡ DCA Live Trading — Bitvavo")

st.error(
    "⚠️ **LIVE TRADING MODE — Real money is at risk.** "
    "All orders are placed on Bitvavo with real funds. "
    "Make sure you have tested this strategy with paper trading first.",
    icon="🚨",
)


# ─────────────────────────────────────────────────────────────────────────────
# STATE MANAGEMENT
# ─────────────────────────────────────────────────────────────────────────────
def _default_state() -> dict:
    return {
        "open_trade":       None,
        "closed_trades":    [],
        "last_candle_time": int(time.time() * 1000),
        "trade_number":     1,
        "log":              [],
        "bot_running":      False,
    }

def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return _default_state()

def save_state(state: dict) -> None:
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2, default=str)

def _log(state: dict, msg: str) -> None:
    ts = datetime.now(AMS).strftime("%Y-%m-%d %H:%M:%S")
    state["log"].insert(0, f"[{ts}] {msg}")
    state["log"] = state["log"][:200]


# ─────────────────────────────────────────────────────────────────────────────
# API CREDENTIALS  (read from .streamlit/secrets.toml — never from the UI)
# ─────────────────────────────────────────────────────────────────────────────
try:
    api_key    = st.secrets["BITVAVO_API_KEY"]
    api_secret = st.secrets["BITVAVO_API_SECRET"]
except KeyError:
    st.error("API credentials not found. Add BITVAVO_API_KEY and BITVAVO_API_SECRET to .streamlit/secrets.toml")
    st.stop()

# ─────────────────────────────────────────────────────────────────────────────
# BITVAVO CLIENT
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_resource
def _get_bitvavo(key: str, secret: str):
    try:
        from python_bitvavo_api.bitvavo import Bitvavo as _Bitvavo
        return _Bitvavo({"APIKEY": key, "APISECRET": secret})
    except ImportError:
        st.error("python-bitvavo-api not installed. Run: pip install python-bitvavo-api")
        return None

bitvavo = _get_bitvavo(api_key, api_secret)

def _bitvavo_ready() -> bool:
    return bitvavo is not None and bool(api_key) and bool(api_secret)


# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR — STRATEGY SETTINGS
# ─────────────────────────────────────────────────────────────────────────────
st.sidebar.header("⚙️ Strategy Settings")

trading_pair = st.sidebar.selectbox(
    "Trading Pair", ["BTC-EUR", "ETH-EUR", "XRP-EUR", "SOL-EUR"], key="lt_pair"
)
interval = st.sidebar.selectbox(
    "Candle Timeframe", list(INTERVAL_MINUTES.keys()), index=10, key="lt_interval",
)

# ── Order sizing ──────────────────────────────────────────────────────────────
st.sidebar.markdown("**Order Sizing**")
order_mode = st.sidebar.selectbox("Order Size Mode", ["Percentage", "Fixed EUR"], key="lt_om")
if order_mode == "Percentage":
    base_order_value   = st.sidebar.number_input(
        "Base Order (%)", value=5.6, min_value=0.1, step=0.1, key="lt_base",
        help="Percentage of available EUR balance")
    safety_order_value = st.sidebar.number_input(
        "Safety Order (%)", value=6.3, min_value=0.1, step=0.1, key="lt_so")
else:
    base_order_value   = st.sidebar.number_input(
        "Base Order (EUR)", value=50.0, min_value=1.0, step=10.0, key="lt_base")
    safety_order_value = st.sidebar.number_input(
        "Safety Order (EUR)", value=50.0, min_value=1.0, step=10.0, key="lt_so")

# ── DCA parameters ────────────────────────────────────────────────────────────
st.sidebar.markdown("**DCA Parameters**")
deviation_pct     = st.sidebar.number_input("Price Deviation (%)", value=0.37, min_value=0.01, step=0.01, key="lt_dev")
step_multiplier   = st.sidebar.number_input("Step Multiplier",      value=0.9,  min_value=0.1,  step=0.1,  key="lt_step")
volume_scale      = st.sidebar.number_input("Volume Scale",          value=1.26, min_value=1.0,  step=0.01, key="lt_vs")
take_profit_pct   = st.sidebar.number_input("Take Profit (%)",       value=1.46, min_value=0.01, step=0.01, key="lt_tp")
max_safety_orders = st.sidebar.slider("Max Safety Orders", 1, 20, 10, key="lt_maxso")

# ── Entry filters ─────────────────────────────────────────────────────────────
st.sidebar.markdown("**Entry Filters**")
rsi_filter    = st.sidebar.checkbox("RSI Filter", value=False, key="lt_rsi_f")
rsi_period    = st.sidebar.number_input("RSI Period",    value=14,   min_value=2,   step=1,   key="lt_rsi_p",  disabled=not rsi_filter)
rsi_threshold = st.sidebar.number_input("RSI Threshold", value=55.0, min_value=1.0, max_value=99.0, step=1.0, key="lt_rsi_t", disabled=not rsi_filter)

sma_filter = st.sidebar.checkbox("SMA Filter", value=False, key="lt_sma_f")
sma_period = st.sidebar.number_input("SMA Period", value=50, min_value=2, step=1, key="lt_sma_p", disabled=not sma_filter)

ema_filter = st.sidebar.checkbox("EMA Filter", value=False, key="lt_ema_f")
ema_period = st.sidebar.number_input("EMA Period", value=21, min_value=2, step=1, key="lt_ema_p", disabled=not ema_filter)

volume_filter     = st.sidebar.checkbox("Volume Filter", value=False, key="lt_vol_f")
volume_ma_period  = st.sidebar.number_input("Volume MA Period",  value=20,  min_value=2, step=1,   key="lt_vol_p", disabled=not volume_filter)
volume_multiplier = st.sidebar.number_input("Volume Multiplier", value=1.0, min_value=0.1, step=0.1, key="lt_vol_m", disabled=not volume_filter)

bb_filter = st.sidebar.checkbox("Bollinger Bands Filter", value=False, key="lt_bb_f")
bb_period = st.sidebar.number_input("BB Period",  value=20,  min_value=2, step=1,   key="lt_bb_p", disabled=not bb_filter)
bb_std    = st.sidebar.number_input("BB Std Dev", value=2.0, min_value=0.1, step=0.1, key="lt_bb_s", disabled=not bb_filter)

macd_filter = st.sidebar.checkbox("MACD Filter", value=False, key="lt_macd_f")
macd_fast   = st.sidebar.number_input("MACD Fast",   value=12, min_value=2, step=1, key="lt_macd_fs", disabled=not macd_filter)
macd_slow   = st.sidebar.number_input("MACD Slow",   value=26, min_value=2, step=1, key="lt_macd_sl", disabled=not macd_filter)
macd_signal = st.sidebar.number_input("MACD Signal", value=9,  min_value=2, step=1, key="lt_macd_sg", disabled=not macd_filter)
macd_mode   = st.sidebar.selectbox(
    "MACD Entry Mode", ["histogram_positive", "macd_above_signal", "macd_above_zero"],
    key="lt_macd_m", disabled=not macd_filter,
)

bos_filter   = st.sidebar.checkbox("BOS Filter", value=False, key="lt_bos_f")
bos_lookback = st.sidebar.number_input("BOS Lookback", value=10, min_value=2, step=1, key="lt_bos_lb", disabled=not bos_filter)
bos_recency  = st.sidebar.number_input("BOS Recency (candles)", value=5, min_value=1, step=1, key="lt_bos_r", disabled=not bos_filter)

sr_filter        = st.sidebar.checkbox("S&R Filter", value=True, key="lt_sr_f")
sr_lookback      = st.sidebar.number_input("S&R Lookback (candles)", value=5, min_value=1, step=1, key="lt_sr_lb", disabled=not sr_filter)
sr_proximity_pct = st.sidebar.number_input("Proximity to Support (%)", value=9.8, min_value=0.1, step=0.1, key="lt_sr_p", disabled=not sr_filter)

ma_cross_filter = st.sidebar.checkbox("MA Cross Filter", value=False, key="lt_mac_f")
ma_cross_type   = st.sidebar.selectbox("MA Type", ["SMA", "EMA"], key="lt_mac_t", disabled=not ma_cross_filter)
ma_fast_period  = st.sidebar.number_input("Fast MA Period", value=50,  min_value=2, step=1, key="lt_mac_fp", disabled=not ma_cross_filter)
ma_slow_period  = st.sidebar.number_input("Slow MA Period", value=200, min_value=2, step=1, key="lt_mac_sp", disabled=not ma_cross_filter)
ma_cross_mode   = st.sidebar.selectbox(
    "MA Cross Mode", ["golden_cross_regime", "fresh_crossover"],
    key="lt_mac_m", disabled=not ma_cross_filter,
)

atr_dynamic = st.sidebar.checkbox("ATR Dynamic TP/SL", value=False, key="lt_atr_f")
atr_period  = st.sidebar.number_input("ATR Period",        value=14,  min_value=2,   step=1,   key="lt_atr_p",  disabled=not atr_dynamic)
atr_tp_mult = st.sidebar.number_input("ATR TP Multiplier", value=2.0, min_value=0.1, step=0.1, key="lt_atr_tp", disabled=not atr_dynamic)
atr_sl_mult = st.sidebar.number_input("ATR SL Multiplier", value=1.5, min_value=0.1, step=0.1, key="lt_atr_sl", disabled=not atr_dynamic)

# ── Exit options ──────────────────────────────────────────────────────────────
st.sidebar.markdown("**Exit Options**")
trailing_tp       = st.sidebar.checkbox("Trailing Take Profit", value=False, key="lt_ttp")
trail_pct         = st.sidebar.number_input("Trail Distance (%)", value=0.3, min_value=0.05, step=0.05, key="lt_trail", disabled=not trailing_tp)
stop_loss_enabled = st.sidebar.checkbox("Stop Loss", value=True, key="lt_sl_e")
stop_loss_pct     = st.sidebar.number_input("Stop Loss (%)", value=17.5, min_value=0.1, step=0.5, key="lt_sl_p", disabled=not stop_loss_enabled)
time_stop_enabled = st.sidebar.checkbox("Time Stop", value=False, key="lt_ts_e")
time_stop_hours   = st.sidebar.number_input("Max Duration (hours)", value=24, min_value=1, step=1, key="lt_ts_h", disabled=not time_stop_enabled)

compounding = st.sidebar.checkbox("Enable Compounding", value=True, key="lt_comp")

# ── Bot controls ──────────────────────────────────────────────────────────────
st.sidebar.divider()
st.sidebar.markdown("**Bot Controls**")
refresh_secs = st.sidebar.selectbox(
    "Auto-refresh interval",
    [10, 30, 60, 120, 300], index=2,
    format_func=lambda x: f"{x}s" if x < 60 else f"{x // 60}m",
    key="lt_refresh",
)
_state_preview     = load_state()
_persisted_running = _state_preview.get("bot_running", False)
bot_running = st.sidebar.toggle(
    "▶️ Bot Running", value=_persisted_running, key="lt_running",
    help="When ON the bot checks for signals and places real orders automatically",
)
if st.sidebar.button("🔄 Reset — clear all trades & log", key="lt_reset"):
    fresh = _default_state()
    save_state(fresh)
    for _k in list(st.session_state.keys()):
        if _k.startswith("_lt_state"):
            del st.session_state[_k]
    st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# INDICATORS  (identical to paper trading)
# ─────────────────────────────────────────────────────────────────────────────
def compute_rsi(closes: pd.Series, period: int) -> pd.Series:
    delta    = closes.diff()
    gain     = delta.clip(lower=0)
    loss     = (-delta).clip(lower=0)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period).mean()
    rs       = avg_gain / avg_loss.replace(0, 1e-10)
    return (100 - 100 / (1 + rs)).where(avg_loss > 0, 100)

def compute_sma(series: pd.Series, period: int) -> pd.Series:
    return series.rolling(period).mean()

def compute_ema(closes: pd.Series, period: int) -> pd.Series:
    return closes.ewm(span=period, adjust=False).mean()

def compute_bollinger_bands(closes: pd.Series, period: int, num_std: float) -> tuple:
    middle = closes.rolling(period).mean()
    std    = closes.rolling(period).std(ddof=0)
    return middle + num_std * std, middle, middle - num_std * std

def compute_macd(closes: pd.Series, fast: int, slow: int, signal: int) -> tuple:
    fast_ema    = closes.ewm(span=fast,   adjust=False).mean()
    slow_ema    = closes.ewm(span=slow,   adjust=False).mean()
    macd_line   = fast_ema - slow_ema
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    return macd_line, signal_line, macd_line - signal_line

def compute_bos(highs: pd.Series, lows: pd.Series, closes: pd.Series, swing_lookback: int) -> pd.Series:
    lb  = swing_lookback
    win = 2 * lb + 1
    swing_high = highs.rolling(win, center=True).max()
    swing_low  = lows.rolling(win,  center=True).min()
    prev_sh = swing_high.shift(lb + 1).ffill()
    prev_sl = swing_low.shift(lb  + 1).ffill()
    bos = pd.Series(0, index=closes.index, dtype=int)
    bos[closes > prev_sh] =  1
    bos[closes < prev_sl] = -1
    return bos

def compute_support(lows: pd.Series, lookback: int) -> pd.Series:
    return lows.rolling(lookback).min()

def compute_atr(highs: pd.Series, lows: pd.Series, closes: pd.Series, period: int) -> pd.Series:
    prev_close = closes.shift(1)
    tr = pd.concat([
        highs - lows,
        (highs - prev_close).abs(),
        (lows  - prev_close).abs(),
    ], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / period, min_periods=period).mean()


# ─────────────────────────────────────────────────────────────────────────────
# CANDLE FETCHING  (public endpoint — no API key needed)
# ─────────────────────────────────────────────────────────────────────────────
def fetch_candles(market: str, itvl: str, n: int = LOOKBACK) -> list:
    import requests
    try:
        resp = requests.get(
            f"{BASE_URL}/{market}/candles",
            params={"interval": itvl, "limit": n},
            timeout=10,
        )
        data = resp.json()
    except Exception:
        return []
    if not isinstance(data, list):
        return []
    return sorted(
        [{"time": int(c[0]), "open": float(c[1]), "high": float(c[2]),
          "low": float(c[3]), "close": float(c[4]),
          "volume": float(c[5]) if len(c) > 5 else 0.0}
         for c in data],
        key=lambda x: x["time"],
    )


# ─────────────────────────────────────────────────────────────────────────────
# BITVAVO ACCOUNT HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def fetch_eur_balance() -> float | None:
    """Fetch available EUR balance from Bitvavo."""
    if not _bitvavo_ready():
        return None
    try:
        result = bitvavo.balance({"symbol": "EUR"})
        if isinstance(result, list) and result:
            return float(result[0].get("available", 0))
        if isinstance(result, dict) and "errorCode" in result:
            st.error(f"Bitvavo error: {result.get('error', result)}")
    except Exception as e:
        st.error(f"Balance fetch failed: {e}")
    return None

def _parse_fills(order: dict) -> tuple[float, float, float]:
    """
    Parse a completed Bitvavo order response.
    Returns (coins_received, eur_spent, fee_paid).
    For buy orders: coins = filledAmount, eur = filledAmountQuote
    For sell orders: coins = filledAmount, eur = filledAmountQuote
    """
    coins = float(order.get("filledAmount", 0) or 0)
    eur   = float(order.get("filledAmountQuote", 0) or 0)
    fee   = float(order.get("feePaid", 0) or 0)
    return coins, eur, fee

def place_market_buy(market: str, eur_amount: float) -> dict | None:
    """Place a market buy order for `eur_amount` EUR. Returns order dict or None."""
    if not _bitvavo_ready():
        return None
    try:
        order = bitvavo.placeOrder(
            market, "buy", "market",
            {"amountQuote": f"{eur_amount:.2f}"},
        )
        if isinstance(order, dict) and "errorCode" in order:
            st.error(f"Buy order failed: {order.get('error', order)}")
            return None
        return order
    except Exception as e:
        st.error(f"Buy order exception: {e}")
        return None

def place_market_sell(market: str, coin_amount: float) -> dict | None:
    """Place a market sell order for `coin_amount` coins. Returns order dict or None."""
    if not _bitvavo_ready():
        return None
    try:
        order = bitvavo.placeOrder(
            market, "sell", "market",
            {"amount": f"{coin_amount:.8f}"},
        )
        if isinstance(order, dict) and "errorCode" in order:
            st.error(f"Sell order failed: {order.get('error', order)}")
            return None
        return order
    except Exception as e:
        st.error(f"Sell order exception: {e}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# ORDER SIZING HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def _get_order_sizes(eur_balance: float) -> tuple[float, float]:
    ref = eur_balance
    if order_mode == "Percentage":
        return ref * base_order_value / 100, ref * safety_order_value / 100
    return base_order_value, safety_order_value

def _build_ladder(entry_price: float, so_base: float) -> list:
    """Returns list of (trigger_price, eur_size) for each safety order."""
    ladder, cum, alloc = [], 0.0, so_base
    for n in range(max_safety_orders):
        cum += (deviation_pct / 100) * (step_multiplier ** n)
        ladder.append((entry_price * (1 - cum), alloc))
        alloc *= volume_scale
    return ladder


# ─────────────────────────────────────────────────────────────────────────────
# INDICATOR PRE-COMPUTATION
# ─────────────────────────────────────────────────────────────────────────────
def compute_all_indicators(candles: list) -> dict:
    closes  = pd.Series([c["close"]  for c in candles])
    highs   = pd.Series([c["high"]   for c in candles])
    lows    = pd.Series([c["low"]    for c in candles])
    volumes = pd.Series([c.get("volume", 0.0) for c in candles])

    ind = {}
    ind["rsi"]     = compute_rsi(closes, int(rsi_period)).tolist()   if rsi_filter    else [0.0]            * len(closes)
    ind["sma"]     = compute_sma(closes, int(sma_period)).tolist()   if sma_filter    else [float("inf")]   * len(closes)
    ind["ema"]     = compute_ema(closes, int(ema_period)).tolist()   if ema_filter    else [float("inf")]   * len(closes)
    ind["vol_ma"]  = compute_sma(volumes, int(volume_ma_period)).tolist() if volume_filter else [0.0]       * len(closes)
    ind["atr"]     = compute_atr(highs, lows, closes, int(atr_period)).tolist() if atr_dynamic else [0.0]  * len(closes)

    if bb_filter:
        _, _, bb_lower = compute_bollinger_bands(closes, int(bb_period), bb_std)
        ind["bb_lower"] = bb_lower.tolist()
    else:
        ind["bb_lower"] = [float("-inf")] * len(closes)

    if macd_filter:
        ml, ms, mh = compute_macd(closes, int(macd_fast), int(macd_slow), int(macd_signal))
        ind["macd_line"] = ml.tolist()
        ind["macd_sig"]  = ms.tolist()
        ind["macd_hist"] = mh.tolist()
    else:
        ind["macd_line"] = ind["macd_sig"] = ind["macd_hist"] = [0.0] * len(closes)

    ind["bos"]     = compute_bos(highs, lows, closes, int(bos_lookback)).tolist() if bos_filter else [1] * len(closes)
    ind["support"] = compute_support(lows, int(sr_lookback)).tolist() if sr_filter else [0.0] * len(closes)

    if ma_cross_filter:
        fn = compute_ema if ma_cross_type == "EMA" else compute_sma
        ind["ma_fast"] = fn(closes, int(ma_fast_period)).tolist()
        ind["ma_slow"] = fn(closes, int(ma_slow_period)).tolist()
    else:
        ind["ma_fast"] = [1.0] * len(closes)
        ind["ma_slow"] = [0.0] * len(closes)

    return ind


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY GATE  (identical to paper trading)
# ─────────────────────────────────────────────────────────────────────────────
def check_entry(i: int, candles: list, ind: dict) -> bool:
    c = candles[i]["close"]

    rsi_ok = not rsi_filter or (ind["rsi"][i] == ind["rsi"][i] and ind["rsi"][i] < rsi_threshold)
    sma_ok = not sma_filter or (ind["sma"][i] == ind["sma"][i] and c < ind["sma"][i])
    ema_ok = not ema_filter or (ind["ema"][i] == ind["ema"][i] and c < ind["ema"][i])
    vol_ok = not volume_filter or (
        ind["vol_ma"][i] == ind["vol_ma"][i] and ind["vol_ma"][i] > 0 and
        candles[i].get("volume", 0.0) > volume_multiplier * ind["vol_ma"][i]
    )
    bb_ok = not bb_filter or (ind["bb_lower"][i] == ind["bb_lower"][i] and c < ind["bb_lower"][i])

    if macd_filter:
        if macd_mode == "histogram_positive":
            macd_ok = ind["macd_hist"][i] == ind["macd_hist"][i] and ind["macd_hist"][i] > 0
        elif macd_mode == "macd_above_signal":
            macd_ok = ind["macd_line"][i] == ind["macd_line"][i] and ind["macd_line"][i] > ind["macd_sig"][i]
        else:
            macd_ok = ind["macd_line"][i] == ind["macd_line"][i] and ind["macd_line"][i] > 0
    else:
        macd_ok = True

    if bos_filter:
        start  = max(0, i - int(bos_recency) + 1)
        bos_ok = any(ind["bos"][k] == 1 for k in range(start, i + 1))
    else:
        bos_ok = True

    sr_ok = not sr_filter or (
        ind["support"][i] == ind["support"][i] and ind["support"][i] > 0 and
        c <= ind["support"][i] * (1 + sr_proximity_pct / 100)
    )

    if ma_cross_filter:
        fv, sv = ind["ma_fast"][i], ind["ma_slow"][i]
        if fv != fv or sv != sv:
            ma_ok = False
        elif ma_cross_mode == "golden_cross_regime":
            ma_ok = fv > sv
        else:
            ma_ok = False
            for k in range(max(1, i - 2), i + 1):
                pk = k - 1
                if (ind["ma_fast"][k] == ind["ma_fast"][k] and
                        ind["ma_slow"][k] == ind["ma_slow"][k] and
                        ind["ma_fast"][pk] == ind["ma_fast"][pk] and
                        ind["ma_slow"][pk] == ind["ma_slow"][pk] and
                        ind["ma_fast"][k] > ind["ma_slow"][k] and
                        ind["ma_fast"][pk] <= ind["ma_slow"][pk]):
                    ma_ok = True
                    break
    else:
        ma_ok = True

    return rsi_ok and sma_ok and ema_ok and vol_ok and bb_ok and macd_ok and bos_ok and sr_ok and ma_ok


# ─────────────────────────────────────────────────────────────────────────────
# LIVE TRADING ENGINE
# ─────────────────────────────────────────────────────────────────────────────
def process_candle(idx: int, candles: list, ind: dict, state: dict, eur_balance: float) -> None:
    """
    Process candle at `idx`. Places real Bitvavo orders when signals fire.
    eur_balance is the current live EUR balance from Bitvavo.
    """
    candle = candles[idx]
    high   = candle["high"]
    low    = candle["low"]
    close  = candle["close"]
    ts_ms  = candle["time"]
    ts_str = datetime.fromtimestamp(ts_ms / 1000, tz=AMS).strftime("%Y-%m-%d %H:%M")

    ot = state["open_trade"]

    if ot is None:
        # ── Try to open a new trade ───────────────────────────────────────────
        if not check_entry(idx, candles, ind):
            return

        base_eur, so_base_eur = _get_order_sizes(eur_balance)
        if eur_balance <= 0 or base_eur <= 0 or base_eur > eur_balance:
            _log(state, f"⚠️ Insufficient balance (€{eur_balance:.2f}) for base order €{base_eur:.2f}")
            return

        # Place real market buy order
        order = place_market_buy(trading_pair, base_eur)
        if order is None:
            _log(state, "❌ Base order placement failed — skipping entry")
            return

        coins, cost_eur, fee = _parse_fills(order)
        if coins <= 0:
            _log(state, f"❌ Base order returned 0 coins (order: {order})")
            return

        avg_price  = cost_eur / coins if coins > 0 else close
        entry_price = avg_price

        atr_entry = ind["atr"][idx]
        atr_valid = atr_dynamic and atr_entry == atr_entry and atr_entry > 0

        state["open_trade"] = {
            "trade_number":    state["trade_number"],
            "entry_time":      ts_str,
            "entry_candle_ms": ts_ms,
            "entry_price":     entry_price,
            "avg_price":       avg_price,
            "total_coins":     coins,
            "total_cost":      cost_eur,
            "total_spent":     cost_eur + fee,
            "fees_paid":       fee,
            "ladder":          _build_ladder(entry_price, so_base_eur),
            "ladder_idx":      0,
            "trailing_active": False,
            "trail_peak":      0.0,
            "atr_entry":       atr_entry if atr_valid else None,
            "micro_orders":    [{
                "type":     "Base Order",
                "time":     ts_str,
                "price":    entry_price,
                "eur":      cost_eur + fee,
                "coins":    coins,
                "order_id": order.get("orderId", ""),
            }],
        }
        _log(state, f"🟢 Trade #{state['trade_number']} OPENED — {trading_pair} @ €{entry_price:,.4f} "
                    f"| Base €{cost_eur + fee:.2f} | OrderID: {order.get('orderId', '?')}")
        # Entry price IS the fill price — no SO can have triggered yet. Return.
        return

    # ── Manage open trade ─────────────────────────────────────────────────────
    ot         = state["open_trade"]
    ladder     = ot["ladder"]
    ladder_idx = ot["ladder_idx"]

    # Fill safety orders where current price has reached trigger
    while ladder_idx < len(ladder):
        so_price, so_eur = ladder[ladder_idx]
        if low > so_price:
            break
        if so_eur > eur_balance:
            _log(state, f"⚠️ SO #{ladder_idx + 1} skipped — insufficient balance "
                        f"(need €{so_eur:.2f}, have €{eur_balance:.2f})")
            ladder_idx += 1
            continue

        order = place_market_buy(trading_pair, so_eur)
        if order is None:
            _log(state, f"❌ SO #{ladder_idx + 1} order failed — skipping")
            ladder_idx += 1
            continue

        c, cb, fee = _parse_fills(order)
        if c <= 0:
            ladder_idx += 1
            continue

        actual_price = cb / c if c > 0 else so_price
        ot["total_coins"] += c
        ot["total_cost"]  += cb
        ot["total_spent"] += cb + fee
        ot["fees_paid"]   += fee
        ot["avg_price"]    = ot["total_cost"] / ot["total_coins"]
        ot["micro_orders"].append({
            "type":     f"Safety {ladder_idx + 1}",
            "time":     ts_str,
            "price":    actual_price,
            "eur":      cb + fee,
            "coins":    c,
            "order_id": order.get("orderId", ""),
        })
        # Update balance estimate for subsequent SO checks this cycle
        eur_balance -= (cb + fee)
        _log(state, f"  ↪ SO #{ladder_idx + 1} filled @ €{actual_price:,.4f} | "
                    f"New avg €{ot['avg_price']:,.4f} | OrderID: {order.get('orderId', '?')}")
        ladder_idx += 1
    ot["ladder_idx"] = ladder_idx

    avg         = ot["avg_price"]
    exit_price  = None
    exit_reason = None

    # Stop loss
    if exit_price is None and stop_loss_enabled:
        sl_price = (avg - atr_sl_mult * ot["atr_entry"]
                    if ot["atr_entry"] is not None
                    else avg * (1 - stop_loss_pct / 100))
        if low <= sl_price:
            exit_price  = sl_price
            exit_reason = "Stop Loss"

    # Take profit (fixed or trailing)
    if exit_price is None:
        tp_price = (avg + atr_tp_mult * ot["atr_entry"]
                    if ot["atr_entry"] is not None
                    else avg * (1 + take_profit_pct / 100))
        if trailing_tp:
            if not ot["trailing_active"] and high >= tp_price:
                ot["trailing_active"] = True
                ot["trail_peak"]      = high
            elif ot["trailing_active"]:
                if high > ot["trail_peak"]:
                    ot["trail_peak"] = high
                trail_stop = ot["trail_peak"] * (1 - trail_pct / 100)
                if low <= trail_stop:
                    exit_price  = trail_stop
                    exit_reason = "Trailing TP"
        else:
            if high >= tp_price:
                exit_price  = tp_price
                exit_reason = "Take Profit"

    # Time stop
    if exit_price is None and time_stop_enabled:
        elapsed = (ts_ms - ot["entry_candle_ms"]) / 60_000
        if elapsed >= time_stop_hours * 60:
            exit_price  = close
            exit_reason = "Time Stop"

    if exit_price is not None:
        order = place_market_sell(trading_pair, ot["total_coins"])
        if order is None:
            _log(state, f"❌ Sell order failed ({exit_reason}) — will retry next refresh")
            return

        _, proceeds, sell_fee = _parse_fills(order)
        ot["fees_paid"] += sell_fee
        gross_profit = proceeds - ot["total_spent"]
        net_profit   = gross_profit - sell_fee

        closed = {
            "trade":            ot["trade_number"],
            "entry_time":       ot["entry_time"],
            "exit_time":        ts_str,
            "entry_price":      ot["entry_price"],
            "avg_price":        avg,
            "exit_price":       proceeds / ot["total_coins"] if ot["total_coins"] > 0 else exit_price,
            "exit_reason":      exit_reason,
            "gross_profit":     round(gross_profit, 4),
            "net_profit":       round(net_profit,   4),
            "fees":             round(ot["fees_paid"], 4),
            "capital_deployed": round(ot["total_spent"], 4),
            "sos_filled":       ladder_idx,
            "roi_pct":          round(net_profit / ot["total_spent"] * 100, 3),
            "sell_order_id":    order.get("orderId", ""),
        }
        state["closed_trades"].append(closed)
        emoji = "🟢" if net_profit >= 0 else "🔴"
        _log(state, f"{emoji} Trade #{ot['trade_number']} CLOSED ({exit_reason}) "
                    f"@ €{closed['exit_price']:,.4f} | Net P&L €{net_profit:+.2f} "
                    f"| OrderID: {order.get('orderId', '?')}")
        state["open_trade"]   = None
        state["trade_number"] += 1


# ─────────────────────────────────────────────────────────────────────────────
# MAIN — fetch, process, display
# ─────────────────────────────────────────────────────────────────────────────
state = load_state()

# Persist toggle state
if state.get("bot_running") != bot_running:
    state["bot_running"] = bot_running
    save_state(state)

# ── API key guard ─────────────────────────────────────────────────────────────
if not _bitvavo_ready():
    st.warning("Enter your Bitvavo API Key and Secret in the sidebar to enable live trading.")
    st.stop()

# ── Fetch live EUR balance from Bitvavo ───────────────────────────────────────
with st.spinner("Fetching account balance…"):
    eur_balance = fetch_eur_balance()

if eur_balance is None:
    st.error("Could not fetch EUR balance from Bitvavo. Check your API credentials and permissions.")
    st.stop()

# ── Fetch candles ─────────────────────────────────────────────────────────────
with st.spinner(f"Fetching {trading_pair} {interval} candles…"):
    all_candles = fetch_candles(trading_pair, interval, LOOKBACK)

if not all_candles:
    st.error("Could not fetch candles from Bitvavo. Check your connection.")
    st.stop()

closed_candles = all_candles[:-1]
live_candle    = all_candles[-1]

# ── Process new closed candles ────────────────────────────────────────────────
if bot_running:
    new_candles = [c for c in closed_candles if c["time"] > state["last_candle_time"]]
    if new_candles:
        ind_closed = compute_all_indicators(closed_candles)
        for candle in new_candles:
            idx = next((i for i, c in enumerate(closed_candles) if c["time"] == candle["time"]), None)
            if idx is None:
                continue
            process_candle(idx, closed_candles, ind_closed, state, eur_balance)
        state["last_candle_time"] = closed_candles[-1]["time"]
        save_state(state)

    # ── Process live (forming) candle ─────────────────────────────────────────
    _live_as_candle = {
        "time":   live_candle["time"],
        "open":   live_candle["open"],
        "high":   live_candle["close"],  # current price only
        "low":    live_candle["close"],  # current price only
        "close":  live_candle["close"],
        "volume": live_candle.get("volume", 0.0),
    }
    _candles_with_live = closed_candles + [_live_as_candle]
    ind_live  = compute_all_indicators(_candles_with_live)
    _live_idx = len(_candles_with_live) - 1
    process_candle(_live_idx, _candles_with_live, ind_live, state, eur_balance)
    state["last_candle_time"] = live_candle["time"]
    save_state(state)

# ── Live price and display vars ───────────────────────────────────────────────
live_price   = live_candle["close"]
last_ts      = datetime.fromtimestamp(closed_candles[-1]["time"] / 1000, tz=AMS).strftime("%Y-%m-%d %H:%M") if closed_candles else "—"
next_refresh = datetime.now(AMS) + dt.timedelta(seconds=refresh_secs)

# ── Header metrics ────────────────────────────────────────────────────────────
total_net    = sum(t["net_profit"] for t in state["closed_trades"])
total_trades = len(state["closed_trades"])
win_rate     = (sum(1 for t in state["closed_trades"] if t["net_profit"] > 0) / total_trades * 100
                if total_trades else 0.0)
roi_overall  = total_net / eur_balance * 100 if eur_balance > 0 else 0.0

m1, m2, m3, m4, m5, m6, m7 = st.columns(7)
m1.metric("EUR Balance",    f"€{eur_balance:,.2f}")
m2.metric("Net Profit",     f"€{total_net:+,.2f}")
m3.metric("ROI",            f"{roi_overall:+.2f}%",
          help="Net profit from closed trades / current EUR balance")
m4.metric("Closed Trades",  total_trades)
m5.metric("Win Rate",       f"{win_rate:.1f}%")
m6.metric("Live Price",     f"€{live_price:,.4f}", help=f"Currently forming {interval} candle")
m7.metric("Last Candle",    last_ts)

st.divider()

# ── Open trade card ───────────────────────────────────────────────────────────
ot = state["open_trade"]
if ot:
    avg        = ot["avg_price"]
    unrealized = (ot["total_coins"] * live_price - ot["total_spent"])
    tp_price   = (avg + atr_tp_mult * ot["atr_entry"]
                  if ot["atr_entry"] is not None
                  else avg * (1 + take_profit_pct / 100))
    sl_price   = (avg - atr_sl_mult * ot["atr_entry"]
                  if ot["atr_entry"] is not None and stop_loss_enabled
                  else avg * (1 - stop_loss_pct / 100) if stop_loss_enabled else None)

    pnl_color = "normal" if unrealized >= 0 else "inverse"
    st.subheader(f"📂 Open Trade #{ot['trade_number']} — {trading_pair}")
    o1, o2, o3, o4, o5, o6 = st.columns(6)
    o1.metric("Opened",         ot["entry_time"])
    o2.metric("Entry Price",    f"€{ot['entry_price']:,.4f}")
    o3.metric("Avg Price",      f"€{avg:,.4f}")
    o4.metric("TP Price",       f"€{tp_price:,.4f}")
    o5.metric("SOs Filled",     ot["ladder_idx"])
    o6.metric("Deployed",       f"€{ot['total_spent']:,.2f}")

    o7, o8, o9, _ = st.columns(4)
    o7.metric("Unrealised P&L", f"€{unrealized:+.2f}",
              delta=f"{unrealized / ot['total_spent'] * 100:+.2f}%",
              delta_color=pnl_color)
    o8.metric("Gap to TP",      f"{(tp_price / live_price - 1) * 100:+.2f}%")
    if sl_price:
        o9.metric("Stop Loss",  f"€{sl_price:,.4f}")

    if ot.get("trailing_active"):
        st.info(f"🔔 Trailing TP active — peak €{ot['trail_peak']:,.4f} | "
                f"trail stop €{ot['trail_peak'] * (1 - trail_pct / 100):,.4f}")

    # ── Next pending SO trigger prices ───────────────────────────────────────
    _pending_sos = ot["ladder"][ot["ladder_idx"]:]
    if _pending_sos:
        st.markdown("**Pending Safety Orders**")
        _so_cols = st.columns(min(len(_pending_sos), 5))
        for _si, (_sp, _se) in enumerate(_pending_sos[:5]):
            _drop_pct = (live_price - _sp) / live_price * 100
            _so_cols[_si].metric(
                f"SO #{ot['ladder_idx'] + _si + 1}",
                f"€{_sp:,.4f}",
                delta=f"{_drop_pct:.2f}% away",
                delta_color="inverse",
                help=f"Size: €{_se:.2f}",
            )
    else:
        st.caption("All safety orders filled.")

    with st.expander("📋 Order-level detail", expanded=False):
        _mo_df = pd.DataFrame(ot["micro_orders"])
        _fmt = {"price": "€{:.4f}", "eur": "€{:.2f}", "coins": "{:.6f}"}
        st.dataframe(
            _mo_df.style.format(_fmt),
            use_container_width=True,
            hide_index=True,
        )
else:
    st.info("⏳ No open trade — waiting for next entry signal.")

st.divider()

# ── Closed trades table ───────────────────────────────────────────────────────
if state["closed_trades"]:
    st.subheader("💰 Closed Trades")
    df_closed = pd.DataFrame(state["closed_trades"])
    st.dataframe(
        df_closed.style
            .format({
                "entry_price":      "€{:.4f}",
                "avg_price":        "€{:.4f}",
                "exit_price":       "€{:.4f}",
                "gross_profit":     "€{:.2f}",
                "net_profit":       "€{:.2f}",
                "fees":             "€{:.2f}",
                "capital_deployed": "€{:.2f}",
                "roi_pct":          "{:.3f}%",
                "sos_filled":       "{:.0f}",
            })
            .map(lambda v: "color: #2ecc71" if isinstance(v, float) and v > 0 else
                           "color: #e74c3c" if isinstance(v, float) and v < 0 else "",
                 subset=["net_profit"]),
        use_container_width=True,
        hide_index=True,
    )
    st.download_button(
        "⬇️ Download trades CSV",
        data=df_closed.to_csv(index=False).encode(),
        file_name=f"live_trades_{trading_pair}_{datetime.now(AMS).strftime('%Y%m%d')}.csv",
        mime="text/csv",
    )

# ── Activity log ──────────────────────────────────────────────────────────────
if state["log"]:
    with st.expander("📋 Activity Log", expanded=True):
        st.text("\n".join(state["log"][:50]))

# ── Auto-refresh ─────────────────────────────────────────────────────────────
if bot_running:
    st.caption(
        f"⚡ Live bot running — next refresh at "
        f"{next_refresh.strftime('%H:%M:%S')} "
        f"(every {refresh_secs}s)"
    )
    time.sleep(refresh_secs)
    st.rerun()
else:
    st.caption("⏸️ Bot paused — toggle **▶️ Bot Running** in the sidebar to activate live trading.")
    if st.button("🔄 Refresh now", key="manual_refresh"):
        st.rerun()
