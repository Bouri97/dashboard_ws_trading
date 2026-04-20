"""
paper_trading_bitvavo.py  —  DCA Paper Trading Dashboard (Bitvavo)
──────────────────────────────────────────────────────────────────
Simulates the DCA strategy in real time using live Bitvavo OHLCV data.
No real orders are placed — all trades are virtual.

State (balance, open trade, closed trades) is persisted to
  paper_state.json  next to this script, so the bot survives restarts.

Run with:
  streamlit run paper_trading_bitvavo.py
"""

import json
import time
import datetime as dt
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
import streamlit as st

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────
INTERVAL_MINUTES: dict[str, int] = {
    "1m": 1, "5m": 5, "15m": 15, "30m": 30,
    "1h": 60, "2h": 120, "4h": 240, "6h": 360,
    "8h": 480, "12h": 720, "1d": 1440, "3d": 4320, "1w": 10080,
}
BASE_URL    = "https://api.bitvavo.com/v2"
LOOKBACK    = 500          # candles fetched each refresh for indicator warmup
STATE_FILE  = Path(__file__).parent / "paper_state.json"


# ─────────────────────────────────────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(layout="wide", page_title="DCA Paper Trader — Bitvavo")
st.title("📄 DCA Paper Trading — Bitvavo (Simulated)")
st.caption("No real orders are placed. All trades are fully virtual.")


# ─────────────────────────────────────────────────────────────────────────────
# STATE MANAGEMENT  (defined early — used in sidebar before main section)
# ─────────────────────────────────────────────────────────────────────────────
def _default_state(bal: float) -> dict:
    return {
        "balance":          bal,
        "initial_balance":  bal,
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
    # initial_balance not yet known here — use 1000 as safe fallback;
    # main section corrects it if balance was never set.
    return _default_state(1000.0)

def save_state(state: dict) -> None:
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2, default=str)

def _log(state: dict, msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    state["log"].insert(0, f"[{ts}] {msg}")
    state["log"] = state["log"][:200]


# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR — STRATEGY SETTINGS
# ─────────────────────────────────────────────────────────────────────────────
st.sidebar.header("⚙️ Strategy Settings")

trading_pair = st.sidebar.selectbox(
    "Trading Pair", ["BTC-EUR", "ETH-EUR", "XRP-EUR", "SOL-EUR"], key="pt_pair"
)
interval = st.sidebar.selectbox(
    "Candle Timeframe", list(INTERVAL_MINUTES.keys()), index=10, key="pt_interval",
    help="Strategy is evaluated on each newly closed candle of this timeframe.",
)
initial_balance = st.sidebar.number_input(
    "Initial Balance (EUR)", value=1000.0, min_value=1.0, step=100.0, key="pt_bal"
)

# ── Order sizing ──────────────────────────────────────────────────────────────
st.sidebar.markdown("**Order Sizing**")
order_mode = st.sidebar.selectbox("Order Size Mode", ["Percentage", "Fixed EUR"], key="pt_om")
if order_mode == "Percentage":
    base_order_value   = st.sidebar.number_input(
        "Base Order (%)", value=5.6, min_value=0.1, step=0.1, key="pt_base",
        help="Keep small — capital must cover all safety orders too")
    safety_order_value = st.sidebar.number_input(
        "Safety Order (%)", value=6.3, min_value=0.1, step=0.1, key="pt_so")
else:
    base_order_value   = st.sidebar.number_input(
        "Base Order (EUR)", value=50.0, min_value=1.0, step=10.0, key="pt_base")
    safety_order_value = st.sidebar.number_input(
        "Safety Order (EUR)", value=50.0, min_value=1.0, step=10.0, key="pt_so")

# ── DCA parameters ────────────────────────────────────────────────────────────
st.sidebar.markdown("**DCA Parameters**")
deviation_pct     = st.sidebar.number_input(
    "Price Deviation (%)", value=0.37, min_value=0.01, step=0.01, key="pt_dev")
step_multiplier   = st.sidebar.number_input(
    "Step Multiplier", value=0.9, min_value=0.1, step=0.1, key="pt_step")
volume_scale      = st.sidebar.number_input(
    "Volume Scale", value=1.26, min_value=1.0, step=0.01, key="pt_vs")
take_profit_pct   = st.sidebar.number_input(
    "Take Profit (%)", value=1.46, min_value=0.01, step=0.01, key="pt_tp")
max_safety_orders = st.sidebar.slider("Max Safety Orders", 1, 20, 10, key="pt_maxso")

# ── Entry filters ─────────────────────────────────────────────────────────────
st.sidebar.markdown("**Entry Filters**")
rsi_filter    = st.sidebar.checkbox("RSI Filter", value=False, key="pt_rsi_f")
rsi_period    = st.sidebar.number_input("RSI Period",    value=14,   min_value=2,   step=1,   key="pt_rsi_p",  disabled=not rsi_filter)
rsi_threshold = st.sidebar.number_input("RSI Threshold", value=55.0, min_value=1.0, max_value=99.0, step=1.0, key="pt_rsi_t", disabled=not rsi_filter)

sma_filter = st.sidebar.checkbox("SMA Filter", value=False, key="pt_sma_f")
sma_period = st.sidebar.number_input("SMA Period", value=50, min_value=2, step=1, key="pt_sma_p", disabled=not sma_filter)

ema_filter = st.sidebar.checkbox("EMA Filter", value=False, key="pt_ema_f")
ema_period = st.sidebar.number_input("EMA Period", value=21, min_value=2, step=1, key="pt_ema_p", disabled=not ema_filter)

volume_filter     = st.sidebar.checkbox("Volume Filter", value=False, key="pt_vol_f")
volume_ma_period  = st.sidebar.number_input("Volume MA Period",  value=20,  min_value=2, step=1,   key="pt_vol_p", disabled=not volume_filter)
volume_multiplier = st.sidebar.number_input("Volume Multiplier", value=1.0, min_value=0.1, step=0.1, key="pt_vol_m", disabled=not volume_filter)

bb_filter = st.sidebar.checkbox("Bollinger Bands Filter", value=False, key="pt_bb_f")
bb_period = st.sidebar.number_input("BB Period",  value=20,  min_value=2, step=1,   key="pt_bb_p", disabled=not bb_filter)
bb_std    = st.sidebar.number_input("BB Std Dev", value=2.0, min_value=0.1, step=0.1, key="pt_bb_s", disabled=not bb_filter)

macd_filter = st.sidebar.checkbox("MACD Filter", value=False, key="pt_macd_f")
macd_fast   = st.sidebar.number_input("MACD Fast",   value=12, min_value=2, step=1, key="pt_macd_fs", disabled=not macd_filter)
macd_slow   = st.sidebar.number_input("MACD Slow",   value=26, min_value=2, step=1, key="pt_macd_sl", disabled=not macd_filter)
macd_signal = st.sidebar.number_input("MACD Signal", value=9,  min_value=2, step=1, key="pt_macd_sg", disabled=not macd_filter)
macd_mode   = st.sidebar.selectbox(
    "MACD Entry Mode", ["histogram_positive", "macd_above_signal", "macd_above_zero"],
    key="pt_macd_m", disabled=not macd_filter,
)

bos_filter   = st.sidebar.checkbox("BOS Filter", value=False, key="pt_bos_f")
bos_lookback = st.sidebar.number_input("BOS Lookback", value=10, min_value=2, step=1, key="pt_bos_lb", disabled=not bos_filter)
bos_recency  = st.sidebar.number_input("BOS Recency (candles)", value=5, min_value=1, step=1, key="pt_bos_r", disabled=not bos_filter)

sr_filter        = st.sidebar.checkbox("S&R Filter", value=True, key="pt_sr_f")
sr_lookback      = st.sidebar.number_input("S&R Lookback (candles)", value=5, min_value=1, step=1, key="pt_sr_lb", disabled=not sr_filter)
sr_proximity_pct = st.sidebar.number_input("Proximity to Support (%)", value=9.8, min_value=0.1, step=0.1, key="pt_sr_p", disabled=not sr_filter)

ma_cross_filter = st.sidebar.checkbox("MA Cross Filter", value=False, key="pt_mac_f")
ma_cross_type   = st.sidebar.selectbox("MA Type", ["SMA", "EMA"], key="pt_mac_t", disabled=not ma_cross_filter)
ma_fast_period  = st.sidebar.number_input("Fast MA Period", value=50,  min_value=2, step=1, key="pt_mac_fp", disabled=not ma_cross_filter)
ma_slow_period  = st.sidebar.number_input("Slow MA Period", value=200, min_value=2, step=1, key="pt_mac_sp", disabled=not ma_cross_filter)
ma_cross_mode   = st.sidebar.selectbox(
    "MA Cross Mode", ["golden_cross_regime", "fresh_crossover"],
    key="pt_mac_m", disabled=not ma_cross_filter,
)

atr_dynamic = st.sidebar.checkbox("ATR Dynamic TP/SL", value=False, key="pt_atr_f")
atr_period  = st.sidebar.number_input("ATR Period",          value=14,  min_value=2,  step=1,   key="pt_atr_p",  disabled=not atr_dynamic)
atr_tp_mult = st.sidebar.number_input("ATR TP Multiplier",   value=2.0, min_value=0.1, step=0.1, key="pt_atr_tp", disabled=not atr_dynamic)
atr_sl_mult = st.sidebar.number_input("ATR SL Multiplier",   value=1.5, min_value=0.1, step=0.1, key="pt_atr_sl", disabled=not atr_dynamic)

# ── Exit options ──────────────────────────────────────────────────────────────
st.sidebar.markdown("**Exit Options**")
trailing_tp       = st.sidebar.checkbox("Trailing Take Profit", value=False, key="pt_ttp")
trail_pct         = st.sidebar.number_input("Trail Distance (%)", value=0.3, min_value=0.05, step=0.05, key="pt_trail", disabled=not trailing_tp)
stop_loss_enabled = st.sidebar.checkbox("Stop Loss", value=True, key="pt_sl_e")
stop_loss_pct     = st.sidebar.number_input("Stop Loss (%)", value=17.5, min_value=0.1, step=0.5, key="pt_sl_p", disabled=not stop_loss_enabled)
time_stop_enabled = st.sidebar.checkbox("Time Stop", value=False, key="pt_ts_e")
time_stop_hours   = st.sidebar.number_input("Max Duration (hours)", value=24, min_value=1, step=1, key="pt_ts_h", disabled=not time_stop_enabled)

fee_rate    = st.sidebar.number_input("Taker Fee (%)", value=0.25, min_value=0.0, step=0.05, format="%.2f", key="pt_fee") / 100
compounding = st.sidebar.checkbox("Enable Compounding", value=True, key="pt_comp")

# ── Bot controls ──────────────────────────────────────────────────────────────
st.sidebar.divider()
st.sidebar.markdown("**Bot Controls**")
refresh_secs = st.sidebar.selectbox(
    "Auto-refresh interval",
    [10, 30, 60, 120, 300], index=2,
    format_func=lambda x: f"{x}s" if x < 60 else f"{x // 60}m",
    key="pt_refresh",
    help="How often the bot checks for newly closed candles",
)
# Read persisted running state so the toggle survives page refreshes
_state_preview = load_state()
_persisted_running = _state_preview.get("bot_running", False)

bot_running = st.sidebar.toggle(
    "▶️ Bot Running", value=_persisted_running, key="pt_running",
    help="When ON the page auto-refreshes and processes new candles automatically",
)
if st.sidebar.button("🔄 Reset — clear all trades & balance", key="pt_reset"):
    fresh = _default_state(initial_balance)
    try:
        save_state(fresh)
    except Exception:
        pass
    # Also clear from session state so the in-memory copy is gone
    for _k in list(st.session_state.keys()):
        if _k.startswith("_pt_state"):
            del st.session_state[_k]
    st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# INDICATORS  (identical to backtest dashboard)
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
# LIVE CANDLE FETCHING
# ─────────────────────────────────────────────────────────────────────────────
def fetch_candles(market: str, itvl: str, n: int = LOOKBACK) -> list:
    """Fetch the most recent `n` closed + 1 forming candle from Bitvavo REST."""
    try:
        resp = requests.get(
            f"{BASE_URL}/{market}/candles",
            params={"interval": itvl, "limit": n},
            timeout=10,
        )
        data = resp.json()
    except Exception as e:
        return []
    if not isinstance(data, list):
        return []
    candles = sorted(
        [{"time": int(c[0]), "open": float(c[1]), "high": float(c[2]),
          "low": float(c[3]), "close": float(c[4]),
          "volume": float(c[5]) if len(c) > 5 else 0.0}
         for c in data],
        key=lambda x: x["time"],
    )
    return candles


# ─────────────────────────────────────────────────────────────────────────────
# ORDER SIZING HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def _get_order_sizes(balance: float) -> tuple:
    ref = balance if compounding else initial_balance
    if order_mode == "Percentage":
        return ref * base_order_value / 100, ref * safety_order_value / 100
    return base_order_value, safety_order_value

def _build_ladder(entry_price: float, so_base: float) -> list:
    """Returns list of (trigger_price, eur_size) for each safety order."""
    ladder, cum, alloc = [], 0.0, so_base
    for n in range(max_safety_orders):
        cum   += (deviation_pct / 100) * (step_multiplier ** n)
        ladder.append((entry_price * (1 - cum), alloc))
        alloc *= volume_scale
    return ladder

def _buy(eur: float, price: float) -> tuple:
    fee      = eur * fee_rate
    cost_net = eur - fee
    return cost_net / price, cost_net, fee   # (coins, cost_basis, fee_eur)


# ─────────────────────────────────────────────────────────────────────────────
# INDICATOR PRE-COMPUTATION
# ─────────────────────────────────────────────────────────────────────────────
def compute_all_indicators(candles: list) -> dict:
    closes  = pd.Series([c["close"]  for c in candles])
    highs   = pd.Series([c["high"]   for c in candles])
    lows    = pd.Series([c["low"]    for c in candles])
    volumes = pd.Series([c.get("volume", 0.0) for c in candles])

    ind = {}
    ind["rsi"]      = compute_rsi(closes, int(rsi_period)).tolist()   if rsi_filter    else [0.0]     * len(closes)
    ind["sma"]      = compute_sma(closes, int(sma_period)).tolist()   if sma_filter    else [float("inf")] * len(closes)
    ind["ema"]      = compute_ema(closes, int(ema_period)).tolist()   if ema_filter    else [float("inf")] * len(closes)
    ind["vol_ma"]   = compute_sma(volumes, int(volume_ma_period)).tolist() if volume_filter else [0.0] * len(closes)
    ind["atr"]      = compute_atr(highs, lows, closes, int(atr_period)).tolist() if atr_dynamic else [0.0] * len(closes)

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
# ENTRY GATE
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
        start = max(0, i - int(bos_recency) + 1)
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
# PAPER TRADING ENGINE — process one new closed candle
# ─────────────────────────────────────────────────────────────────────────────
def process_candle(idx: int, candles: list, ind: dict, state: dict) -> None:
    """
    Process the candle at `idx`.  Mutates `state` in place.
    Entry logic fires at the CLOSE of the trigger candle.
    SO fills / exits are evaluated using the candle's high/low.
    """
    candle = candles[idx]
    high   = candle["high"]
    low    = candle["low"]
    close  = candle["close"]
    ts_ms  = candle["time"]
    ts_str = datetime.fromtimestamp(ts_ms / 1000).strftime("%Y-%m-%d %H:%M")

    ot = state["open_trade"]

    if ot is None:
        # ── Try to open a new trade ───────────────────────────────────────────
        if not check_entry(idx, candles, ind):
            return

        base_eur, so_base_eur = _get_order_sizes(state["balance"])
        if state["balance"] <= 0 or base_eur <= 0 or base_eur > state["balance"]:
            return

        coins, cost_basis, fee = _buy(base_eur, close)
        state["balance"] -= base_eur
        avg = cost_basis / coins

        # ATR at entry for dynamic TP/SL
        atr_entry = ind["atr"][idx]
        atr_valid = atr_dynamic and atr_entry == atr_entry and atr_entry > 0

        state["open_trade"] = {
            "trade_number":    state["trade_number"],
            "entry_time":      ts_str,
            "entry_candle_ms": ts_ms,
            "entry_price":     close,
            "avg_price":       avg,
            "total_coins":     coins,
            "total_cost":      cost_basis,
            "total_spent":     base_eur,
            "fees_paid":       fee,
            "ladder":          _build_ladder(close, so_base_eur),
            "ladder_idx":      0,
            "trailing_active": False,
            "trail_peak":      0.0,
            "atr_entry":       atr_entry if atr_valid else None,
            "micro_orders":    [{
                "type":  "Base Order",
                "time":  ts_str,
                "price": close,
                "eur":   base_eur,
                "coins": coins,
            }],
        }
        _log(state, f"🟢 Trade #{state['trade_number']} OPENED — {trading_pair} @ €{close:,.4f} "
                    f"| Base €{base_eur:.2f} | Balance €{state['balance']:,.2f}")
        # Fall through — check SO fills and exits on this same candle

    # ── Manage open trade ─────────────────────────────────────────────────────
    ot         = state["open_trade"]   # re-read in case it was just opened above
    ladder     = ot["ladder"]
    ladder_idx = ot["ladder_idx"]

    # Fill safety orders
    while ladder_idx < len(ladder):
        so_price, so_eur = ladder[ladder_idx]
        if low > so_price:
            break
        if so_eur > state["balance"]:
            ladder_idx += 1
            continue
        c, cb, fee    = _buy(so_eur, so_price)
        state["balance"] -= so_eur
        ot["total_coins"] += c
        ot["total_cost"]  += cb
        ot["total_spent"] += so_eur
        ot["fees_paid"]   += fee
        ot["avg_price"]    = ot["total_cost"] / ot["total_coins"]
        ot["micro_orders"].append({
            "type":  f"Safety {ladder_idx + 1}",
            "time":  ts_str,
            "price": so_price,
            "eur":   so_eur,
            "coins": c,
        })
        _log(state, f"  ↪ SO #{ladder_idx + 1} filled @ €{so_price:,.4f} | "
                    f"New avg €{ot['avg_price']:,.4f}")
        ladder_idx += 1
    ot["ladder_idx"] = ladder_idx

    avg = ot["avg_price"]
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
        candle_min = INTERVAL_MINUTES[interval]
        elapsed    = (ts_ms - ot["entry_candle_ms"]) / 60_000
        if elapsed >= time_stop_hours * 60:
            exit_price  = close
            exit_reason = "Time Stop"

    if exit_price is not None:
        proceeds     = ot["total_coins"] * exit_price
        sell_fee     = proceeds * fee_rate
        ot["fees_paid"] += sell_fee
        gross_profit = proceeds - ot["total_spent"]
        net_profit   = gross_profit - sell_fee

        if compounding:
            state["balance"] += ot["total_spent"] + net_profit
        else:
            state["balance"] += ot["total_spent"]

        closed = {
            "trade":           ot["trade_number"],
            "entry_time":      ot["entry_time"],
            "exit_time":       ts_str,
            "entry_price":     ot["entry_price"],
            "avg_price":       avg,
            "exit_price":      exit_price,
            "exit_reason":     exit_reason,
            "gross_profit":    round(gross_profit, 4),
            "net_profit":      round(net_profit,   4),
            "fees":            round(ot["fees_paid"], 4),
            "capital_deployed":round(ot["total_spent"], 4),
            "sos_filled":      ladder_idx,
            "roi_pct":         round(net_profit / ot["total_spent"] * 100, 3),
        }
        state["closed_trades"].append(closed)
        emoji = "🟢" if net_profit >= 0 else "🔴"
        _log(state, f"{emoji} Trade #{ot['trade_number']} CLOSED ({exit_reason}) @ €{exit_price:,.4f} "
                    f"| Net P&L €{net_profit:+.2f} | Balance €{state['balance']:,.2f}")
        state["open_trade"]  = None
        state["trade_number"] += 1


# ─────────────────────────────────────────────────────────────────────────────
# MAIN — fetch, process, display
# ─────────────────────────────────────────────────────────────────────────────
state = load_state()

# Persist the toggle state immediately so refreshes restore it correctly
if state.get("bot_running") != bot_running:
    state["bot_running"] = bot_running
    save_state(state)

# If initial_balance changed since last run (e.g. user reset sidebar),
# only reset if there are no trades yet.
if not state["closed_trades"] and state["open_trade"] is None:
    if abs(state["initial_balance"] - initial_balance) > 0.01:
        state = _default_state(initial_balance)
        save_state(state)

# ── Fetch candles ─────────────────────────────────────────────────────────────
with st.spinner(f"Fetching {trading_pair} {interval} candles…"):
    all_candles = fetch_candles(trading_pair, interval, LOOKBACK)

if not all_candles:
    st.error("Could not fetch candles from Bitvavo. Check your connection.")
    st.stop()

# The last candle is still forming — used for live checks but not marked as closed
closed_candles = all_candles[:-1]
live_candle    = all_candles[-1]

# ── Process any new closed candles first ─────────────────────────────────────
new_candles = [c for c in closed_candles if c["time"] > state["last_candle_time"]]

if new_candles:
    ind_closed = compute_all_indicators(closed_candles)
    for candle in new_candles:
        idx = next((i for i, c in enumerate(closed_candles) if c["time"] == candle["time"]), None)
        if idx is None:
            continue
        process_candle(idx, closed_candles, ind_closed, state)
    state["last_candle_time"] = closed_candles[-1]["time"]
    save_state(state)

# ── Process the live (forming) candle on every refresh ───────────────────────
# Use close price as both high and low for the live candle.
# The intraday high/low from Bitvavo reflect the full day's range, which may
# include price moves that happened BEFORE the current trade was opened.
# Using close (current price) only means SOs/TP/SL fire based on where price
# actually is right now — not where it was earlier in the day.
_live_as_candle = {
    "time":   live_candle["time"],
    "open":   live_candle["open"],
    "high":   live_candle["close"],  # current price only — intraday high may predate entry
    "low":    live_candle["close"],  # current price only — intraday low may predate entry
    "close":  live_candle["close"],
    "volume": live_candle.get("volume", 0.0),
}
# Append live candle to get valid indicator values at the live index
_candles_with_live = closed_candles + [_live_as_candle]
ind_live = compute_all_indicators(_candles_with_live)
_live_idx = len(_candles_with_live) - 1
process_candle(_live_idx, _candles_with_live, ind_live, state)
save_state(state)

# ── Live price ────────────────────────────────────────────────────────────────
live_price   = live_candle["close"]
last_ts      = datetime.fromtimestamp(closed_candles[-1]["time"] / 1000).strftime("%Y-%m-%d %H:%M") if closed_candles else "—"
next_refresh = datetime.now() + dt.timedelta(seconds=refresh_secs)

# ── Header metrics ────────────────────────────────────────────────────────────
total_net    = sum(t["net_profit"]   for t in state["closed_trades"])
total_trades = len(state["closed_trades"])
win_rate     = (sum(1 for t in state["closed_trades"] if t["net_profit"] > 0) / total_trades * 100
                if total_trades else 0.0)
roi_overall  = (state["balance"] - state["initial_balance"]) / state["initial_balance"] * 100

m1, m2, m3, m4, m5, m6, m7 = st.columns(7)
m1.metric("Balance",       f"€{state['balance']:,.2f}", delta=f"{roi_overall:+.2f}%")
m2.metric("Net Profit",    f"€{total_net:+,.2f}")
m3.metric("ROI",           f"{roi_overall:+.2f}%",
          help=f"Net profit / start balance (€{state['initial_balance']:,.2f})")
m4.metric("Closed Trades", total_trades)
m5.metric("Win Rate",      f"{win_rate:.1f}%")
m6.metric("Live Price",    f"€{live_price:,.4f}", help=f"Currently forming {interval} candle")
m7.metric("Last Candle",   last_ts)

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
        st.dataframe(
            _mo_df.style.format({
                "price": "€{:.4f}",
                "eur":   "€{:.2f}",
                "coins": "{:.6f}",
            }),
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
    # Download
    st.download_button(
        "⬇️ Download trades CSV",
        data=df_closed.to_csv(index=False).encode(),
        file_name=f"paper_trades_{trading_pair}_{dt.datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv",
    )

# ── Activity log ──────────────────────────────────────────────────────────────
if state["log"]:
    with st.expander("📋 Activity Log", expanded=True):
        st.text("\n".join(state["log"][:50]))

# ── Auto-refresh ─────────────────────────────────────────────────────────────
if bot_running:
    st.caption(
        f"🔄 Bot running — next refresh at "
        f"{next_refresh.strftime('%H:%M:%S')} "
        f"(every {refresh_secs}s) · "
        f"{len(new_candles)} new candle(s) processed this cycle."
    )
    time.sleep(refresh_secs)
    st.rerun()
else:
    st.caption("⏸️ Bot paused — toggle **▶️ Bot Running** in the sidebar to start.")
    if st.button("🔄 Refresh now", key="manual_refresh"):
        st.rerun()
