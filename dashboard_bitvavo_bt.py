"""
dashboard_bitvavo_bt.py — DCA Spot Backtest Dashboard (Bitvavo)
───────────────────────────────────────────────────────────────
Backtests a DCA strategy using real Bitvavo OHLCV data.
Supports multiple timeframes (1m → 1w) and optional leverage up to 3×.

Leverage note
─────────────
  Leverage amplifies P&L (gains and losses × leverage multiplier).
  Liquidation triggers when price drops 1/leverage below avg entry.
  At 1× leverage (default) behaviour is identical to a pure spot strategy.

Strategy enhancements vs basic DCA
────────────────────────────────────
  Entry filters:
    • RSI filter  — only open a trade when RSI(n) is below a threshold
    • SMA filter  — only open a trade when price is below SMA(n)
    • EMA filter  — only open a trade when price is below EMA(n)
    • Volume filter — only open when volume exceeds its moving average
    • Bollinger Bands — only open when price is below the lower band
    • MACD filter — only open when MACD conditions are met

  Exit options (any combination):
    • Trailing TP — activate at TP%, then trail X% below the peak
    • Stop Loss   — close trade if avg price drops by X%
    • Time Stop   — close at market after N hours open
"""

import datetime as dt
import time

import pandas as pd
import requests
import streamlit as st
from datetime import date, timedelta, datetime

# Bitvavo supported intervals → duration in minutes
INTERVAL_MINUTES: dict[str, int] = {
    "1m": 1, "5m": 5, "15m": 15, "30m": 30,
    "1h": 60, "2h": 120, "4h": 240, "6h": 360,
    "8h": 480, "12h": 720, "1d": 1440, "3d": 4320, "1w": 10080,
}


# ─────────────────────────────────────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(layout="wide")
st.title("🇧🇪 DCA Spot Backtest Dashboard (Bitvavo)")


# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────────────────────────────
st.sidebar.header("⚙️ Strategy Settings")

# ── Presets: init + apply any pending load ─────────────────────────────────────
import json as _json
if "presets" not in st.session_state:
    st.session_state["presets"] = {}
if "_pending_load" in st.session_state:
    for _pk, _pv in st.session_state.pop("_pending_load").items():
        st.session_state[_pk] = _pv

# ── Trading Style Preset ───────────────────────────────────────────────────────
trading_style = st.sidebar.selectbox(
    "Trading Style Preset",
    ["Custom", "Scalping", "Day Trade", "Swing Trade"],
    key="trading_style",
    help="Informational preset — shows recommended settings for each style. Tweak individual values below.",
)
if trading_style == "Scalping":
    st.sidebar.info(
        "**Scalping:** Use 1m–5m candles, tight TP (0.3–0.8%), many small SOs, "
        "high trade frequency. Keep fees in mind — each trade chips away at thin margins."
    )
elif trading_style == "Day Trade":
    st.sidebar.info(
        "**Day Trade:** Use 15m–1h candles, TP 1–3%, moderate SOs (3–8), "
        "enable Time Stop at 12–24 h to avoid overnight exposure."
    )
elif trading_style == "Swing Trade":
    st.sidebar.info(
        "**Swing Trade:** Use 4h–1d candles, wide TP (3–10%), fewer SOs (2–5), "
        "multi-day holds. MA Cross / Golden Cross filter works well here."
    )

trading_pair = st.sidebar.selectbox(
    "Trading Pair", ["BTC-EUR", "ETH-EUR", "XRP-EUR", "SOL-EUR"],
    key="trading_pair",
)
interval = st.sidebar.selectbox(
    "Candle Timeframe", list(INTERVAL_MINUTES.keys()), index=10,
    key="interval",
    help="Candle interval used for data download and strategy evaluation",
)
initial_balance = st.sidebar.number_input(
    "Initial Balance (EUR)", value=1000.0, min_value=1.0, step=100.0,
    key="initial_balance",
)
leverage = st.sidebar.number_input(
    "Leverage", value=1.0, min_value=1.0, max_value=3.0, step=0.1, format="%.1f",
    key="leverage",
    help=(
        "1× = pure spot (no amplification, no liquidation). "
        "Up to 3×: P&L is multiplied by this factor. "
        "Liquidation triggers when price drops 1/leverage below avg entry."
    ),
)

# ── Order sizing ──────────────────────────────────────────────────────────────
st.sidebar.markdown("**Order Sizing**")
order_mode = st.sidebar.selectbox("Order Size Mode", ["Percentage", "Fixed EUR"], key="order_mode")
if order_mode == "Percentage":
    base_order_value   = st.sidebar.number_input(
        "Base Order (%)", value=5.6, min_value=0.1, step=0.1, key="base_order_value",
        help="Keep small — capital must cover all safety orders too")
    safety_order_value = st.sidebar.number_input(
        "Safety Order (%)", value=6.3, min_value=0.1, step=0.1, key="safety_order_value")
else:
    base_order_value   = st.sidebar.number_input("Base Order (EUR)",   value=50.0, min_value=1.0, step=10.0, key="base_order_value")
    safety_order_value = st.sidebar.number_input("Safety Order (EUR)", value=50.0, min_value=1.0, step=10.0, key="safety_order_value")

# ── DCA parameters ────────────────────────────────────────────────────────────
st.sidebar.markdown("**DCA Parameters**")
deviation_pct     = st.sidebar.number_input(
    "Price Deviation (%)", value=0.37, min_value=0.01, step=0.01, key="deviation_pct",
    help="Each SO opens this % further below the previous SO")
step_multiplier   = st.sidebar.number_input("Step Multiplier", value=0.9, min_value=0.1, step=0.1, key="step_multiplier")
volume_scale      = st.sidebar.number_input(
    "Volume Scale", value=1.05, min_value=1.0, step=0.01, key="volume_scale",
    help="Spot: keep 1.0–1.10. Higher values exhaust balance fast without leverage.")
take_profit_pct   = st.sidebar.number_input(
    "Take Profit (%)", value=10.0, min_value=0.01, step=0.1, key="take_profit_pct",
    help="Minimum 0.5% to cover round-trip fees (0.25% buy + 0.25% sell)")
max_safety_orders = st.sidebar.slider("Max Safety Orders", 1, 20, 10, key="max_safety_orders")

# ── Entry filters ─────────────────────────────────────────────────────────────
st.sidebar.markdown("**Entry Filters**")
rsi_filter    = st.sidebar.checkbox("RSI Filter", value=False, key="rsi_filter",
                                    help="Only open a trade when RSI is below threshold (avoid overbought entries)")
rsi_period    = st.sidebar.number_input("RSI Period",    value=14, min_value=2,  step=1,  key="rsi_period",    disabled=not rsi_filter)
rsi_threshold = st.sidebar.number_input("RSI Threshold", value=55.0, min_value=1.0, max_value=99.0, step=1.0,
                                         key="rsi_threshold", disabled=not rsi_filter,
                                         help="Only enter if RSI < this value. 50 = neutral, 40 = oversold area")

sma_filter = st.sidebar.checkbox("SMA Filter", value=False, key="sma_filter",
                                  help="Only open a trade when price is below the SMA (mean-reversion entries)")
sma_period = st.sidebar.number_input("SMA Period", value=50, min_value=2, step=1, key="sma_period", disabled=not sma_filter,
                                      help="Simple moving average period (e.g. 50 = last 50 candles)")

ema_filter = st.sidebar.checkbox("EMA Filter", value=False, key="ema_filter",
                                  help="Only open a trade when price is below EMA(n) — reacts faster than SMA")
ema_period = st.sidebar.number_input("EMA Period", value=21, min_value=2, step=1, key="ema_period",
                                      disabled=not ema_filter,
                                      help="Exponential moving average period (e.g. 21 = short-term trend)")

volume_filter     = st.sidebar.checkbox("Volume Filter", value=False, key="volume_filter",
                                         help="Only open a trade when volume exceeds its moving average — confirms activity")
volume_ma_period  = st.sidebar.number_input("Volume MA Period", value=20, min_value=2, step=1,
                                             key="volume_ma_period", disabled=not volume_filter,
                                             help="SMA period applied to volume (e.g. 20 = recent average activity)")
volume_multiplier = st.sidebar.number_input("Volume Multiplier", value=1.0, min_value=0.1, step=0.1,
                                             key="volume_multiplier", disabled=not volume_filter,
                                             help="Enter only when volume > multiplier × volume SMA (e.g. 1.5 = 50% above average)")

bb_filter = st.sidebar.checkbox("Bollinger Bands Filter", value=False, key="bb_filter",
                                 help="Only open a trade when price is below the lower Bollinger Band — oversold entries")
bb_period = st.sidebar.number_input("BB Period", value=20, min_value=2, step=1, key="bb_period",
                                     disabled=not bb_filter,
                                     help="Rolling window for BB middle band and std dev")
bb_std    = st.sidebar.number_input("BB Std Dev", value=2.0, min_value=0.1, step=0.1, key="bb_std",
                                     disabled=not bb_filter,
                                     help="Number of standard deviations for the bands (typically 2.0)")

macd_filter  = st.sidebar.checkbox("MACD Filter", value=False, key="macd_filter",
                                    help="Only open a trade when MACD conditions are met")
macd_fast    = st.sidebar.number_input("MACD Fast",   value=12, min_value=2, step=1, key="macd_fast",   disabled=not macd_filter)
macd_slow    = st.sidebar.number_input("MACD Slow",   value=26, min_value=2, step=1, key="macd_slow",   disabled=not macd_filter)
macd_signal  = st.sidebar.number_input("MACD Signal", value=9,  min_value=2, step=1, key="macd_signal", disabled=not macd_filter)
macd_mode    = st.sidebar.selectbox(
    "MACD Entry Mode",
    ["histogram_positive", "macd_above_signal", "macd_above_zero"],
    key="macd_mode", disabled=not macd_filter,
    help=(
        "histogram_positive: histogram > 0 (momentum turning up)\n"
        "macd_above_signal: MACD line > signal line (golden cross)\n"
        "macd_above_zero: MACD line > 0 (long-term bullish bias)"
    ),
)

candle_pattern_filter = st.sidebar.checkbox(
    "Candlestick Pattern Filter", value=False, key="candle_pattern_filter",
    help="Only open a trade when one of the selected candlestick patterns is detected",
)
candle_patterns = st.sidebar.multiselect(
    "Patterns",
    ["Hammer", "Bullish Engulfing", "Doji", "Morning Star"],
    default=["Hammer", "Bullish Engulfing"],
    key="candle_patterns",
    disabled=not candle_pattern_filter,
    help=(
        "Hammer: small body at top, long lower wick ≥2× body\n"
        "Bullish Engulfing: current green body fully engulfs previous body\n"
        "Doji: body < 10% of total candle range\n"
        "Morning Star: 3-candle reversal pattern (bearish, small body, bullish)"
    ),
)
candle_vol_confirm = st.sidebar.checkbox(
    "Volume Confirmation", value=False, key="candle_vol_confirm",
    disabled=not candle_pattern_filter,
    help="Pattern only counts if accompanied by above-average volume (volume > volume MA)",
)
candle_confirm_candle = st.sidebar.checkbox(
    "Confirmation Candle", value=False, key="candle_confirm_candle",
    disabled=not candle_pattern_filter,
    help="Only enter if the candle AFTER the pattern closes in the bullish direction",
)

regime_filter = st.sidebar.checkbox(
    "Market Regime Filter", value=False, key="regime_filter",
    help="Filter entries based on ADX-detected market regime (trending vs ranging)",
)
regime_mode = st.sidebar.selectbox(
    "Regime Mode",
    ["Trending (ADX > threshold)", "Ranging (ADX < threshold)", "Any"],
    key="regime_mode",
    disabled=not regime_filter,
    help=(
        "Trending: ADX above threshold — enter only in trending markets\n"
        "Ranging: ADX below threshold — enter only in sideways markets\n"
        "Any: no regime filter applied"
    ),
)
adx_period    = st.sidebar.number_input(
    "ADX Period", value=14, min_value=2, step=1, key="adx_period",
    disabled=not regime_filter,
    help="Period for the ADX (Average Directional Index) calculation",
)
adx_threshold = st.sidebar.number_input(
    "ADX Threshold", value=25.0, min_value=1.0, max_value=99.0, step=1.0,
    key="adx_threshold",
    disabled=not regime_filter,
    help="ADX values above this indicate a trending market; below indicate ranging",
)
trend_direction_filter = st.sidebar.checkbox(
    "Trend Direction Filter", value=False, key="trend_direction_filter",
    disabled=not regime_filter,
    help="Only enter when EMA slope confirms uptrend (EMA rising) — combines with regime filter",
)
trend_ema_period = st.sidebar.number_input(
    "Trend EMA Period", value=50, min_value=2, step=1, key="trend_ema_period",
    disabled=not (regime_filter and trend_direction_filter),
    help="EMA period used to determine trend direction (slope > 0 = uptrend)",
)

bos_filter   = st.sidebar.checkbox("Break of Structure (BOS) Filter", value=False, key="bos_filter",
                                    help="Only enter after a confirmed bullish break of structure (price closes above a prior swing high)")
bos_lookback = st.sidebar.number_input("BOS Swing Lookback", value=10, min_value=2, step=1, key="bos_lookback",
                                        disabled=not bos_filter,
                                        help="Candles on each side to confirm a swing high/low (higher = fewer but stronger swings)")
bos_recency  = st.sidebar.number_input("BOS Recency (candles)", value=5, min_value=1, step=1, key="bos_recency",
                                        disabled=not bos_filter,
                                        help="Entry is valid only if a bullish BOS occurred within the last N candles")

sr_filter        = st.sidebar.checkbox("Support / Resistance Filter", value=True, key="sr_filter",
                                        help="Only enter when price is within X% above the rolling support level")
sr_lookback      = st.sidebar.number_input("S&R Lookback (candles)", value=5, min_value=1, step=1,
                                            key="sr_lookback", disabled=not sr_filter,
                                            help="Rolling window to find recent support (low) and resistance (high) levels")
sr_proximity_pct = st.sidebar.number_input("Proximity to Support (%)", value=9.8, min_value=0.1, step=0.1,
                                            key="sr_proximity_pct", disabled=not sr_filter,
                                            help="Enter only when close ≤ support × (1 + X%) — i.e. close to support")

ma_cross_filter = st.sidebar.checkbox("MA Cross / Trend Filter", value=False, key="ma_cross_filter",
                                       help="Only enter when a Golden Cross regime or fresh MA crossover is detected")
ma_cross_type   = st.sidebar.selectbox("MA Type", ["SMA", "EMA"], key="ma_cross_type",
                                        disabled=not ma_cross_filter,
                                        help="Moving average type used for fast and slow lines")
ma_fast_period  = st.sidebar.number_input("Fast MA Period", value=50,  min_value=2, step=1, key="ma_fast_period",
                                           disabled=not ma_cross_filter,
                                           help="Fast moving average period (e.g. 50)")
ma_slow_period  = st.sidebar.number_input("Slow MA Period", value=200, min_value=2, step=1, key="ma_slow_period",
                                           disabled=not ma_cross_filter,
                                           help="Slow moving average period (e.g. 200)")
ma_cross_mode   = st.sidebar.selectbox(
    "MA Cross Mode",
    ["golden_cross_regime", "fresh_crossover"],
    key="ma_cross_mode", disabled=not ma_cross_filter,
    help=(
        "golden_cross_regime: fast > slow (bullish regime, any candle)\n"
        "fresh_crossover: fast just crossed above slow within last 3 candles"
    ),
)

atr_dynamic  = st.sidebar.checkbox("ATR Dynamic TP/SL", value=False, key="atr_dynamic",
                                    help="Replace fixed TP% and Stop Loss% with ATR-based levels")
atr_period   = st.sidebar.number_input("ATR Period", value=14, min_value=2, step=1, key="atr_period",
                                        disabled=not atr_dynamic,
                                        help="Period for Wilder-smoothed Average True Range")
atr_tp_mult  = st.sidebar.number_input("ATR TP Multiplier", value=2.0, min_value=0.1, step=0.1, key="atr_tp_mult",
                                        disabled=not atr_dynamic,
                                        help="TP = avg_entry + N × ATR (overrides fixed TP%)")
atr_sl_mult  = st.sidebar.number_input("ATR SL Multiplier", value=1.5, min_value=0.1, step=0.1, key="atr_sl_mult",
                                        disabled=not atr_dynamic,
                                        help="SL = avg_entry − N × ATR (only active when Stop Loss is also enabled)")

# ── Exit options ──────────────────────────────────────────────────────────────
st.sidebar.markdown("**Exit Options**")
trailing_tp   = st.sidebar.checkbox("Trailing Take Profit", value=False, key="trailing_tp",
                                     help="Activate at TP%, then trail below the peak price")
trail_pct     = st.sidebar.number_input(
    "Trail Distance (%)", value=0.3, min_value=0.05, step=0.05, key="trail_pct", disabled=not trailing_tp,
    help="Sell when price falls this % below the highest price since TP activated")

stop_loss_enabled = st.sidebar.checkbox("Stop Loss", value=True, key="stop_loss_enabled",
                                         help="Close trade if price drops this % below average buy price")
stop_loss_pct     = st.sidebar.number_input(
    "Stop Loss (%)", value=17.5, min_value=0.1, step=0.5, key="stop_loss_pct", disabled=not stop_loss_enabled,
    help="Close at a loss if avg price drops by this %. Accounts for all filled SOs.")

time_stop_enabled = st.sidebar.checkbox("Time Stop", value=False, key="time_stop_enabled",
                                         help="Close trade at market price after N hours open")
time_stop_hours   = st.sidebar.number_input(
    "Max Trade Duration (hours)", value=24, min_value=1, step=1, key="time_stop_hours", disabled=not time_stop_enabled)

# ── Backtest period ───────────────────────────────────────────────────────────
st.sidebar.markdown("**Backtest Period**")
date_from = st.sidebar.date_input("From", value=date(2025, 1, 1), key="date_from")
date_to   = st.sidebar.date_input("To",   value=date.today(), key="date_to")

if date_from >= date_to:
    st.sidebar.error("'From' must be before 'To'.")

_itvl_min        = INTERVAL_MINUTES[interval]
candles_expected = max((date_to - date_from).days * 24 * 60 // _itvl_min, 1)
_dl_est          = candles_expected / 1440 * 0.5  # ~1440 candles/request
st.sidebar.caption(
    f"≈ {candles_expected:,} candles ({interval}) — est. download {_dl_est:.0f}s"
    + (" ⚠️ large range" if candles_expected > 200_000 else "")
)

fee_rate    = st.sidebar.number_input(
    "Taker Fee (%)", value=0.25, min_value=0.0, step=0.05, format="%.2f", key="fee_rate_pct"
) / 100
compounding = st.sidebar.checkbox("Enable Compounding", value=True, key="compounding")

# ── Capital at risk preview ───────────────────────────────────────────────────
def _capital_needed(balance, mode, base_val, so_val, vol_scale, n_so):
    base = balance * base_val / 100 if mode == "Percentage" else base_val
    so   = balance * so_val   / 100 if mode == "Percentage" else so_val
    total, alloc = base, so
    for _ in range(n_so):
        total += alloc
        alloc *= vol_scale
    return total

cap_needed = _capital_needed(initial_balance, order_mode, base_order_value,
                              safety_order_value, volume_scale, max_safety_orders)
cap_pct    = cap_needed / initial_balance * 100
cap_icon   = "🟢" if cap_pct <= 90 else "🟡" if cap_pct <= 110 else "🔴"
st.sidebar.markdown(f"**{cap_icon} Max capital at risk:** €{cap_needed:,.0f} ({cap_pct:.0f}%)")
if cap_pct > 100:
    st.sidebar.warning(
        f"Settings require €{cap_needed:,.0f} but balance is €{initial_balance:,.0f}. "
        "Later SOs will be skipped when balance runs out."
    )

run_button = st.sidebar.button("🚀 Run Backtest")

# ── Settings Presets ──────────────────────────────────────────────────────────
_PRESET_KEYS = [
    "trading_style", "trading_pair", "interval", "initial_balance", "leverage",
    "order_mode", "base_order_value", "safety_order_value",
    "deviation_pct", "step_multiplier", "volume_scale", "take_profit_pct", "max_safety_orders",
    "rsi_filter", "rsi_period", "rsi_threshold",
    "sma_filter", "sma_period", "ema_filter", "ema_period",
    "volume_filter", "volume_ma_period", "volume_multiplier",
    "bb_filter", "bb_period", "bb_std",
    "macd_filter", "macd_fast", "macd_slow", "macd_signal", "macd_mode",
    "bos_filter", "bos_lookback", "bos_recency",
    "sr_filter", "sr_lookback", "sr_proximity_pct",
    "ma_cross_filter", "ma_cross_type", "ma_fast_period", "ma_slow_period", "ma_cross_mode",
    "atr_dynamic", "atr_period", "atr_tp_mult", "atr_sl_mult",
    "candle_pattern_filter", "candle_patterns", "candle_vol_confirm", "candle_confirm_candle",
    "regime_filter", "regime_mode", "adx_period", "adx_threshold",
    "trend_direction_filter", "trend_ema_period",
    "trailing_tp", "trail_pct",
    "stop_loss_enabled", "stop_loss_pct",
    "time_stop_enabled", "time_stop_hours",
    "date_from", "date_to", "fee_rate_pct", "compounding",
    "opt_min_trades",
]

st.sidebar.divider()
with st.sidebar.expander("💾 Settings Presets", expanded=False):
    st.caption("Save your current sidebar settings under a name, then reload them any time.")

    _preset_name = st.text_input("Preset name", placeholder="e.g. BTC aggressive", key="_preset_name_input")
    _col_save, _col_del = st.columns(2)

    if _col_save.button("💾 Save", key="_save_preset_btn", use_container_width=True):
        if _preset_name.strip():
            _snapshot = {}
            for _k in _PRESET_KEYS:
                _v = st.session_state.get(_k)
                if _v is not None:
                    # Convert date objects to ISO string for JSON compatibility
                    _snapshot[_k] = _v.isoformat() if hasattr(_v, "isoformat") else _v
            st.session_state["presets"][_preset_name.strip()] = _snapshot
            st.success(f"Saved **{_preset_name.strip()}**")
        else:
            st.warning("Enter a preset name first.")

    # JSON export
    if st.session_state["presets"]:
        _export_str = _json.dumps(st.session_state["presets"], indent=2, default=str)
        st.download_button(
            "⬇️ Export all presets (JSON)",
            data=_export_str,
            file_name="dca_presets.json",
            mime="application/json",
            use_container_width=True,
            key="_export_presets_btn",
        )

    # JSON import
    _upload = st.file_uploader("⬆️ Import presets (JSON)", type="json", key="_import_presets_upload")
    if _upload:
        try:
            _imported = _json.load(_upload)
            st.session_state["presets"].update(_imported)
            st.success(f"Imported {len(_imported)} preset(s).")
        except Exception as _e:
            st.error(f"Import failed: {_e}")

    # List saved presets
    if st.session_state["presets"]:
        st.markdown("**Saved presets**")
        for _pname, _pdata in list(st.session_state["presets"].items()):
            _pc1, _pc2, _pc3 = st.columns([3, 1, 1])
            _pc1.markdown(f"`{_pname}`")
            if _pc2.button("Load", key=f"_load_{_pname}", use_container_width=True):
                # Convert ISO date strings back to date objects before loading
                _load_data = {}
                for _k, _v in _pdata.items():
                    if _k in ("date_from", "date_to") and isinstance(_v, str):
                        try:
                            from datetime import date as _date_cls
                            _load_data[_k] = _date_cls.fromisoformat(_v)
                        except Exception:
                            _load_data[_k] = _v
                    else:
                        _load_data[_k] = _v
                st.session_state["_pending_load"] = _load_data
                st.rerun()
            if _pc3.button("🗑️", key=f"_del_{_pname}", use_container_width=True):
                del st.session_state["presets"][_pname]
                st.rerun()
    else:
        st.info("No presets saved yet.")

# ── Optimizer sidebar ─────────────────────────────────────────────────────────
st.sidebar.divider()
with st.sidebar.expander("🤖 Auto-Optimizer", expanded=False):
    st.caption(
        "**Optuna TPE** (Bayesian search) — learns from each trial which regions "
        "look promising. Tests pairs, timeframes and order sizing automatically."
    )
    st.markdown("**Study persistence**")
    opt_study_name = st.text_input(
        "Study name", value="dca_study",
        key="opt_study_name",
        help="Trials are saved to a SQLite file next to the script. Use the same name to continue a previous run.",
    )
    _db_path  = str(__import__("pathlib").Path(__file__).parent / f"{opt_study_name.strip() or 'dca_study'}.db")
    _db_exists = __import__("os.path").path.exists(_db_path)
    if _db_exists:
        import optuna as _optuna_peek
        try:
            _peek = _optuna_peek.load_study(study_name=opt_study_name.strip(), storage=f"sqlite:///{_db_path}")
            st.caption(f"📂 Existing study: **{len(_peek.trials)} trials** saved — new run will continue from here.")
        except Exception:
            st.caption(f"📂 DB file exists but no study named `{opt_study_name.strip()}` yet.")
    else:
        st.caption("🆕 No existing study — will be created on first run.")
    if _db_exists and st.button("🗑️ Clear study (start fresh)", key="opt_clear_study"):
        import os as _os
        _os.remove(_db_path)
        st.success("Study cleared.")
        st.rerun()

    opt_metric = st.selectbox(
        "Optimize for",
        ["Net Profit (EUR)", "Win Rate (%)", "Sharpe Ratio", "Profit Factor", "Calmar Ratio",
         "Composite (Profit × Sharpe)"],
        key="opt_metric",
        help=(
            "Composite (Profit × Sharpe): optimises net_profit × sharpe_ratio (both normalised) "
            "to prevent overfitting to a single metric."
        ),
    )
    opt_min_trades = st.number_input(
        "Min Trades (penalty)", value=5, min_value=1, step=1, key="opt_min_trades",
        help=(
            "Trials with fewer than this number of trades are penalised. "
            "Prevents the optimizer from picking strategies that got lucky on 1–2 trades."
        ),
    )
    opt_trials = st.number_input(
        "Number of Trials", value=75, min_value=10, max_value=500, step=25,
        help="Each trial tests one full parameter set. 75–150 is usually enough.",
        key="opt_trials",
    )
    opt_wf_split = st.slider(
        "Train / Validate split (%)", 50, 90, 70, step=5,
        help="First X% of candles for optimization; remaining for validation.",
        key="opt_wf_split",
    )

    st.markdown("**Assets & Timeframes**")
    opt_pairs = st.multiselect(
        "Trading Pairs", ["BTC-EUR", "ETH-EUR", "XRP-EUR", "SOL-EUR"],
        default=[trading_pair], key="opt_pairs",
        help="All selected pairs will be downloaded and tested.",
    )
    opt_intervals = st.multiselect(
        "Timeframes", list(INTERVAL_MINUTES.keys()),
        default=[interval], key="opt_intervals",
        help="Each (pair × timeframe) combination is pre-downloaded once then reused.",
    )

    st.markdown("**Order Sizing ranges** *(min → max)*")
    _sz_unit = "%" if order_mode == "Percentage" else "EUR"
    _c1, _c2 = st.columns(2)
    _def_base = base_order_value
    _def_so   = safety_order_value
    opt_base_min = _c1.number_input(f"Base {_sz_unit} min", value=max(_def_base * 0.5, 0.1),
                                     min_value=0.1, step=0.5 if order_mode == "Percentage" else 5.0,
                                     key="opt_base_min", format="%.1f")
    opt_base_max = _c2.number_input(f"Base {_sz_unit} max", value=_def_base * 2.0,
                                     min_value=0.2, step=0.5 if order_mode == "Percentage" else 5.0,
                                     key="opt_base_max", format="%.1f")
    opt_so_size_min = _c1.number_input(f"Safety {_sz_unit} min", value=max(_def_so * 0.5, 0.1),
                                        min_value=0.1, step=0.5 if order_mode == "Percentage" else 5.0,
                                        key="opt_so_size_min", format="%.1f")
    opt_so_size_max = _c2.number_input(f"Safety {_sz_unit} max", value=_def_so * 2.0,
                                        min_value=0.2, step=0.5 if order_mode == "Percentage" else 5.0,
                                        key="opt_so_size_max", format="%.1f")

    st.markdown("**DCA parameter ranges** *(min → max)*")
    opt_tp_min   = _c1.number_input("TP % min",      value=0.3,  min_value=0.1, step=0.1,  key="opt_tp_min",   format="%.1f")
    opt_tp_max   = _c2.number_input("TP % max",      value=5.0,  min_value=0.2, step=0.5,  key="opt_tp_max",   format="%.1f")
    opt_dev_min  = _c1.number_input("Dev % min",     value=0.3,  min_value=0.1, step=0.1,  key="opt_dev_min",  format="%.1f")
    opt_dev_max  = _c2.number_input("Dev % max",     value=3.0,  min_value=0.2, step=0.5,  key="opt_dev_max",  format="%.1f")
    opt_so_min   = _c1.number_input("SOs min",       value=2,    min_value=1,   step=1,    key="opt_so_min")
    opt_so_max   = _c2.number_input("SOs max",       value=10,   min_value=2,   step=1,    key="opt_so_max")
    opt_vs_min   = _c1.number_input("VolScale min",  value=1.0,  min_value=1.0, step=0.05, key="opt_vs_min",   format="%.2f")
    opt_vs_max   = _c2.number_input("VolScale max",  value=1.3,  min_value=1.0, step=0.05, key="opt_vs_max",   format="%.2f")
    opt_step_min = _c1.number_input("StepMul min",   value=0.8,  min_value=0.1, step=0.1,  key="opt_step_min", format="%.1f")
    opt_step_max = _c2.number_input("StepMul max",   value=2.0,  min_value=0.2, step=0.1,  key="opt_step_max", format="%.1f")
    if rsi_filter:
        opt_rsi_min = _c1.number_input("RSI Thr min", value=30.0, min_value=1.0,  max_value=98.0, step=5.0, key="opt_rsi_min")
        opt_rsi_max = _c2.number_input("RSI Thr max", value=75.0, min_value=2.0,  max_value=99.0, step=5.0, key="opt_rsi_max")
    else:
        opt_rsi_min = opt_rsi_max = rsi_threshold

    if stop_loss_enabled:
        st.markdown("**Stop Loss range** *(min → max)*")
        opt_sl_min = _c1.number_input("SL % min", value=2.0, min_value=0.1, step=0.5, key="opt_sl_min", format="%.1f")
        opt_sl_max = _c2.number_input("SL % max", value=15.0, min_value=0.5, step=0.5, key="opt_sl_max", format="%.1f")
    else:
        opt_sl_min = opt_sl_max = stop_loss_pct

    if sr_filter:
        st.markdown("**S&R Filter ranges** *(min → max)*")
        opt_sr_lb_min   = _c1.number_input("S&R Lookback min", value=20,  min_value=5,  step=5,   key="opt_sr_lb_min")
        opt_sr_lb_max   = _c2.number_input("S&R Lookback max", value=100, min_value=10, step=10,  key="opt_sr_lb_max")
        opt_sr_prox_min = _c1.number_input("SR Prox % min",    value=0.5, min_value=0.1, step=0.5, key="opt_sr_prox_min", format="%.1f")
        opt_sr_prox_max = _c2.number_input("SR Prox % max",    value=5.0, min_value=0.5, step=0.5, key="opt_sr_prox_max", format="%.1f")
    else:
        opt_sr_lb_min = opt_sr_lb_max = sr_lookback
        opt_sr_prox_min = opt_sr_prox_max = sr_proximity_pct

    st.caption(
        "ℹ️ **Timeframe** is already swept automatically — add multiple timeframes in "
        "**Assets & Timeframes** above and the optimizer will test each combination."
    )
    run_opt_btn = st.button("🤖 Run Auto-Optimizer", key="run_opt")


# ─────────────────────────────────────────────────────────────────────────────
# DATA
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False)
def download_history(market: str, date_from: date, date_to: date, interval: str) -> list:
    itvl_ms     = INTERVAL_MINUTES[interval] * 60_000
    start_ms    = int(dt.datetime.combine(date_from, dt.time.min).timestamp() * 1000)
    end_ms      = int(dt.datetime.combine(date_to,   dt.time.max).timestamp() * 1000)
    total_exp   = max(int((end_ms - start_ms) / itvl_ms), 1)
    url         = f"https://api.bitvavo.com/v2/{market}/candles"
    candles     = []
    current_end = end_ms
    progress    = st.progress(0, text="Downloading candles from Bitvavo…")

    while current_end > start_ms:
        try:
            resp = requests.get(url,
                                params={"interval": interval, "limit": 1440,
                                        "start": start_ms, "end": current_end},
                                timeout=15)
            data = resp.json()
        except Exception as e:
            st.error(f"Download error: {e}")
            break
        if isinstance(data, dict) and "errorCode" in data:
            st.error(f"Bitvavo API error: {data}")
            break
        if not data:
            break
        for c in data:
            candles.append({"time": int(c[0]), "open": float(c[1]),
                             "high": float(c[2]), "low": float(c[3]), "close": float(c[4]),
                             "volume": float(c[5]) if len(c) > 5 else 0.0})
        oldest = min(int(c[0]) for c in data)
        if oldest <= start_ms:
            break
        current_end = oldest - itvl_ms
        progress.progress(min(len(candles) / total_exp, 1.0),
                          text=f"Downloaded {len(candles):,} candles…")
        time.sleep(0.15)

    progress.empty()
    candles.sort(key=lambda x: x["time"])
    seen, unique = set(), []
    for c in candles:
        if c["time"] not in seen:
            seen.add(c["time"])
            unique.append(c)
    return unique


# ─────────────────────────────────────────────────────────────────────────────
# INDICATORS
# ─────────────────────────────────────────────────────────────────────────────
def compute_rsi(closes: pd.Series, period: int) -> pd.Series:
    """Wilder-smoothed RSI — same as TradingView default."""
    delta    = closes.diff()
    gain     = delta.clip(lower=0)
    loss     = (-delta).clip(lower=0)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period).mean()
    rs       = avg_gain / avg_loss.replace(0, 1e-10)
    return (100 - 100 / (1 + rs)).where(avg_loss > 0, 100)


def compute_sma(closes: pd.Series, period: int) -> pd.Series:
    return closes.rolling(period).mean()


def compute_ema(closes: pd.Series, period: int) -> pd.Series:
    """Exponential Moving Average — same alpha as TradingView default."""
    return closes.ewm(span=period, adjust=False).mean()


def compute_bollinger_bands(
    closes: pd.Series, period: int, num_std: float
) -> tuple:
    """Returns (upper_band, middle_band, lower_band)."""
    middle = closes.rolling(period).mean()
    std    = closes.rolling(period).std(ddof=0)
    return middle + num_std * std, middle, middle - num_std * std


def compute_macd(
    closes: pd.Series, fast: int, slow: int, signal: int
) -> tuple:
    """Returns (macd_line, signal_line, histogram)."""
    fast_ema    = closes.ewm(span=fast,   adjust=False).mean()
    slow_ema    = closes.ewm(span=slow,   adjust=False).mean()
    macd_line   = fast_ema - slow_ema
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    histogram   = macd_line - signal_line
    return macd_line, signal_line, histogram


def compute_bos(
    highs: pd.Series, lows: pd.Series, closes: pd.Series, swing_lookback: int
) -> pd.Series:
    """Break of Structure: +1 = bullish BOS confirmed, -1 = bearish BOS, 0 = none.
    Swing points use a rolling window centred on each candle; the result is
    shifted by (lb+1) candles so no future data leaks into the signal."""
    lb = swing_lookback
    win = 2 * lb + 1
    swing_high = highs.rolling(win, center=True).max()
    swing_low  = lows.rolling(win,  center=True).min()
    # Shift forward so the swing is only 'confirmed' after lb right-side bars
    prev_sh = swing_high.shift(lb + 1).ffill()
    prev_sl = swing_low.shift(lb  + 1).ffill()
    bos = pd.Series(0, index=closes.index, dtype=int)
    bos[closes > prev_sh] =  1
    bos[closes < prev_sl] = -1
    return bos


def compute_support(lows: pd.Series, lookback: int) -> pd.Series:
    """Rolling support level = lowest low over the last `lookback` candles."""
    return lows.rolling(lookback).min()


def compute_resistance(highs: pd.Series, lookback: int) -> pd.Series:
    """Rolling resistance level = highest high over the last `lookback` candles."""
    return highs.rolling(lookback).max()


def compute_atr(
    highs: pd.Series, lows: pd.Series, closes: pd.Series, period: int
) -> pd.Series:
    """Wilder-smoothed Average True Range."""
    prev_close = closes.shift(1)
    tr = pd.concat([
        highs - lows,
        (highs - prev_close).abs(),
        (lows  - prev_close).abs(),
    ], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / period, min_periods=period).mean()


def compute_adx(
    highs: pd.Series, lows: pd.Series, closes: pd.Series, period: int = 14
) -> pd.Series:
    """Standard ADX using Wilder smoothing.

    Returns the ADX line as a pd.Series aligned to the input index.
    Values are NaN for the first (2 × period) rows while the smoothing
    warms up (same behaviour as TradingView's built-in ADX).
    """
    # True Range
    prev_close = closes.shift(1)
    tr = pd.concat([
        highs - lows,
        (highs - prev_close).abs(),
        (lows  - prev_close).abs(),
    ], axis=1).max(axis=1)

    # Directional movement
    up_move   = highs - highs.shift(1)
    down_move = lows.shift(1) - lows
    plus_dm  = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
    minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)

    # Wilder smoothing
    alpha     = 1.0 / period
    atr_s     = tr.ewm(alpha=alpha, min_periods=period).mean()
    plus_di   = 100 * plus_dm.ewm(alpha=alpha,  min_periods=period).mean() / atr_s.replace(0, 1e-10)
    minus_di  = 100 * minus_dm.ewm(alpha=alpha, min_periods=period).mean() / atr_s.replace(0, 1e-10)

    dx_denom  = (plus_di + minus_di).replace(0, 1e-10)
    dx        = (100 * (plus_di - minus_di).abs() / dx_denom)
    adx       = dx.ewm(alpha=alpha, min_periods=period).mean()
    return adx


def compute_candlestick_patterns(
    opens: pd.Series,
    highs: pd.Series,
    lows: pd.Series,
    closes: pd.Series,
    patterns: list,
) -> pd.Series:
    """Pre-compute a boolean Series — True when at least one of the requested
    candlestick patterns is detected on that candle.

    Supported patterns: Hammer, Bullish Engulfing, Doji, Morning Star.
    All calculations are fully vectorised (no Python loop per candle).
    """
    n      = len(closes)
    result = pd.Series(False, index=closes.index)

    body   = (closes - opens).abs()
    candle_range = highs - lows
    # Avoid division by zero for doji / flat candles
    safe_range = candle_range.replace(0, 1e-10)
    safe_body  = body.replace(0, 1e-10)

    upper_wick = highs  - closes.where(closes >= opens, opens)
    lower_wick = closes.where(closes >= opens, opens) - lows

    if "Hammer" in patterns:
        # Conditions: long lower wick ≥ 2× body, upper wick ≤ body
        hammer = (lower_wick >= 2 * safe_body) & (upper_wick <= body)
        result = result | hammer

    if "Bullish Engulfing" in patterns:
        prev_open  = opens.shift(1)
        prev_close = closes.shift(1)
        # Current candle is green AND its body fully engulfs previous body
        curr_green  = closes > opens
        eng_high    = closes > prev_open.where(prev_open > prev_close, prev_close)
        eng_low     = opens  < prev_close.where(prev_close < prev_open, prev_open)
        bull_eng    = curr_green & eng_high & eng_low
        result      = result | bull_eng

    if "Doji" in patterns:
        doji   = body < 0.10 * safe_range
        result = result | doji

    if "Morning Star" in patterns:
        # Candle i-2: bearish (close < open)
        # Candle i-1: small body (< 30% of i-2 range)
        # Candle i  : bullish, closes above midpoint of i-2
        prev2_open  = opens.shift(2)
        prev2_close = closes.shift(2)
        prev2_range = (highs.shift(2) - lows.shift(2)).replace(0, 1e-10)
        prev1_body  = (closes.shift(1) - opens.shift(1)).abs()

        first_bear    = prev2_close < prev2_open
        second_small  = prev1_body < 0.30 * prev2_range
        third_bull    = closes > opens
        midpoint      = (prev2_open + prev2_close) / 2
        third_above   = closes > midpoint

        morning_star  = first_bear & second_small & third_bull & third_above
        result        = result | morning_star

    # First 2 candles can never satisfy multi-candle patterns — leave as False
    return result


# ─────────────────────────────────────────────────────────────────────────────
# ENGINE
# ─────────────────────────────────────────────────────────────────────────────
class DCASpotBacktester:
    def __init__(self, params: dict):
        self.balance         = params["initial_balance"]
        self.initial_balance = params["initial_balance"]
        self.deviation_pct   = params["deviation_pct"]
        self.step_multiplier = params["step_multiplier"]
        self.volume_scale    = params["volume_scale"]
        self.tp_pct          = params["take_profit_pct"]
        self.max_so          = params["max_safety_orders"]
        self.order_mode      = params["order_mode"]
        self.base_value      = params["base_order"]
        self.so_value        = params["safety_order"]
        self.compounding     = params["compounding"]
        self.fee_rate        = params["fee_rate"]
        # Entry filters
        self.rsi_filter      = params["rsi_filter"]
        self.rsi_period      = params["rsi_period"]
        self.rsi_threshold   = params["rsi_threshold"]
        self.sma_filter       = params["sma_filter"]
        self.sma_period       = params["sma_period"]
        self.ema_filter       = params["ema_filter"]
        self.ema_period       = params["ema_period"]
        self.volume_filter    = params["volume_filter"]
        self.volume_ma_period = params["volume_ma_period"]
        self.volume_mult      = params["volume_multiplier"]
        self.bb_filter        = params["bb_filter"]
        self.bb_period        = params["bb_period"]
        self.bb_std           = params["bb_std"]
        self.macd_filter      = params["macd_filter"]
        self.macd_fast        = params["macd_fast"]
        self.macd_slow        = params["macd_slow"]
        self.macd_signal      = params["macd_signal"]
        self.macd_mode        = params["macd_mode"]
        # Advanced TA filters
        self.bos_filter       = params["bos_filter"]
        self.bos_lookback     = params["bos_lookback"]
        self.bos_recency      = params["bos_recency"]
        self.sr_filter        = params["sr_filter"]
        self.sr_lookback      = params["sr_lookback"]
        self.sr_proximity_pct = params["sr_proximity_pct"]
        self.ma_cross_filter  = params["ma_cross_filter"]
        self.ma_cross_type    = params["ma_cross_type"]
        self.ma_fast_period   = params["ma_fast_period"]
        self.ma_slow_period   = params["ma_slow_period"]
        self.ma_cross_mode    = params["ma_cross_mode"]
        self.atr_dynamic      = params["atr_dynamic"]
        self.atr_period       = params["atr_period"]
        self.atr_tp_mult      = params["atr_tp_mult"]
        self.atr_sl_mult      = params["atr_sl_mult"]
        # Candlestick pattern filter
        self.candle_pattern_filter  = params["candle_pattern_filter"]
        self.candle_patterns        = params["candle_patterns"]
        self.candle_vol_confirm     = params.get("candle_vol_confirm", False)
        self.candle_confirm_candle  = params.get("candle_confirm_candle", False)
        # Market regime filter
        self.regime_filter          = params["regime_filter"]
        self.regime_mode            = params["regime_mode"]
        self.adx_period             = params["adx_period"]
        self.adx_threshold          = params["adx_threshold"]
        self.trend_direction_filter = params.get("trend_direction_filter", False)
        self.trend_ema_period       = params.get("trend_ema_period", 50)
        # Leverage & timeframe
        self.leverage        = params["leverage"]
        self.candle_min      = params["candle_min"]
        # Exit options
        self.trailing_tp     = params["trailing_tp"]
        self.trail_pct       = params["trail_pct"]
        self.stop_loss       = params["stop_loss_enabled"]
        self.stop_loss_pct   = params["stop_loss_pct"]
        self.time_stop       = params["time_stop_enabled"]
        self.time_stop_min   = params["time_stop_hours"] * 60

    def _get_order_sizes(self):
        ref = self.balance if self.compounding else self.initial_balance
        if self.order_mode == "Percentage":
            return ref * self.base_value / 100, ref * self.so_value / 100
        return self.base_value, self.so_value

    def _build_ladder(self, entry: float, so_base: float) -> list:
        ladder, cum, alloc = [], 0.0, so_base
        for n in range(self.max_so):
            cum   += (self.deviation_pct / 100) * (self.step_multiplier ** n)
            ladder.append((entry * (1 - cum), alloc))
            alloc *= self.volume_scale
        return ladder

    def _buy(self, eur: float, price: float):
        fee      = eur * self.fee_rate
        cost_net = eur - fee
        return cost_net / price, cost_net, fee   # (coins, cost_basis, fee_eur)

    def run(self, candles: list):
        # ── Pre-compute indicators ────────────────────────────────────────────
        closes   = pd.Series([c["close"] for c in candles])
        highs    = pd.Series([c["high"]  for c in candles])
        lows     = pd.Series([c["low"]   for c in candles])

        rsi_vals = compute_rsi(closes, self.rsi_period).tolist() if self.rsi_filter else [0.0] * len(closes)
        sma_vals = compute_sma(closes, self.sma_period).tolist() if self.sma_filter else [float("inf")] * len(closes)
        ema_vals = compute_ema(closes, self.ema_period).tolist() if self.ema_filter else [float("inf")] * len(closes)

        volumes      = pd.Series([c.get("volume", 0.0) for c in candles])
        vol_ma_vals  = compute_sma(volumes, self.volume_ma_period).tolist() if self.volume_filter else [0.0] * len(closes)

        if self.bb_filter:
            _, _, bb_lower = compute_bollinger_bands(closes, self.bb_period, self.bb_std)
            bb_lower_vals  = bb_lower.tolist()
        else:
            bb_lower_vals  = [float("-inf")] * len(closes)

        if self.macd_filter:
            macd_line, macd_sig, macd_hist = compute_macd(closes, self.macd_fast, self.macd_slow, self.macd_signal)
            macd_line_vals  = macd_line.tolist()
            macd_sig_vals   = macd_sig.tolist()
            macd_hist_vals  = macd_hist.tolist()
        else:
            macd_line_vals = macd_sig_vals = macd_hist_vals = [0.0] * len(closes)

        # Advanced TA
        if self.bos_filter:
            bos_vals = compute_bos(highs, lows, closes, self.bos_lookback).tolist()
        else:
            bos_vals = [1] * len(closes)   # sentinel: always passes

        if self.sr_filter:
            support_vals = compute_support(lows, self.sr_lookback).tolist()
        else:
            support_vals = [0.0] * len(closes)  # sentinel: close always ≥ 0 so always passes

        if self.ma_cross_filter:
            if self.ma_cross_type == "SMA":
                ma_fast_vals = compute_sma(closes, self.ma_fast_period).tolist()
                ma_slow_vals = compute_sma(closes, self.ma_slow_period).tolist()
            else:
                ma_fast_vals = compute_ema(closes, self.ma_fast_period).tolist()
                ma_slow_vals = compute_ema(closes, self.ma_slow_period).tolist()
        else:
            ma_fast_vals = [1.0] * len(closes)
            ma_slow_vals = [0.0] * len(closes)  # fast > slow always True

        atr_vals = compute_atr(highs, lows, closes, self.atr_period).tolist() if self.atr_dynamic else [0.0] * len(closes)

        # Candlestick pattern filter — pre-compute boolean signal
        opens = pd.Series([c["open"] for c in candles])
        if self.candle_pattern_filter and self.candle_patterns:
            candle_sig_raw = compute_candlestick_patterns(
                opens, highs, lows, closes, self.candle_patterns
            )
            # Volume confirmation: pattern only valid when volume > volume MA
            if self.candle_vol_confirm:
                vol_ma_cp = compute_sma(volumes, self.volume_ma_period if self.volume_filter else 20)
                candle_sig_raw = candle_sig_raw & (volumes > vol_ma_cp)
            # Confirmation candle: shift signal forward by 1 — enter on the candle
            # AFTER the pattern, which must close bullish
            if self.candle_confirm_candle:
                next_bullish = (closes > opens)  # current candle is bullish
                candle_sig_raw = candle_sig_raw.shift(1).fillna(False) & next_bullish
            candle_sig_vals = candle_sig_raw.tolist()
        else:
            candle_sig_vals = [True] * len(closes)   # sentinel: always passes

        # Market regime filter — pre-compute ADX and optional EMA slope
        if self.regime_filter and self.regime_mode != "Any":
            adx_vals = compute_adx(highs, lows, closes, self.adx_period).tolist()
        else:
            adx_vals = [0.0] * len(closes)   # sentinel

        # Trend direction: EMA slope (current EMA > previous EMA = uptrend)
        if self.regime_filter and self.trend_direction_filter:
            trend_ema = compute_ema(closes, self.trend_ema_period)
            trend_up_vals = (trend_ema > trend_ema.shift(1)).tolist()
        else:
            trend_up_vals = [True] * len(closes)   # sentinel

        trades, equity_curve, micro_trades = [], [self.balance], []
        i = 0
        trade_number = 1

        while i < len(candles) - 1:
            # ── Entry filters ─────────────────────────────────────────────────
            close_i = candles[i]["close"]
            rsi_ok = not self.rsi_filter or (
                rsi_vals[i] == rsi_vals[i] and   # not NaN
                rsi_vals[i] < self.rsi_threshold
            )
            sma_ok = not self.sma_filter or (
                sma_vals[i] == sma_vals[i] and
                close_i < sma_vals[i]
            )
            ema_ok = not self.ema_filter or (
                ema_vals[i] == ema_vals[i] and
                close_i < ema_vals[i]
            )
            vol_ok = not self.volume_filter or (
                vol_ma_vals[i] == vol_ma_vals[i] and
                vol_ma_vals[i] > 0 and
                candles[i].get("volume", 0.0) > self.volume_mult * vol_ma_vals[i]
            )
            bb_ok = not self.bb_filter or (
                bb_lower_vals[i] == bb_lower_vals[i] and
                close_i < bb_lower_vals[i]
            )
            if self.macd_filter:
                mode = self.macd_mode
                if mode == "histogram_positive":
                    macd_ok = macd_hist_vals[i] == macd_hist_vals[i] and macd_hist_vals[i] > 0
                elif mode == "macd_above_signal":
                    macd_ok = (macd_line_vals[i] == macd_line_vals[i] and
                               macd_line_vals[i] > macd_sig_vals[i])
                else:  # macd_above_zero
                    macd_ok = macd_line_vals[i] == macd_line_vals[i] and macd_line_vals[i] > 0
            else:
                macd_ok = True

            # BOS filter — any bullish BOS within last bos_recency candles
            if self.bos_filter:
                start_bos = max(0, i - self.bos_recency + 1)
                bos_ok = any(bos_vals[k] == 1 for k in range(start_bos, i + 1))
            else:
                bos_ok = True

            # S&R filter — close is within proximity_pct above rolling support
            sr_ok = not self.sr_filter or (
                support_vals[i] == support_vals[i] and
                support_vals[i] > 0 and
                close_i <= support_vals[i] * (1 + self.sr_proximity_pct / 100)
            )

            # MA Cross / Trend filter
            if self.ma_cross_filter:
                fv = ma_fast_vals[i]
                sv = ma_slow_vals[i]
                if fv != fv or sv != sv:  # NaN
                    ma_cross_ok = False
                elif self.ma_cross_mode == "golden_cross_regime":
                    ma_cross_ok = fv > sv
                else:  # fresh_crossover — fast crossed above slow within last 3 candles
                    ma_cross_ok = False
                    for k in range(max(1, i - 2), i + 1):
                        pk = k - 1
                        if (ma_fast_vals[k] == ma_fast_vals[k] and
                                ma_slow_vals[k] == ma_slow_vals[k] and
                                ma_fast_vals[pk] == ma_fast_vals[pk] and
                                ma_slow_vals[pk] == ma_slow_vals[pk] and
                                ma_fast_vals[k] > ma_slow_vals[k] and
                                ma_fast_vals[pk] <= ma_slow_vals[pk]):
                            ma_cross_ok = True
                            break
            else:
                ma_cross_ok = True

            # Candlestick pattern filter
            candle_ok = not self.candle_pattern_filter or bool(candle_sig_vals[i])

            # Market regime filter (ADX-based)
            if self.regime_filter and self.regime_mode != "Any":
                adx_v = adx_vals[i]
                if adx_v != adx_v:  # NaN — not enough history yet
                    regime_ok = False
                elif self.regime_mode == "Trending (ADX > threshold)":
                    regime_ok = adx_v > self.adx_threshold
                else:  # Ranging (ADX < threshold)
                    regime_ok = adx_v < self.adx_threshold
            else:
                regime_ok = True

            # Trend direction filter (EMA slope)
            trend_ok = not (self.regime_filter and self.trend_direction_filter) or bool(trend_up_vals[i])

            if not (rsi_ok and sma_ok and ema_ok and vol_ok and bb_ok and macd_ok
                    and bos_ok and sr_ok and ma_cross_ok and candle_ok and regime_ok and trend_ok):
                i += 1
                continue

            # ── Open base order ───────────────────────────────────────────────
            entry_price           = candles[i]["close"]
            start_time            = candles[i]["time"]
            base_eur, so_base_eur = self._get_order_sizes()

            if self.balance <= 0 or base_eur <= 0 or base_eur > self.balance:
                break

            coins, cost_basis, fee = self._buy(base_eur, entry_price)
            self.balance -= base_eur
            total_coins   = coins
            total_cost    = cost_basis
            total_spent   = base_eur
            fees_paid     = fee
            avg           = total_cost / total_coins

            micro_trades.append({"trade_id": str(trade_number), "type": "Base Order",
                                  "time": datetime.fromtimestamp(start_time / 1000),
                                  "price": entry_price, "eur": base_eur,
                                  "coins": coins, "fee_eur": fee})

            # ATR at entry — used for dynamic TP/SL if enabled
            atr_at_entry = atr_vals[i] if (self.atr_dynamic and atr_vals[i] == atr_vals[i] and atr_vals[i] > 0) else None

            ladder          = self._build_ladder(entry_price, so_base_eur)
            idx             = 0
            j               = i + 1
            exit_price      = None
            exit_reason     = None
            trailing_active = False
            trail_peak      = 0.0

            # ── Candle loop ───────────────────────────────────────────────────
            while j < len(candles):
                high  = candles[j]["high"]
                low   = candles[j]["low"]
                close = candles[j]["close"]
                ts    = candles[j]["time"]

                # Fill safety orders
                while idx < len(ladder):
                    so_price, so_eur = ladder[idx]
                    if low > so_price:
                        break
                    if so_eur > self.balance:
                        idx += 1
                        continue
                    c, cb, fee    = self._buy(so_eur, so_price)
                    self.balance -= so_eur
                    total_coins  += c
                    total_cost   += cb
                    total_spent  += so_eur
                    fees_paid    += fee
                    avg           = total_cost / total_coins
                    micro_trades.append({"trade_id": f"{trade_number}.{idx+1}",
                                         "type": f"Safety {idx+1}",
                                         "time": datetime.fromtimestamp(ts / 1000),
                                         "price": so_price, "eur": so_eur,
                                         "coins": c, "fee_eur": fee})
                    idx += 1

                # Liquidation (leverage > 1 only)
                if self.leverage > 1 and exit_price is None:
                    liq_price = avg * (self.leverage - 1)
                    if low <= liq_price:
                        exit_price  = liq_price
                        exit_reason = "Liquidated"

                # Stop loss (checked before TP — if both trigger, SL took priority)
                if exit_price is None and self.stop_loss:
                    if self.atr_dynamic and atr_at_entry is not None:
                        stop_price = avg - self.atr_sl_mult * atr_at_entry
                    else:
                        stop_price = avg * (1 - self.stop_loss_pct / 100)
                    if low <= stop_price:
                        exit_price  = stop_price
                        exit_reason = "Stop Loss"

                # Take profit (fixed or trailing)
                if exit_price is None:
                    if self.atr_dynamic and atr_at_entry is not None:
                        tp_price = avg + self.atr_tp_mult * atr_at_entry
                    else:
                        tp_price = avg * (1 + self.tp_pct / 100)
                    if self.trailing_tp:
                        if not trailing_active and high >= tp_price:
                            trailing_active = True
                            trail_peak      = high
                        elif trailing_active:
                            if high > trail_peak:
                                trail_peak = high
                            trail_stop = trail_peak * (1 - self.trail_pct / 100)
                            if low <= trail_stop:
                                exit_price  = trail_stop
                                exit_reason = "Trailing TP"
                    else:
                        if high >= tp_price:
                            exit_price  = tp_price
                            exit_reason = "Take Profit"

                # Time stop
                if exit_price is None and self.time_stop:
                    if (j - i) * self.candle_min >= self.time_stop_min:
                        exit_price  = close
                        exit_reason = "Time Stop"

                if exit_price is not None:
                    proceeds     = total_coins * exit_price
                    sell_fee     = proceeds * self.fee_rate
                    fees_paid   += sell_fee
                    # Leverage: amplify the raw gain/loss; cap loss at full margin
                    gross_profit = (proceeds - total_spent) * self.leverage
                    net_profit   = gross_profit - sell_fee
                    net_profit   = max(net_profit, -total_spent)  # never lose more than margin

                    if self.compounding:
                        self.balance += total_spent + net_profit
                    else:
                        self.balance += total_spent

                    equity_curve.append(self.balance)
                    micro_trades.append({"trade_id": f"{trade_number}+",
                                         "type": exit_reason,
                                         "time": datetime.fromtimestamp(ts / 1000),
                                         "price": exit_price, "eur": proceeds - sell_fee,
                                         "coins": -total_coins, "fee_eur": sell_fee})
                    trades.append({
                        "trade":                trade_number,
                        "entry_time":           datetime.fromtimestamp(start_time / 1000),
                        "exit_time":            datetime.fromtimestamp(ts / 1000),
                        "entry_price":          entry_price,
                        "avg_price":            avg,
                        "tp_price":             avg * (1 + self.tp_pct / 100),
                        "exit_price":           exit_price,
                        "exit_reason":          exit_reason,
                        "gross_profit_eur":     gross_profit,
                        "fees_eur":             fees_paid,
                        "net_profit_eur":       net_profit,
                        "safety_orders_filled": idx,
                        "capital_deployed":     total_spent,
                        "roi_pct":              (net_profit / total_spent) * 100,
                        "rsi_at_entry":    rsi_vals[i]       if self.rsi_filter    else None,
                        "ema_at_entry":    ema_vals[i]       if self.ema_filter    else None,
                        "bb_lower_entry":  bb_lower_vals[i]  if self.bb_filter     else None,
                        "macd_hist_entry": macd_hist_vals[i] if self.macd_filter   else None,
                        "vol_ratio_entry": (
                            candles[i].get("volume", 0.0) / vol_ma_vals[i]
                            if self.volume_filter and vol_ma_vals[i] > 0 else None
                        ),
                        "duration_min":    (ts - start_time) / 60_000,
                    })
                    break

                j += 1

            # ── Still open at end of period ───────────────────────────────────
            if j >= len(candles) and exit_price is None:
                last   = candles[-1]["close"]
                return trades, equity_curve, micro_trades, {
                    "trade":               trade_number,
                    "entry_time":          datetime.fromtimestamp(start_time / 1000),
                    "entry_price":         entry_price,
                    "avg_price":           avg,
                    "tp_price":            avg * (1 + self.tp_pct / 100),
                    "last_price":          last,
                    "unrealized_pnl":      (total_coins * last - total_spent) * self.leverage,
                    "fees_so_far":         fees_paid,
                    "safety_orders_filled": idx,
                    "total_spent":         total_spent,
                    "coins":               total_coins,
                }

            i = j
            trade_number += 1

        return trades, equity_curve, micro_trades, None


# ─────────────────────────────────────────────────────────────────────────────
# ANALYTICS HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def max_drawdown(equity: list) -> float:
    peak, mdd = equity[0], 0.0
    for v in equity:
        peak = max(peak, v)
        mdd  = max(mdd, (peak - v) / peak * 100)
    return mdd


def sharpe_ratio(roi_series: pd.Series, trades_per_year: float) -> float:
    if len(roi_series) < 2 or roi_series.std() == 0:
        return 0.0
    return (roi_series.mean() / roi_series.std()) * (trades_per_year ** 0.5)


def profit_factor(net_series: pd.Series) -> float:
    wins   = net_series[net_series > 0].sum()
    losses = net_series[net_series < 0].abs().sum()
    return wins / losses if losses > 0 else float("inf")


def max_consec_losses(net_series: pd.Series) -> int:
    best = cur = 0
    for v in net_series:
        cur  = cur + 1 if v < 0 else 0
        best = max(best, cur)
    return best


# ─────────────────────────────────────────────────────────────────────────────
# RUN
# ─────────────────────────────────────────────────────────────────────────────
if run_button and date_from < date_to:
    with st.spinner("Downloading candles…"):
        candles = download_history(trading_pair, date_from, date_to, interval)

    if not candles:
        st.error("No candles returned. Check pair or date range.")
        st.stop()

    period_days = max((date_to - date_from).days, 1)
    price_min   = min(c["low"]   for c in candles)
    price_max   = max(c["high"]  for c in candles)
    lev_label = f"{leverage:.1f}×" if leverage > 1 else "spot (1×)"
    st.caption(
        f"**{len(candles):,} {interval} candles** · "
        f"{datetime.fromtimestamp(candles[0]['time']/1000).strftime('%Y-%m-%d %H:%M')} → "
        f"{datetime.fromtimestamp(candles[-1]['time']/1000).strftime('%Y-%m-%d %H:%M')} · "
        f"Price range: €{price_min:,.4f} – €{price_max:,.4f} · Leverage: {lev_label}"
    )

    params = {
        "initial_balance":   initial_balance,
        "base_order":        base_order_value,
        "safety_order":      safety_order_value,
        "order_mode":        order_mode,
        "deviation_pct":     deviation_pct,
        "step_multiplier":   step_multiplier,
        "volume_scale":      volume_scale,
        "take_profit_pct":   take_profit_pct,
        "max_safety_orders": max_safety_orders,
        "compounding":       compounding,
        "fee_rate":          fee_rate,
        "rsi_filter":        rsi_filter,
        "rsi_period":        int(rsi_period),
        "rsi_threshold":     rsi_threshold,
        "sma_filter":        sma_filter,
        "sma_period":        int(sma_period),
        "ema_filter":        ema_filter,
        "ema_period":        int(ema_period),
        "volume_filter":     volume_filter,
        "volume_ma_period":  int(volume_ma_period),
        "volume_multiplier": volume_multiplier,
        "bb_filter":         bb_filter,
        "bb_period":         int(bb_period),
        "bb_std":            bb_std,
        "macd_filter":       macd_filter,
        "macd_fast":         int(macd_fast),
        "macd_slow":         int(macd_slow),
        "macd_signal":       int(macd_signal),
        "macd_mode":         macd_mode,
        "bos_filter":        bos_filter,
        "bos_lookback":      int(bos_lookback),
        "bos_recency":       int(bos_recency),
        "sr_filter":         sr_filter,
        "sr_lookback":       int(sr_lookback),
        "sr_proximity_pct":  sr_proximity_pct,
        "ma_cross_filter":   ma_cross_filter,
        "ma_cross_type":     ma_cross_type,
        "ma_fast_period":    int(ma_fast_period),
        "ma_slow_period":    int(ma_slow_period),
        "ma_cross_mode":     ma_cross_mode,
        "atr_dynamic":       atr_dynamic,
        "atr_period":        int(atr_period),
        "atr_tp_mult":       atr_tp_mult,
        "atr_sl_mult":       atr_sl_mult,
        "candle_pattern_filter":  candle_pattern_filter,
        "candle_patterns":        list(candle_patterns),
        "candle_vol_confirm":     candle_vol_confirm,
        "candle_confirm_candle":  candle_confirm_candle,
        "regime_filter":          regime_filter,
        "regime_mode":            regime_mode,
        "adx_period":             int(adx_period),
        "adx_threshold":          adx_threshold,
        "trend_direction_filter": trend_direction_filter,
        "trend_ema_period":       int(trend_ema_period),
        "trailing_tp":            trailing_tp,
        "trail_pct":         trail_pct,
        "stop_loss_enabled": stop_loss_enabled,
        "stop_loss_pct":     stop_loss_pct,
        "time_stop_enabled": time_stop_enabled,
        "time_stop_hours":   int(time_stop_hours),
        "leverage":          leverage,
        "candle_min":        INTERVAL_MINUTES[interval],
    }

    engine = DCASpotBacktester(params)
    trades, equity, micro_trades, open_trade = engine.run(candles)

    df       = pd.DataFrame(trades)
    df_micro = pd.DataFrame(micro_trades)

    if not df.empty:
        df["entry_time"] = pd.to_datetime(df["entry_time"])
        df["exit_time"]  = pd.to_datetime(df["exit_time"])

    # ── Buy & Hold benchmark ──────────────────────────────────────────────────
    bh_buy_fee    = initial_balance * fee_rate
    bh_coins      = (initial_balance - bh_buy_fee) / candles[0]["close"]
    bh_sell_value = bh_coins * candles[-1]["close"]
    bh_sell_fee   = bh_sell_value * fee_rate
    bh_final      = bh_sell_value - bh_sell_fee
    bh_roi        = (bh_final - initial_balance) / initial_balance * 100

    # ── Core metrics ──────────────────────────────────────────────────────────
    total_net     = df["net_profit_eur"].sum()  if not df.empty else 0.0
    total_fees    = df["fees_eur"].sum()        if not df.empty else 0.0
    win_rate      = (df["net_profit_eur"] > 0).mean() * 100 if not df.empty else 0.0
    roi_overall   = (equity[-1] - initial_balance) / initial_balance * 100
    dd            = max_drawdown(equity)
    ann_roi       = roi_overall / period_days * 365
    trades_per_mo = len(df) / (period_days / 30) if period_days > 0 else 0
    avg_dur       = df["duration_min"].mean() if not df.empty else 0.0
    pf            = profit_factor(df["net_profit_eur"]) if not df.empty else 0.0
    mcl           = max_consec_losses(df["net_profit_eur"]) if not df.empty else 0
    tpy           = trades_per_mo * 12
    sr            = sharpe_ratio(df["roi_pct"], tpy) if not df.empty else 0.0
    calmar        = ann_roi / dd if dd > 0 else 0.0
    avg_so        = df["safety_orders_filled"].mean() if not df.empty else 0.0
    n_liq         = (df["exit_reason"] == "Liquidated").sum() if not df.empty else 0

    # Row 1
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Closed Trades",    len(df))
    c2.metric("Win Rate",         f"{win_rate:.1f}%")
    c3.metric("Net Profit (EUR)", f"€{total_net:,.2f}")
    c4.metric("Total Fees (EUR)", f"€{total_fees:,.2f}")
    c5.metric("Final Balance",    f"€{equity[-1]:,.2f}", delta=f"{roi_overall:+.2f}%")
    c6.metric("Buy & Hold ROI",   f"{bh_roi:+.2f}%",
              delta=f"DCA {roi_overall - bh_roi:+.2f}% vs B&H",
              delta_color="normal")

    # Row 2
    c7, c8, c9, c10, c11, c12 = st.columns(6)
    c7.metric("Annualised ROI",   f"{ann_roi:.1f}%")
    c8.metric("Max Drawdown",     f"{dd:.2f}%",      delta_color="inverse")
    c9.metric("Sharpe Ratio",     f"{sr:.2f}",       help="Risk-adjusted return. >1 = good, >2 = excellent")
    c10.metric("Profit Factor",   f"{pf:.2f}" if pf != float('inf') else "∞",
               help="Gross wins / gross losses. >1.5 = good")
    c11.metric("Calmar Ratio",    f"{calmar:.2f}",   help="Ann. ROI / Max Drawdown. >1 = good")
    c12.metric("Max Consec. Loss",f"{mcl}",          delta_color="inverse")

    # Row 3
    c13, c14, c15, c16 = st.columns(4)
    c13.metric("Trades / Month",  f"{trades_per_mo:.1f}")
    c14.metric("Avg Duration",    f"{avg_dur * INTERVAL_MINUTES[interval]:.0f} min")
    c15.metric("Avg SOs Filled",  f"{avg_so:.1f}")
    c16.metric("Liquidations",    n_liq, delta_color="inverse",
               help="Number of trades closed by liquidation (leverage only)")

    # ── Open trade warning ────────────────────────────────────────────────────
    if open_trade:
        pnl = open_trade["unrealized_pnl"]
        st.warning(
            f"⚠️ **Trade #{open_trade['trade']} still open at end of period** — "
            f"TP not reached. Stats above include only {len(df)} closed trade(s). "
            f"Unrealised P&L: {'🟢' if pnl>=0 else '🔴'} €{pnl:,.2f}"
        )
        oc1,oc2,oc3,oc4,oc5,oc6 = st.columns(6)
        oc1.metric("Opened",       str(open_trade["entry_time"])[:16])
        oc2.metric("Entry Price",  f"€{open_trade['entry_price']:,.4f}")
        oc3.metric("Avg Price",    f"€{open_trade['avg_price']:,.4f}")
        oc4.metric("TP Price",     f"€{open_trade['tp_price']:,.4f}")
        oc5.metric("Last Price",   f"€{open_trade['last_price']:,.4f}")
        oc6.metric("SOs Filled",   open_trade["safety_orders_filled"])
        oc7, oc8 = st.columns(2)
        oc7.metric("Unrealised P&L", f"€{pnl:,.2f}",
                   delta=f"{pnl/open_trade['total_spent']*100:.2f}% on invested")
        oc8.metric("Gap to TP",
                   f"{(open_trade['tp_price']/open_trade['last_price']-1)*100:.2f}%")

    st.divider()

    # ── BTC Price vs Trade Entries/Exits ─────────────────────────────────────
    if not df.empty:
        st.subheader("📈 BTC Price vs Bot Trades")
        try:
            import plotly.graph_objects as _go
            from plotly.subplots import make_subplots as _make_subplots

            # Build price series from candles (sample down to max 2000 points for performance)
            _step   = max(len(candles) // 2000, 1)
            _times  = [datetime.fromtimestamp(c["time"] / 1000) for c in candles[::_step]]
            _prices = [c["close"] for c in candles[::_step]]

            # Cumulative P&L over time (aligned to exit times)
            _df_sorted  = df.sort_values("exit_time").copy()
            _df_sorted["cum_pnl"] = _df_sorted["net_profit_eur"].cumsum()

            _fig = _make_subplots(
                rows=2, cols=1,
                shared_xaxes=True,
                row_heights=[0.65, 0.35],
                vertical_spacing=0.05,
                subplot_titles=("Price + Trade Signals", "Cumulative P&L (EUR)"),
            )

            # ── Price line ────────────────────────────────────────────────────
            _fig.add_trace(_go.Scatter(
                x=_times, y=_prices,
                mode="lines",
                name="BTC Price",
                line=dict(color="#4a90d9", width=1),
                hovertemplate="%{x|%Y-%m-%d %H:%M}<br>€%{y:,.2f}<extra></extra>",
            ), row=1, col=1)

            # ── Entry markers (green triangles up) ───────────────────────────
            _entries_win  = df[df["net_profit_eur"] >= 0]
            _entries_loss = df[df["net_profit_eur"] <  0]

            _fig.add_trace(_go.Scatter(
                x=_entries_win["entry_time"],
                y=_entries_win["entry_price"],
                mode="markers",
                name="Entry (win)",
                marker=dict(symbol="triangle-up", color="#2ecc71", size=9),
                hovertemplate="Entry (win)<br>%{x|%Y-%m-%d %H:%M}<br>€%{y:,.4f}<extra></extra>",
            ), row=1, col=1)

            _fig.add_trace(_go.Scatter(
                x=_entries_loss["entry_time"],
                y=_entries_loss["entry_price"],
                mode="markers",
                name="Entry (loss)",
                marker=dict(symbol="triangle-up", color="#e67e22", size=9),
                hovertemplate="Entry (loss)<br>%{x|%Y-%m-%d %H:%M}<br>€%{y:,.4f}<extra></extra>",
            ), row=1, col=1)

            # ── Exit markers (red/green triangles down) ───────────────────────
            _exits_win  = df[df["net_profit_eur"] >= 0]
            _exits_loss = df[df["net_profit_eur"] <  0]

            _fig.add_trace(_go.Scatter(
                x=_exits_win["exit_time"],
                y=_exits_win["exit_price"],
                mode="markers",
                name="Exit (profit)",
                marker=dict(symbol="triangle-down", color="#27ae60", size=9),
                hovertemplate=(
                    "Exit ✅<br>%{x|%Y-%m-%d %H:%M}<br>€%{y:,.4f}"
                    "<br>Net: €%{customdata:,.2f}<extra></extra>"
                ),
                customdata=_exits_win["net_profit_eur"],
            ), row=1, col=1)

            _fig.add_trace(_go.Scatter(
                x=_exits_loss["exit_time"],
                y=_exits_loss["exit_price"],
                mode="markers",
                name="Exit (loss)",
                marker=dict(symbol="triangle-down", color="#e74c3c", size=9),
                hovertemplate=(
                    "Exit ❌<br>%{x|%Y-%m-%d %H:%M}<br>€%{y:,.4f}"
                    "<br>Net: €%{customdata:,.2f}<extra></extra>"
                ),
                customdata=_exits_loss["net_profit_eur"],
            ), row=1, col=1)

            # ── Cumulative P&L line ───────────────────────────────────────────
            _fig.add_trace(_go.Scatter(
                x=_df_sorted["exit_time"],
                y=_df_sorted["cum_pnl"],
                mode="lines",
                name="Cum. P&L",
                line=dict(color="#f39c12", width=2),
                fill="tozeroy",
                fillcolor="rgba(243,156,18,0.15)",
                hovertemplate="%{x|%Y-%m-%d %H:%M}<br>€%{y:,.2f}<extra></extra>",
            ), row=2, col=1)

            # Zero line on P&L chart
            _fig.add_hline(y=0, line_dash="dash", line_color="rgba(255,255,255,0.3)", row=2, col=1)

            _fig.update_layout(
                height=600,
                template="plotly_dark",
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
                margin=dict(l=0, r=0, t=40, b=0),
                hovermode="x unified",
            )
            _fig.update_yaxes(title_text="Price (EUR)", row=1, col=1)
            _fig.update_yaxes(title_text="P&L (EUR)",   row=2, col=1)
            _fig.update_xaxes(title_text="Date",        row=2, col=1)

            st.plotly_chart(_fig, use_container_width=True)

        except ImportError:
            st.info("Install plotly for the price chart: `pip install plotly`")

    st.divider()

    # ── Equity curve + Buy & Hold ─────────────────────────────────────────────
    st.subheader("📊 Equity Curve vs Buy & Hold")
    if len(equity) > 1 and len(candles) > 1:
        # Align buy-and-hold to equity curve length
        n    = len(equity)
        step = max(len(candles) // n, 1)
        bh_prices = [candles[min(k * step, len(candles)-1)]["close"] for k in range(n)]
        bh_curve  = [(initial_balance - bh_buy_fee) / candles[0]["close"] * p - bh_sell_fee / n
                     for p in bh_prices]
        chart_df = pd.DataFrame({
            "DCA Strategy (EUR)":  equity,
            "Buy & Hold (EUR)":    bh_curve,
        })
        st.line_chart(chart_df)

    # ── Monthly P&L breakdown ─────────────────────────────────────────────────
    if not df.empty:
        st.subheader("📅 Monthly P&L Breakdown")
        df["year"]  = df["exit_time"].dt.year
        df["month"] = df["exit_time"].dt.month
        pivot = df.pivot_table(
            values="net_profit_eur", index="year", columns="month", aggfunc="sum"
        )
        month_names = {1:"Jan",2:"Feb",3:"Mar",4:"Apr",5:"May",6:"Jun",
                       7:"Jul",8:"Aug",9:"Sep",10:"Oct",11:"Nov",12:"Dec"}
        pivot.columns = [month_names.get(m, m) for m in pivot.columns]
        pivot["Total"] = pivot.sum(axis=1)
        pivot.index.name = "Year"
        st.dataframe(
            pivot.style.format("€{:.2f}", na_rep="—"),
            use_container_width=True,
        )

    # ── Exit reason breakdown ─────────────────────────────────────────────────
    if not df.empty and df["exit_reason"].nunique() > 1:
        st.subheader("🚪 Exit Reason Breakdown")
        reason_df = df.groupby("exit_reason").agg(
            Count=("net_profit_eur", "count"),
            Net_EUR=("net_profit_eur", "sum"),
            Avg_ROI=("roi_pct", "mean"),
        ).rename(columns={"Net_EUR": "Net P&L (EUR)", "Avg_ROI": "Avg ROI (%)"})
        st.dataframe(reason_df.style.format({"Net P&L (EUR)": "€{:.2f}", "Avg ROI (%)": "{:.3f}%"}),
                     use_container_width=True)

    # ── Trade P&L distribution ────────────────────────────────────────────────
    if not df.empty and len(df) >= 3:
        st.subheader("📈 Trade P&L Distribution")
        pnl_series  = df["net_profit_eur"]
        bin_min, bin_max = pnl_series.min(), pnl_series.max()
        n_bins      = min(20, len(df))
        bin_width   = (bin_max - bin_min) / n_bins if bin_max > bin_min else 1
        bins        = [bin_min + i * bin_width for i in range(n_bins + 1)]
        labels      = [f"€{bins[i]:.2f}" for i in range(n_bins)]
        counts      = pd.cut(pnl_series, bins=bins, labels=labels, include_lowest=True).value_counts().sort_index()
        st.bar_chart(counts.rename("Trades"))

    # ── SO fill analysis ──────────────────────────────────────────────────────
    if not df.empty:
        st.subheader("🔒 Safety Order Fill Analysis")
        so_counts = df["safety_orders_filled"].value_counts().sort_index()
        so_df     = pd.DataFrame({
            "SOs Filled":    so_counts.index,
            "Trade Count":   so_counts.values,
            "% of Trades":   (so_counts.values / len(df) * 100).round(1),
        })
        col1, col2 = st.columns([1, 2])
        with col1:
            st.dataframe(so_df, use_container_width=True, hide_index=True)
        with col2:
            st.bar_chart(so_counts.rename("Trades"))

    # ── Best & worst trades ───────────────────────────────────────────────────
    if not df.empty:
        st.subheader("🏆 Top 5 Best & Worst Trades")
        show_cols = ["entry_time", "exit_time", "net_profit_eur", "fees_eur",
                     "duration_min", "safety_orders_filled", "exit_reason", "capital_deployed"]
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("### 🟢 Best")
            st.dataframe(df.sort_values("net_profit_eur", ascending=False).head(5)[show_cols],
                         use_container_width=True)
        with c2:
            st.markdown("### 🔴 Worst")
            st.dataframe(df.sort_values("net_profit_eur").head(5)[show_cols],
                         use_container_width=True)

    # ── Full trade table ──────────────────────────────────────────────────────
    if not df.empty:
        st.subheader("💰 All Trades")
        disp = df[[
            "trade","entry_time","exit_time","entry_price","avg_price","tp_price",
            "exit_price","exit_reason","gross_profit_eur","fees_eur","net_profit_eur",
            "safety_orders_filled","capital_deployed","duration_min","roi_pct",
        ]].rename(columns={
            "entry_price":"Entry €","avg_price":"Avg €","tp_price":"TP €",
            "exit_price":"Exit €","exit_reason":"Reason",
            "gross_profit_eur":"Gross (€)","fees_eur":"Fees (€)","net_profit_eur":"Net (€)",
            "safety_orders_filled":"SOs","capital_deployed":"Capital (€)",
            "duration_min":"Dur (min)","roi_pct":"ROI %",
        })
        st.dataframe(
            disp.style.format({
                "Entry €":"€{:.4f}","Avg €":"€{:.4f}","TP €":"€{:.4f}","Exit €":"€{:.4f}",
                "Gross (€)":"€{:.2f}","Fees (€)":"€{:.2f}","Net (€)":"€{:.2f}",
                "Capital (€)":"€{:.2f}","Dur (min)":"{:.0f}","ROI %":"{:.3f}%",
            }).map(lambda v: "color: #2ecc71" if isinstance(v, str) and v.startswith("€") and
                        float(v.replace("€","").replace(",","")) > 0 else
                        ("color: #e74c3c" if isinstance(v, str) and v.startswith("€") and
                         float(v.replace("€","").replace(",","")) < 0 else ""),
                        subset=["Net (€)"]),
            use_container_width=True,
        )

    # ── Order-level detail ────────────────────────────────────────────────────
    if not df_micro.empty:
        with st.expander("🔍 Order-level detail"):
            st.dataframe(df_micro, use_container_width=True)

elif not run_opt_btn:
    st.info("Configure your settings in the sidebar and press **🚀 Run Backtest** or **🔬 Run Optimizer**.")
    st.markdown("""
    ### Strategy guide

    | Feature | What it does | When to use |
    |---|---|---|
    | **RSI Filter** | Only enter when RSI < threshold | Avoid buying at the top of a rally |
    | **SMA Filter** | Only enter when price < SMA | Only trade in downtrend / below average |
    | **EMA Filter** | Only enter when price < EMA | Faster trend filter than SMA |
    | **Volume Filter** | Only enter when volume > N× avg | Confirm candle activity |
    | **Bollinger Bands** | Only enter when price < lower band | Oversold entries |
    | **MACD Filter** | Only enter on MACD signal | Momentum confirmation |
    | **BOS Filter** | Only enter after a bullish Break of Structure | Structure-based entry confirmation |
    | **S&R Filter** | Only enter when price is near rolling support | Mean-reversion near key levels |
    | **MA Cross Filter** | Only enter in Golden Cross regime or on fresh crossover | Trend-following entries |
    | **ATR Dynamic TP/SL** | TP and SL set as N × ATR from avg entry | Volatility-adaptive exits |
    | **Trailing TP** | Follows price up, sells on pullback | Capture larger moves in trending markets |
    | **Stop Loss** | Cuts loss if avg drops by X% | Limit damage when price keeps falling |
    | **Time Stop** | Close after N hours regardless | Free capital from stagnant trades |

    ### Optimizer
    Open **🔬 Optimizer** in the sidebar to sweep combinations of TP%, Deviation%, Safety Orders,
    Volume Scale, Step Multiplier and RSI Threshold — ranked by Net Profit, Win Rate, Sharpe, or other metrics.

    **Recommended starting point:** Base 5%, SO 5%, Deviation 1.5%, Volume Scale 1.05, TP 1.5%, Max SOs 6, RSI Filter ON (threshold 55)
    """)

# ─────────────────────────────────────────────────────────────────────────────
# AUTO-OPTIMIZER (Optuna TPE)
# ─────────────────────────────────────────────────────────────────────────────
if run_opt_btn and date_from < date_to:
    import optuna
    optuna.logging.set_verbosity(optuna.logging.WARNING)

    opt_pairs_list = opt_pairs if opt_pairs else [trading_pair]
    opt_itvls_list = opt_intervals if opt_intervals else [interval]
    combos         = [(p, iv) for p in opt_pairs_list for iv in opt_itvls_list]

    # ── Pre-download all (pair × timeframe) combinations ─────────────────────
    st.subheader(f"🤖 Auto-Optimizer — {int(opt_trials)} trials · "
                 f"{len(opt_pairs_list)} pair(s) × {len(opt_itvls_list)} timeframe(s)")

    dl_prog      = st.progress(0.0, text="Pre-downloading candle data…")
    candle_store = {}   # (pair, iv) → {train, val, train_days, val_days, itvl_min}

    for di, (pair, iv) in enumerate(combos):
        data = download_history(pair, date_from, date_to, iv)
        if data:
            itvl_m = INTERVAL_MINUTES[iv]
            split  = max(int(len(data) * opt_wf_split / 100), 1)
            candle_store[(pair, iv)] = {
                "train":      data[:split],
                "val":        data[split:],
                "train_days": split * itvl_m / 1440,
                "val_days":   (len(data) - split) * itvl_m / 1440,
                "itvl_min":   itvl_m,
            }
        dl_prog.progress((di + 1) / len(combos),
                         text=f"Downloaded {pair} {iv} ({di+1}/{len(combos)})")

    dl_prog.empty()

    if not candle_store:
        st.error("No candle data available for any selected pair/timeframe.")
        st.stop()

    valid_combos = list(candle_store.keys())
    st.info(
        f"Ready: **{len(valid_combos)}** (pair × timeframe) combination(s). "
        f"Training split: {opt_wf_split}% · Validation: {100 - opt_wf_split}%"
    )

    # ── Helpers ───────────────────────────────────────────────────────────────
    METRIC_KEY = {
        "Net Profit (EUR)":          "net",
        "Win Rate (%)":              "wr",
        "Sharpe Ratio":              "sharpe",
        "Profit Factor":             "pf",
        "Calmar Ratio":              "calmar",
        "Composite (Profit × Sharpe)": "composite",
    }
    opt_key  = METRIC_KEY[opt_metric]

    def _build_params(tp, dev, so, vs, step_mul, rsi_thr, base_o, so_size, itvl_m,
                      bos_rec=None, sr_lb=None, sr_prox=None, ma_fast=None, ma_slow=None,
                      atr_tp=None, atr_sl=None, sl_pct=None,
                      adx_thr=None):
        return {
            "initial_balance":   initial_balance,
            "base_order":        base_o,
            "safety_order":      so_size,
            "order_mode":        order_mode,
            "deviation_pct":     dev,
            "step_multiplier":   step_mul,
            "volume_scale":      vs,
            "take_profit_pct":   tp,
            "max_safety_orders": int(so),
            "compounding":       compounding,
            "fee_rate":          fee_rate,
            "rsi_filter":        rsi_filter,
            "rsi_period":        int(rsi_period),
            "rsi_threshold":     rsi_thr,
            "sma_filter":        sma_filter,
            "sma_period":        int(sma_period),
            "ema_filter":        ema_filter,
            "ema_period":        int(ema_period),
            "volume_filter":     volume_filter,
            "volume_ma_period":  int(volume_ma_period),
            "volume_multiplier": volume_multiplier,
            "bb_filter":         bb_filter,
            "bb_period":         int(bb_period),
            "bb_std":            bb_std,
            "macd_filter":       macd_filter,
            "macd_fast":         int(macd_fast),
            "macd_slow":         int(macd_slow),
            "macd_signal":       int(macd_signal),
            "macd_mode":         macd_mode,
            "bos_filter":        bos_filter,
            "bos_lookback":      int(bos_lookback),
            "bos_recency":       int(bos_rec) if bos_rec is not None else int(bos_recency),
            "sr_filter":         sr_filter,
            "sr_lookback":       int(sr_lb) if sr_lb is not None else int(sr_lookback),
            "sr_proximity_pct":  float(sr_prox) if sr_prox is not None else sr_proximity_pct,
            "ma_cross_filter":   ma_cross_filter,
            "ma_cross_type":     ma_cross_type,
            "ma_fast_period":    int(ma_fast) if ma_fast is not None else int(ma_fast_period),
            "ma_slow_period":    int(ma_slow) if ma_slow is not None else int(ma_slow_period),
            "ma_cross_mode":     ma_cross_mode,
            "atr_dynamic":       atr_dynamic,
            "atr_period":        int(atr_period),
            "atr_tp_mult":       float(atr_tp) if atr_tp is not None else atr_tp_mult,
            "atr_sl_mult":       float(atr_sl) if atr_sl is not None else atr_sl_mult,
            "candle_pattern_filter":  candle_pattern_filter,
            "candle_patterns":        list(candle_patterns),
            "candle_vol_confirm":     candle_vol_confirm,
            "candle_confirm_candle":  candle_confirm_candle,
            "regime_filter":          regime_filter,
            "regime_mode":            regime_mode,
            "adx_period":             int(adx_period),
            "adx_threshold":          float(adx_thr) if adx_thr is not None else adx_threshold,
            "trend_direction_filter": trend_direction_filter,
            "trend_ema_period":       int(trend_ema_period),
            "trailing_tp":       trailing_tp,
            "trail_pct":         trail_pct,
            "stop_loss_enabled": stop_loss_enabled,
            "stop_loss_pct":     float(sl_pct) if sl_pct is not None else stop_loss_pct,
            "time_stop_enabled": time_stop_enabled,
            "time_stop_hours":   int(time_stop_hours),
            "leverage":          leverage,
            "candle_min":        itvl_m,
        }

    def _score(p, candles_list, days):
        eng  = DCASpotBacktester(p)
        t, eq, _, _ = eng.run(candles_list)
        df_r = pd.DataFrame(t)
        if df_r.empty or len(eq) < 2:
            return None
        net_v = df_r["net_profit_eur"].sum()
        wr_v  = (df_r["net_profit_eur"] > 0).mean() * 100
        roi_v = (eq[-1] - initial_balance) / initial_balance * 100
        dd_v  = max_drawdown(eq)
        ann_v = roi_v / max(days, 1) * 365
        n_tr  = len(df_r)
        sr_v  = sharpe_ratio(df_r["roi_pct"], n_tr / max(days / 365, 0.01))
        pf_v  = profit_factor(df_r["net_profit_eur"])
        cal_v = ann_v / dd_v if dd_v > 0 else 0.0
        # Minimum trade count penalty — scale score down for low trade counts
        # penalty = min(1, trades / min_trades) so 0 trades = 0, min_trades+ = 1
        trade_penalty = min(1.0, n_tr / max(int(opt_min_trades), 1))
        # Composite: product of normalised net profit and Sharpe Ratio × penalty
        norm_net  = net_v / max(initial_balance, 1.0)
        comp_v    = norm_net * sr_v * trade_penalty
        return {"net": net_v * trade_penalty, "wr": wr_v, "roi": roi_v, "dd": dd_v,
                "ann_roi": ann_v, "sharpe": sr_v,
                "pf": min(pf_v, 999.0), "calmar": cal_v, "trades": n_tr,
                "composite": comp_v}

    # ── Optuna study ──────────────────────────────────────────────────────────
    import pathlib as _pathlib
    _study_name = opt_study_name.strip() or "dca_study"
    _db_file    = _pathlib.Path(__file__).parent / f"{_study_name}.db"
    _storage    = f"sqlite:///{_db_file}"

    prog_bar   = st.progress(0.0, text="Starting Optuna trials…")
    live_best  = st.empty()
    trial_rows = []

    def objective(trial):
        # Categorical: pair and timeframe
        pair_t = trial.suggest_categorical("pair",     [f"{p}|{iv}" for p, iv in valid_combos])
        sel_pair, sel_iv = pair_t.split("|", 1)
        store  = candle_store[(sel_pair, sel_iv)]

        # Order sizing
        _base_lo = max(float(opt_base_min),    0.1)
        _base_hi = max(float(opt_base_max),    _base_lo + 0.1)
        _so_lo   = max(float(opt_so_size_min), 0.1)
        _so_hi   = max(float(opt_so_size_max), _so_lo   + 0.1)
        base_o   = trial.suggest_float("base_order",   _base_lo, _base_hi)
        so_size  = trial.suggest_float("safety_order", _so_lo,   _so_hi)

        # DCA parameters
        tp      = trial.suggest_float("tp_pct",    max(opt_tp_min,   0.1), max(opt_tp_max,   opt_tp_min   + 0.1))
        dev     = trial.suggest_float("dev_pct",   max(opt_dev_min,  0.1), max(opt_dev_max,  opt_dev_min  + 0.1))
        so      = trial.suggest_int(  "max_so",    int(opt_so_min),        max(int(opt_so_max), int(opt_so_min) + 1))
        vs      = trial.suggest_float("vol_scale", max(opt_vs_min,   1.0), max(opt_vs_max,   opt_vs_min   + 0.01))
        step_m  = trial.suggest_float("step_mul",  max(opt_step_min, 0.1), max(opt_step_max, opt_step_min + 0.1))
        rsi_thr = (trial.suggest_float("rsi_thr",  opt_rsi_min, max(opt_rsi_max, opt_rsi_min + 1.0))
                   if rsi_filter else rsi_threshold)

        # Sweep new advanced TA params if the corresponding filter is enabled
        bos_rec  = (trial.suggest_int(  "bos_recency",    1, 15)
                    if bos_filter else bos_recency)
        sr_lb_v  = (trial.suggest_int(  "sr_lookback",    max(int(opt_sr_lb_min), 5),
                                                           max(int(opt_sr_lb_max), int(opt_sr_lb_min) + 5))
                    if sr_filter else sr_lookback)
        sr_prox  = (trial.suggest_float("sr_proximity",   max(opt_sr_prox_min, 0.1),
                                                           max(opt_sr_prox_max, opt_sr_prox_min + 0.1))
                    if sr_filter else sr_proximity_pct)
        sl_pct_v = (trial.suggest_float("sl_pct",         max(opt_sl_min, 0.1),
                                                           max(opt_sl_max, opt_sl_min + 0.1))
                    if stop_loss_enabled else stop_loss_pct)
        ma_fast  = (trial.suggest_int(  "ma_fast_period", 5,  100)
                    if ma_cross_filter else ma_fast_period)
        ma_slow  = (trial.suggest_int(  "ma_slow_period", max(int(ma_fast) + 10, 50), 300)
                    if ma_cross_filter else ma_slow_period)
        atr_tp_v = (trial.suggest_float("atr_tp_mult",    1.0, 5.0)
                    if atr_dynamic else atr_tp_mult)
        atr_sl_v = (trial.suggest_float("atr_sl_mult",    0.5, 3.0)
                    if atr_dynamic else atr_sl_mult)
        adx_thr_v = (trial.suggest_float("adx_threshold", 10.0, 50.0)
                     if regime_filter and regime_mode != "Any" else adx_threshold)

        p = _build_params(tp, dev, so, vs, step_m, rsi_thr, base_o, so_size, store["itvl_min"],
                          bos_rec=bos_rec, sr_lb=sr_lb_v, sr_prox=sr_prox, sl_pct=sl_pct_v,
                          ma_fast=ma_fast, ma_slow=ma_slow,
                          atr_tp=atr_tp_v, atr_sl=atr_sl_v, adx_thr=adx_thr_v)
        s = _score(p, store["train"], store["train_days"])
        if s is None:
            return float("-inf")

        _sz_lbl = f"{base_o:.1f}{_sz_unit}"
        trial_rows.append({
            "Trial":            trial.number + 1,
            "Pair":             sel_pair,
            "Timeframe":        sel_iv,
            f"Base ({_sz_unit})":   round(base_o,  2),
            f"Safety ({_sz_unit})": round(so_size, 2),
            "TP %":             round(tp,      3),
            "Dev %":            round(dev,     3),
            "Max SOs":          so,
            "Vol Scale":        round(vs,      3),
            "Step Mul":         round(step_m,  3),
            "RSI Thr":          round(rsi_thr, 1) if rsi_filter else "—",
            "BOS Recency":      bos_rec if bos_filter else "—",
            "SR Lookback":      sr_lb_v if sr_filter else "—",
            "SR Proximity %":   round(sr_prox, 2) if sr_filter else "—",
            "SL %":             round(sl_pct_v, 2) if stop_loss_enabled else "—",
            "MA Fast":          ma_fast if ma_cross_filter else "—",
            "MA Slow":          ma_slow if ma_cross_filter else "—",
            "ATR TP Mult":      round(atr_tp_v, 2) if atr_dynamic else "—",
            "ATR SL Mult":      round(atr_sl_v, 2) if atr_dynamic else "—",
            "ADX Threshold":    round(adx_thr_v, 1) if (regime_filter and regime_mode != "Any") else "—",
            "Trades":           s["trades"],
            "Win Rate (%)":     round(s["wr"],     1),
            "Net Profit (EUR)": round(s["net"],    2),
            "ROI (%)":          round(s["roi"],    2),
            "Max DD (%)":       round(s["dd"],     2),
            "Sharpe":           round(s["sharpe"], 3),
            "Profit Factor":    round(s["pf"],     3),
            "Calmar":           round(s["calmar"], 3),
            "Composite":        round(s["composite"], 6),
            opt_metric:         round(s[opt_key],  4),
        })
        return s[opt_key]

    def _on_trial(study, trial):
        n = trial.number + 1
        prog_bar.progress(min(n / int(opt_trials), 1.0), text=f"Trial {n}/{int(opt_trials)}…")
        try:
            bv = study.best_value
            bt = study.best_trial
            pair_str = bt.params.get("pair", "").split("|")
            pair_lbl = f"{pair_str[0]} {pair_str[1]}" if len(pair_str) == 2 else ""
            live_best.metric(
                f"Best {opt_metric} so far — trial {bt.number + 1} · {pair_lbl}",
                f"€{bv:,.2f}" if opt_key == "net" else f"{bv:.4f}",
            )
        except Exception:
            pass

    study = optuna.create_study(
        study_name=_study_name,
        direction="maximize",
        sampler=optuna.samplers.TPESampler(seed=42),
        storage=_storage,
        load_if_exists=True,
    )

    # Populate trial_rows with any previously completed trials so the results
    # table and CSV export include the full history across sessions.
    _prev_trials_count = len([t for t in study.trials
                               if t.state == optuna.trial.TrialState.COMPLETE])
    if _prev_trials_count:
        st.info(
            f"📂 Resuming study **{_study_name}** — "
            f"{_prev_trials_count} existing trial(s) loaded from disk."
        )

    study.optimize(objective, n_trials=int(opt_trials), callbacks=[_on_trial],
                   show_progress_bar=False)

    prog_bar.empty()
    live_best.empty()

    # Merge any prior completed trials that weren't re-run this session
    # (their user_attrs won't be in trial_rows, so we reconstruct from study)
    _new_trial_nums = {r["Trial"] for r in trial_rows}
    for _t in study.trials:
        if (_t.state == optuna.trial.TrialState.COMPLETE and
                (_t.number + 1) not in _new_trial_nums):
            # Minimal row from stored params — score comes from stored value
            _pr = _t.params
            trial_rows.append({
                "Trial":            _t.number + 1,
                "Pair":             _pr.get("pair", "").split("|")[0],
                "Timeframe":        _pr.get("pair", "|").split("|")[1] if "|" in _pr.get("pair", "") else "—",
                f"Base ({_sz_unit})":   round(_pr.get("base_order",   0), 2),
                f"Safety ({_sz_unit})": round(_pr.get("safety_order", 0), 2),
                "TP %":             round(_pr.get("tp_pct",      0), 3),
                "Dev %":            round(_pr.get("dev_pct",     0), 3),
                "Max SOs":          _pr.get("max_so", 0),
                "Vol Scale":        round(_pr.get("vol_scale",   1.0), 3),
                "Step Mul":         round(_pr.get("step_mul",    1.0), 3),
                "RSI Thr":          round(_pr.get("rsi_thr",     0), 1) if rsi_filter else "—",
                "BOS Recency":      _pr.get("bos_recency",  "—") if bos_filter else "—",
                "SR Lookback":      _pr.get("sr_lookback",  "—") if sr_filter else "—",
                "SR Proximity %":   round(_pr.get("sr_proximity", 0), 2) if sr_filter else "—",
                "SL %":             round(_pr.get("sl_pct",       0), 2) if stop_loss_enabled else "—",
                "MA Fast":          _pr.get("ma_fast_period", "—") if ma_cross_filter else "—",
                "MA Slow":          _pr.get("ma_slow_period", "—") if ma_cross_filter else "—",
                "ATR TP Mult":      round(_pr.get("atr_tp_mult",  0), 2) if atr_dynamic else "—",
                "ATR SL Mult":      round(_pr.get("atr_sl_mult",  0), 2) if atr_dynamic else "—",
                "ADX Threshold":    round(_pr.get("adx_threshold", 0), 1) if (regime_filter and regime_mode != "Any") else "—",
                "Trades":           "–",
                "Win Rate (%)":     "–",
                "Net Profit (EUR)": "–",
                "ROI (%)":          "–",
                "Max DD (%)":       "–",
                "Sharpe":           "–",
                "Profit Factor":    "–",
                "Calmar":           "–",
                opt_metric:         round(_t.value, 4) if _t.value is not None else None,
            })

    if not trial_rows:
        st.warning("No valid trials produced trades. Check date range or relax filters.")
        st.stop()

    # ── Results dataframe ─────────────────────────────────────────────────────
    df_trials = pd.DataFrame(trial_rows)
    df_trials[opt_metric] = pd.to_numeric(df_trials[opt_metric], errors="coerce")
    df_trials = (
        df_trials
        .sort_values(opt_metric, ascending=False)
        .reset_index(drop=True)
    )
    df_trials.index += 1

    # ── Auto-save results CSV ─────────────────────────────────────────────────
    import datetime as _dt2
    _csv_fname  = f"{_study_name}_results_{_dt2.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    _csv_path   = _pathlib.Path(__file__).parent / _csv_fname
    df_trials.to_csv(_csv_path, index=False)
    st.caption(f"💾 Results auto-saved to `{_csv_path.name}`")

    # ── Best params ───────────────────────────────────────────────────────────
    bp         = study.best_params
    best_pair_iv = bp["pair"].split("|", 1)
    best_pair  = best_pair_iv[0]
    best_iv    = best_pair_iv[1] if len(best_pair_iv) == 2 else interval
    best_base  = bp["base_order"]
    best_so_sz = bp["safety_order"]
    best_tp    = bp["tp_pct"]
    best_dev   = bp["dev_pct"]
    best_so    = bp["max_so"]
    best_vs    = bp["vol_scale"]
    best_step  = bp["step_mul"]
    best_rsi   = bp.get("rsi_thr", rsi_threshold)
    best_store = candle_store.get((best_pair, best_iv), list(candle_store.values())[0])

    st.success(
        f"✅ {len(trial_rows)} trials completed · best **{opt_metric}**: "
        f"{'€' if opt_key == 'net' else ''}{study.best_value:,.4f} "
        f"on **{best_pair}** · **{best_iv}**"
    )

    # Best params banner — row 1: asset & sizing
    bb1, bb2, bb3, bb4 = st.columns(4)
    bb1.metric("Best Pair",       best_pair)
    bb2.metric("Best Timeframe",  best_iv)
    bb3.metric(f"Base Order ({_sz_unit})",   f"{best_base:.2f}")
    bb4.metric(f"Safety Order ({_sz_unit})", f"{best_so_sz:.2f}")

    # Row 2: DCA params
    bp1, bp2, bp3, bp4, bp5, bp6 = st.columns(6)
    bp1.metric("TP %",         f"{best_tp:.2f}%")
    bp2.metric("Deviation %",  f"{best_dev:.2f}%")
    bp3.metric("Max SOs",      best_so)
    bp4.metric("Vol Scale",    f"{best_vs:.3f}")
    bp5.metric("Step Mul",     f"{best_step:.2f}")
    bp6.metric("RSI Threshold", f"{best_rsi:.0f}" if rsi_filter else "—")

    # ── Walk-forward validation ───────────────────────────────────────────────
    st.subheader("🔀 Walk-Forward Validation")
    st.caption(
        f"Best combo: **{best_pair} {best_iv}** · "
        f"Training: first {opt_wf_split}% (~{best_store['train_days']:.0f} days) · "
        f"Validation: remaining {100 - opt_wf_split}% (~{best_store['val_days']:.0f} days)"
    )
    best_train   = df_trials.iloc[0]
    best_bos_rec  = bp.get("bos_recency",    bos_recency)
    best_sr_lb    = bp.get("sr_lookback",    sr_lookback)
    best_sr_prox  = bp.get("sr_proximity",   sr_proximity_pct)
    best_sl_pct   = bp.get("sl_pct",         stop_loss_pct)
    best_ma_fast  = bp.get("ma_fast_period", ma_fast_period)
    best_ma_slow  = bp.get("ma_slow_period", ma_slow_period)
    best_atr_tp   = bp.get("atr_tp_mult",    atr_tp_mult)
    best_atr_sl   = bp.get("atr_sl_mult",    atr_sl_mult)
    best_adx_thr  = bp.get("adx_threshold",  adx_threshold)
    val_p         = _build_params(best_tp, best_dev, best_so, best_vs, best_step,
                                  best_rsi, best_base, best_so_sz, best_store["itvl_min"],
                                  bos_rec=best_bos_rec, sr_lb=best_sr_lb, sr_prox=best_sr_prox,
                                  sl_pct=best_sl_pct, ma_fast=best_ma_fast, ma_slow=best_ma_slow,
                                  atr_tp=best_atr_tp, atr_sl=best_atr_sl, adx_thr=best_adx_thr)
    val_score  = _score(val_p, best_store["val"], best_store["val_days"]) if best_store["val"] else None

    wf1, wf2, wf3, wf4 = st.columns(4)
    def _to_float(v):
        try:
            return float(v)
        except (ValueError, TypeError):
            return 0.0

    _bt_net = _to_float(best_train['Net Profit (EUR)'])
    _bt_wr  = _to_float(best_train['Win Rate (%)'])
    _bt_sh  = _to_float(best_train['Sharpe'])
    _bt_dd  = _to_float(best_train['Max DD (%)'])

    wf1.metric("Train — Net Profit", f"€{_bt_net:,.2f}")
    wf2.metric("Val — Net Profit",
               f"€{val_score['net']:,.2f}" if val_score else "N/A",
               delta=f"{val_score['net'] - _bt_net:+.2f}" if val_score else None)
    wf3.metric("Train — Win Rate",   f"{_bt_wr:.1f}%")
    wf4.metric("Val — Win Rate",
               f"{val_score['wr']:.1f}%" if val_score else "N/A",
               delta=f"{val_score['wr'] - _bt_wr:+.1f}%" if val_score else None)

    wf5, wf6, wf7, wf8 = st.columns(4)
    wf5.metric("Train — Sharpe",  f"{_bt_sh:.3f}")
    wf6.metric("Val — Sharpe",
               f"{val_score['sharpe']:.3f}" if val_score else "N/A",
               delta=f"{val_score['sharpe'] - _bt_sh:+.3f}" if val_score else None)
    wf7.metric("Train — Max DD",  f"{_bt_dd:.2f}%")
    wf8.metric("Val — Max DD",
               f"{val_score['dd']:.2f}%" if val_score else "N/A",
               delta=f"{val_score['dd'] - _bt_dd:+.2f}%" if val_score else None,
               delta_color="inverse")

    if val_score:
        if val_score["net"] < 0 and _bt_net > 0:
            st.warning(
                "⚠️ Profitable on training data but loses on validation — possible overfitting. "
                "Try more trials, a longer date range, or fewer filters."
            )
        elif val_score["net"] > 0:
            st.success("✅ Strategy remains profitable on unseen validation data.")

    # ── Optimization history ──────────────────────────────────────────────────
    st.subheader("📈 Optimization History")
    try:
        import plotly.graph_objects as _go  # noqa: F401
        st.plotly_chart(optuna.visualization.plot_optimization_history(study),
                        use_container_width=True)
    except Exception:
        st.line_chart(df_trials[["Trial", opt_metric]].sort_values("Trial").set_index("Trial"))

    # ── Parameter importance ──────────────────────────────────────────────────
    st.subheader("🎯 Parameter Importance")
    try:
        importance = optuna.importance.get_param_importances(study)
        _labels    = {
            "pair": "Pair×TF", "base_order": f"Base ({_sz_unit})",
            "safety_order": f"Safety ({_sz_unit})", "tp_pct": "TP %",
            "dev_pct": "Dev %", "max_so": "Max SOs", "vol_scale": "Vol Scale",
            "step_mul": "Step Mul", "rsi_thr": "RSI Thr",
            "bos_recency": "BOS Recency", "sr_lookback": "SR Lookback",
            "sr_proximity": "SR Proximity %", "sl_pct": "SL %",
            "ma_fast_period": "MA Fast", "ma_slow_period": "MA Slow",
            "atr_tp_mult": "ATR TP Mult", "atr_sl_mult": "ATR SL Mult",
            "adx_threshold": "ADX Threshold",
        }
        imp_df = pd.DataFrame({
            "Parameter":  [_labels.get(k, k) for k in importance],
            "Importance": list(importance.values()),
        }).sort_values("Importance", ascending=False)
        st.bar_chart(imp_df.set_index("Parameter"))
        st.caption("Higher = more influence on the target metric.")
    except Exception:
        st.info("Need ≥ 20 completed trials to compute parameter importance.")

    # ── Contour: TP% vs Deviation% ────────────────────────────────────────────
    st.subheader("🗺️ Parameter Contour (TP% vs Deviation%)")
    try:
        st.plotly_chart(
            optuna.visualization.plot_contour(study, params=["tp_pct", "dev_pct"]),
            use_container_width=True,
        )
    except Exception:
        pass

    # ── Results by pair & timeframe ───────────────────────────────────────────
    if len(valid_combos) > 1:
        st.subheader("📊 Average Score by Pair & Timeframe")
        _by_combo = (
            df_trials.groupby(["Pair", "Timeframe"])[opt_metric]
            .mean().reset_index()
            .sort_values(opt_metric, ascending=False)
        )
        _by_combo["Pair × TF"] = _by_combo["Pair"] + " · " + _by_combo["Timeframe"]
        st.bar_chart(_by_combo.set_index("Pair × TF")[[opt_metric]])

    # ── All trials table ──────────────────────────────────────────────────────
    st.subheader("🏅 All Trials (ranked)")
    st.dataframe(df_trials, use_container_width=True)
    _dl_cols, _ = st.columns([1, 3])
    _dl_cols.download_button(
        "⬇️ Download results CSV",
        data=df_trials.to_csv(index=False).encode(),
        file_name=_csv_fname,
        mime="text/csv",
        use_container_width=True,
        key="dl_trials_csv",
    )

    st.info(
        "💡 **Apply best settings:** Use the values from the banners above — "
        f"set the sidebar to **{best_pair}**, timeframe **{best_iv}**, "
        f"Base {best_base:.1f}{_sz_unit}, Safety {best_so_sz:.1f}{_sz_unit}, "
        f"TP {best_tp:.2f}%, Dev {best_dev:.2f}%, Max SOs {best_so}, "
        f"Vol Scale {best_vs:.3f}, Step Mul {best_step:.2f}"
        + (f", RSI Thr {best_rsi:.0f}" if rsi_filter else "")
        + (f", SL% {best_sl_pct:.1f}" if stop_loss_enabled else "")
        + (f", BOS Recency {best_bos_rec}" if bos_filter else "")
        + (f", SR Lookback {best_sr_lb} / Proximity {best_sr_prox:.1f}%" if sr_filter else "")
        + (f", MA Fast {best_ma_fast} / Slow {best_ma_slow}" if ma_cross_filter else "")
        + (f", ATR TP×{best_atr_tp:.1f} SL×{best_atr_sl:.1f}" if atr_dynamic else "")
        + " — then click 🚀 Run Backtest."
    )
