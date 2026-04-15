"""
dashboard_WS.py — Live DCA Paper Trading Dashboard
────────────────────────────────────────────────────
Same DCA logic and settings as dashboard.py, but driven by
live Bybit WebSocket data instead of historical candles.

Paper trading only — no real orders are placed on any exchange.

Architecture
─────────────
  Background thread  →  Bybit WebSocket (kline.1 + tickers)
                      →  candle_q (closed 1m candles)
  Streamlit loop     ←  drains candle_q every second
                      →  DCALiveEngine.on_candle()
                      →  re-renders UI
"""

import json
import queue
import threading
import time
from datetime import datetime

import pandas as pd
import streamlit as st

try:
    import websocket
except ImportError:
    st.error("Missing dependency — run:  pip install websocket-client")
    st.stop()


# ─────────────────────────────────────────────────────────────────────────────
# Shared state via st.cache_resource — truly survives all Streamlit reruns
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_resource
def _get_state() -> dict:
    return {
        "lock":               threading.Lock(),
        "candle_q":           queue.Queue(),
        "ticker":             {"price": 0.0, "ts": "—"},
        "ws_info":            {"connected": False, "ws": None, "thread": None, "error": ""},
        # Persisted across browser reconnects:
        "engine":             None,
        "running":            False,
        "candles_processed":  0,
    }


# ── WebSocket callbacks ───────────────────────────────────────────────────────
def _on_message(ws, raw):
    state = _get_state()
    msg   = json.loads(raw)

    # Bybit sends JSON-level pings — respond with pong
    if msg.get("op") == "ping":
        ws.send(json.dumps({"op": "pong"}))
        return

    topic = msg.get("topic", "")

    if topic.startswith("kline."):
        data = msg["data"][0]
        with state["lock"]:
            state["ticker"]["price"] = float(data["close"])
            state["ticker"]["ts"]    = datetime.fromtimestamp(msg["ts"] / 1000).strftime("%H:%M:%S UTC")
        if data.get("confirm"):  # candle closed → queue for engine
            state["candle_q"].put({
                "time":  data["start"],
                "open":  float(data["open"]),
                "high":  float(data["high"]),
                "low":   float(data["low"]),
                "close": float(data["close"]),
            })

    elif topic.startswith("tickers."):
        last = msg["data"].get("lastPrice")
        if last:
            with state["lock"]:
                state["ticker"]["price"] = float(last)
                state["ticker"]["ts"]    = datetime.fromtimestamp(msg["ts"] / 1000).strftime("%H:%M:%S UTC")


def _on_close(ws, code, msg):
    state = _get_state()
    with state["lock"]:
        state["ws_info"]["connected"] = False
        if code or msg:
            state["ws_info"]["error"] = f"Connection closed — code={code}, reason={msg}"


def _on_error(ws, err):
    state = _get_state()
    with state["lock"]:
        state["ws_info"]["connected"] = False
        state["ws_info"]["error"]     = f"{type(err).__name__}: {err}" if err else "Unknown error"


def _ws_thread_fn(symbol: str, is_inverse: bool):
    state = _get_state()
    url   = ("wss://stream.bybit.com/v5/public/inverse" if is_inverse
             else "wss://stream.bybit.com/v5/public/linear")

    def on_open(ws):
        with state["lock"]:
            state["ws_info"]["connected"] = True
            state["ws_info"]["error"]     = ""
        ws.send(json.dumps({
            "op":   "subscribe",
            "args": [f"kline.1.{symbol}", f"tickers.{symbol}"],
        }))

    ws = websocket.WebSocketApp(
        url,
        on_message=_on_message,
        on_open=on_open,
        on_close=_on_close,
        on_error=_on_error,
    )
    with state["lock"]:
        state["ws_info"]["ws"] = ws
    ws.run_forever()


def ws_start(symbol: str, is_inverse: bool):
    state = _get_state()
    with state["lock"]:
        t = state["ws_info"].get("thread")
        if t and t.is_alive():
            return  # already running
    thread = threading.Thread(target=_ws_thread_fn, args=(symbol, is_inverse), daemon=True)
    with state["lock"]:
        state["ws_info"]["thread"] = thread
    thread.start()


def ws_stop():
    state = _get_state()
    with state["lock"]:
        ws = state["ws_info"].get("ws")
        if ws:
            ws.close()
        state["ws_info"].update({"connected": False, "ws": None, "thread": None})


# ─────────────────────────────────────────────────────────────────────────────
# DCA Live Engine  (identical math to DCABacktester, event-driven)
# ─────────────────────────────────────────────────────────────────────────────
class DCALiveEngine:
    """
    Stateful DCA engine: call on_candle() for every closed 1m candle.
    Mirrors DCABacktester logic exactly.
    """

    def __init__(self, params: dict):
        self.balance         = params["initial_balance"]
        self.initial_balance = params["initial_balance"]
        self.leverage        = params["leverage"]
        self.deviation_pct   = params["deviation_pct"]
        self.step_multiplier = params["step_multiplier"]
        self.volume_scale    = params["volume_scale"]
        self.take_profit_pct = params["take_profit_pct"]
        self.max_so          = params["max_safety_orders"]
        self.strategy        = params["strategy"]
        self.order_mode      = params["order_mode"]
        self.base_value      = params["base_order"]
        self.so_value        = params["safety_order"]
        self.compounding     = params["compounding"]
        self.fee_rate        = params["fee_rate"]
        self.is_inverse      = params["is_inverse"]
        self.mmr             = 0.005

        self.trades:       list        = []
        self.equity_curve: list        = [self.balance]
        self.open_trade:   dict | None = None
        self._open_next:   bool        = True

    def _get_order_sizes(self):
        if self.order_mode == "Percentage":
            return (self.balance * self.base_value / 100,
                    self.balance * self.so_value  / 100)
        return self.base_value, self.so_value

    def _build_ladder(self, entry_price: float, so_base: float) -> list:
        ladder, cum, alloc = [], 0.0, so_base
        for n in range(self.max_so):
            cum   += (self.deviation_pct / 100) * (self.step_multiplier ** n)
            price  = (entry_price * (1 - cum) if self.strategy == "Long"
                      else entry_price * (1 + cum))
            ladder.append((price, alloc))
            alloc *= self.volume_scale
        return ladder

    def _open_order(self, alloc: float, price: float):
        if self.is_inverse:
            return alloc, alloc * price, alloc * self.fee_rate
        coins = alloc * self.leverage / price
        return coins, alloc * self.leverage, alloc * self.leverage * self.fee_rate

    def _calc_pnl(self, pos_size, pos_cost, avg, exit_price) -> float:
        if self.is_inverse:
            f = ((1 / avg - 1 / exit_price) if self.strategy == "Long"
                 else (1 / exit_price - 1 / avg))
            return pos_cost * f
        f = ((exit_price - avg) if self.strategy == "Long"
             else (avg - exit_price))
        return pos_size * f

    def _liq_price(self, pos_size, pos_cost, avg) -> float:
        if self.is_inverse:
            if self.strategy == "Long":
                return pos_cost * (1 + self.mmr) / (self.balance + pos_size)
            denom = pos_size - self.balance
            return pos_cost * (1 - self.mmr) / denom if denom > 0 else float("inf")
        if self.strategy == "Long":
            liq = (pos_size * avg - self.balance) / (pos_size * (1 - self.mmr))
            return liq if liq > 0 else 0.0
        return (pos_size * avg + self.balance) / (pos_size * (1 + self.mmr))

    def on_candle(self, candle: dict):
        high  = candle["high"]
        low   = candle["low"]
        close = candle["close"]
        ts    = datetime.fromtimestamp(candle["time"] / 1000)

        if self._open_next and self.open_trade is None:
            base_alloc, so_base = self._get_order_sizes()
            if base_alloc <= 0 or base_alloc > self.balance:
                return
            d_sz, d_ct, fee = self._open_order(base_alloc, close)
            self.open_trade = {
                "trade_number": len(self.trades) + 1,
                "entry_time":   ts,
                "entry_price":  close,
                "pos_size":     d_sz,
                "pos_cost":     d_ct,
                "avg":          close,
                "total_alloc":  base_alloc,
                "fees_paid":    fee,
                "idx":          0,
                "ladder":       self._build_ladder(close, so_base),
                "orders":       [{"type": "Base Order", "time": ts,
                                  "price": close, "alloc": base_alloc}],
            }
            self._open_next = False
            return

        if self.open_trade is None:
            return

        t = self.open_trade

        while t["idx"] < len(t["ladder"]):
            so_price, so_alloc = t["ladder"][t["idx"]]
            hit = ((self.strategy == "Long"  and low  <= so_price) or
                   (self.strategy == "Short" and high >= so_price))
            if not hit:
                break
            d_sz, d_ct, fee = self._open_order(so_alloc, so_price)
            t["pos_size"]    += d_sz
            t["pos_cost"]    += d_ct
            t["avg"]          = t["pos_cost"] / t["pos_size"]
            t["fees_paid"]   += fee
            t["total_alloc"] += so_alloc
            t["orders"].append({"type": f"Safety {t['idx'] + 1}",
                                "time": ts, "price": so_price, "alloc": so_alloc})
            t["idx"] += 1

        tp_factor = 1 + self.take_profit_pct / 100
        tp_price  = (t["avg"] * tp_factor if self.strategy == "Long"
                     else t["avg"] / tp_factor)
        liq       = self._liq_price(t["pos_size"], t["pos_cost"], t["avg"])

        tp_hit  = ((self.strategy == "Long"  and high >= tp_price) or
                   (self.strategy == "Short" and low  <= tp_price))
        liq_hit = ((self.strategy == "Long"  and liq > 0 and low  <= liq) or
                   (self.strategy == "Short" and high >= liq))

        if tp_hit or liq_hit:
            exit_price = liq if liq_hit else tp_price
            gross      = (-t["total_alloc"] if liq_hit else
                          self._calc_pnl(t["pos_size"], t["pos_cost"], t["avg"], exit_price))
            exit_fee   = self._open_order(t["total_alloc"], exit_price)[2] if not liq_hit else 0
            t["fees_paid"] += exit_fee
            net = gross - t["fees_paid"]

            if self.compounding:
                self.balance += net
            self.equity_curve.append(self.balance)

            self.trades.append({
                "trade":                 len(self.trades) + 1,
                "entry_time":            t["entry_time"],
                "exit_time":             ts,
                "entry_price":           t["entry_price"],
                "avg_price":             t["avg"],
                "tp_price":              tp_price,
                "exit_price":            exit_price,
                "gross_profit":          gross,
                "fees":                  t["fees_paid"],
                "net_profit":            net,
                "safety_orders_filled":  t["idx"],
                "liquidated":            liq_hit,
                "roi_pct":               (net / t["total_alloc"]) * 100,
            })
            self.open_trade = None
            self._open_next = True

    def open_trade_snapshot(self, current_price: float) -> dict | None:
        t = self.open_trade
        if t is None or current_price <= 0:
            return None
        unreal    = self._calc_pnl(t["pos_size"], t["pos_cost"], t["avg"], current_price)
        tp_factor = 1 + self.take_profit_pct / 100
        tp_price  = (t["avg"] * tp_factor if self.strategy == "Long"
                     else t["avg"] / tp_factor)
        liq = self._liq_price(t["pos_size"], t["pos_cost"], t["avg"])
        return {
            "trade_number":          t["trade_number"],
            "entry_time":            t["entry_time"],
            "entry_price":           t["entry_price"],
            "avg_price":             t["avg"],
            "tp_price":              tp_price,
            "liq_price":             liq,
            "current_price":         current_price,
            "unrealized_pnl":        unreal,
            "fees_so_far":           t["fees_paid"],
            "safety_orders_filled":  t["idx"],
            "total_alloc":           t["total_alloc"],
            "next_so_price":         t["ladder"][t["idx"]][0] if t["idx"] < len(t["ladder"]) else None,
            "orders":                t["orders"],
        }


# ─────────────────────────────────────────────────────────────────────────────
# Streamlit UI
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(layout="wide")
st.title("📡 DCA Live Trading Dashboard (Paper)")

# ── Sidebar ──────────────────────────────────────────────────────────────────
st.sidebar.header("⚙️ Strategy Settings")

strategy      = st.sidebar.selectbox("Strategy", ["Long", "Short"])
contract_type = st.sidebar.selectbox(
    "Contract Type",
    ["Inverse (BTC/USD)", "Linear (USDT)"],
    help="Inverse: BTC_BTCUSD — balance & P&L in BTC.\n"
         "Linear: BTCUSDT — balance & P&L in USDT.",
)
is_inverse = contract_type.startswith("Inverse")
currency   = "BTC" if is_inverse else "USDT"

if is_inverse:
    initial_balance = st.sidebar.number_input(
        "Initial Balance (BTC)", value=0.06013, min_value=0.001,
        step=0.001, format="%.5f"
    )
else:
    initial_balance = st.sidebar.number_input(
        "Initial Balance (USDT)", value=10000.0, min_value=1.0, step=100.0
    )

leverage = st.sidebar.slider("Leverage", 1, 100, 50)

if is_inverse:
    trading_pair = st.sidebar.selectbox(
        "Trading Pair", ["BTCUSD", "ETHUSD"],
        help="Bybit inverse perpetual symbol (balance & P&L in BTC)",
    )
else:
    trading_pair = st.sidebar.selectbox(
        "Trading Pair", ["BTCUSDT", "ETHUSDT", "XRPUSDT", "SOLUSDT"]
    )

order_mode = st.sidebar.selectbox("Order Size Mode", ["Percentage", f"Fixed {currency}"])
if order_mode == "Percentage":
    base_order_value   = st.sidebar.number_input("Base Order (%)",   value=10.0, min_value=0.1, step=0.1)
    safety_order_value = st.sidebar.number_input("Safety Order (%)", value=10.0, min_value=0.1, step=0.1)
else:
    base_order_value   = st.sidebar.number_input(f"Base Order ({currency})",   value=0.006 if is_inverse else 1000.0)
    safety_order_value = st.sidebar.number_input(f"Safety Order ({currency})", value=0.006 if is_inverse else 1000.0)

deviation_pct     = st.sidebar.number_input("Price Deviation (%)", value=2.0,  min_value=0.1, step=0.1)
step_multiplier   = st.sidebar.number_input("Step Multiplier",     value=1.0,  min_value=0.1, step=0.1)
volume_scale      = st.sidebar.number_input("Volume Scale",        value=1.28, min_value=1.0, step=0.01)
take_profit_pct   = st.sidebar.number_input("Take Profit (%)",     value=1.0,  min_value=0.01, step=0.1)
max_safety_orders = st.sidebar.slider("Max Safety Orders", 1, 30, 17)
fee_rate          = st.sidebar.number_input(
    "Taker Fee (%)", value=0.055, min_value=0.0, step=0.005, format="%.3f"
) / 100
compounding = st.sidebar.checkbox("Enable Compounding", value=True)

# ── Session state — restore from cache on browser reconnect ──────────────────
_s = _get_state()
if "synced" not in st.session_state:
    # First load or browser reconnect: pull persisted values from cache
    st.session_state.running           = _s["running"]
    st.session_state.engine            = _s["engine"]
    st.session_state.candles_processed = _s["candles_processed"]
    st.session_state.synced            = True

# ── Control buttons ───────────────────────────────────────────────────────────
cb1, cb2, cb3 = st.columns([1, 1, 4])

with cb1:
    start_clicked = st.button("▶ Start", type="primary",
                               disabled=st.session_state.running)
with cb2:
    stop_clicked  = st.button("⏹ Stop",
                               disabled=not st.session_state.running)
with cb3:
    reset_clicked = st.button("🔄 Reset (clears all trades)")

if start_clicked:
    params = {
        "initial_balance":   initial_balance,
        "leverage":          leverage,
        "base_order":        base_order_value,
        "safety_order":      safety_order_value,
        "order_mode":        order_mode,
        "deviation_pct":     deviation_pct,
        "step_multiplier":   step_multiplier,
        "volume_scale":      volume_scale,
        "take_profit_pct":   take_profit_pct,
        "max_safety_orders": max_safety_orders,
        "strategy":          strategy,
        "compounding":       compounding,
        "fee_rate":          fee_rate,
        "is_inverse":        is_inverse,
    }
    if st.session_state.engine is None:
        st.session_state.engine = DCALiveEngine(params)
    ws_start(trading_pair, is_inverse)
    st.session_state.running = True
    _s["engine"]  = st.session_state.engine
    _s["running"] = True
    st.rerun()

if stop_clicked:
    ws_stop()
    st.session_state.running = False
    _s["running"] = False
    st.rerun()

if reset_clicked:
    ws_stop()
    st.session_state.running           = False
    st.session_state.engine            = None
    st.session_state.candles_processed = 0
    _s["running"]           = False
    _s["engine"]            = None
    _s["candles_processed"] = 0
    while not _s["candle_q"].empty():
        try:
            _s["candle_q"].get_nowait()
        except queue.Empty:
            break
    st.rerun()

# ── Process any new closed candles ───────────────────────────────────────────
if st.session_state.running and st.session_state.engine is not None:
    engine    = st.session_state.engine
    processed = 0
    while not _s["candle_q"].empty():
        try:
            candle = _s["candle_q"].get_nowait()
            engine.on_candle(candle)
            processed += 1
        except queue.Empty:
            break
    if processed:
        st.session_state.candles_processed += processed
        _s["candles_processed"] = st.session_state.candles_processed

# ── Status bar ────────────────────────────────────────────────────────────────
with _s["lock"]:
    connected  = _s["ws_info"]["connected"]
    ws_error   = _s["ws_info"]["error"]
    live_price = _s["ticker"]["price"]
    live_ts    = _s["ticker"]["ts"]

status_color = "🟢" if connected else "🔴"
status_text  = "Connected" if connected else "Disconnected"

s1, s2, s3 = st.columns(3)
s1.markdown(f"**WebSocket:** {status_color} {status_text}")
s2.markdown(f"**Live Price ({trading_pair}):** `${live_price:,.2f}`  _{live_ts}_")
s3.markdown(f"**Candles processed:** `{st.session_state.candles_processed:,}`")

if ws_error:
    st.error(f"WebSocket error: {ws_error}")

st.divider()

# ── Metrics ───────────────────────────────────────────────────────────────────
engine = st.session_state.engine
fmt    = (lambda v: f"{v:.6f}") if is_inverse else (lambda v: f"{v:,.2f}")

if engine is not None:
    df = pd.DataFrame(engine.trades)

    total_net    = df["net_profit"].sum() if not df.empty else 0
    total_fees   = df["fees"].sum()       if not df.empty else 0
    win_rate     = (df["net_profit"] > 0).mean() * 100 if not df.empty else 0
    liquidations = int(df["liquidated"].sum())         if not df.empty else 0
    roi_pct      = (engine.balance - engine.initial_balance) / engine.initial_balance * 100

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Closed Trades",             len(df))
    c2.metric("Win Rate",                  f"{win_rate:.1f}%")
    c3.metric(f"Net Profit ({currency})",  fmt(total_net))
    c4.metric(f"Total Fees ({currency})",  fmt(total_fees))
    c5.metric("Liquidations",              liquidations)
    c6.metric(f"Balance ({currency})",     fmt(engine.balance),
              delta=f"{roi_pct:+.2f}%")

    # ── Open trade ────────────────────────────────────────────────────────────
    snap = engine.open_trade_snapshot(live_price)
    if snap:
        pnl      = snap["unrealized_pnl"]
        pnl_sign = "🟢" if pnl >= 0 else "🔴"
        st.subheader(f"🔓 Open Trade #{snap['trade_number']}")

        oc1, oc2, oc3, oc4, oc5, oc6 = st.columns(6)
        oc1.metric("Opened",      str(snap["entry_time"])[:16])
        oc2.metric("Entry Price", f"${snap['entry_price']:,.2f}")
        oc3.metric("Avg Price",   f"${snap['avg_price']:,.2f}")
        oc4.metric("TP Price",    f"${snap['tp_price']:,.2f}")
        oc5.metric("Liq Price",   f"${snap['liq_price']:,.0f}")
        oc6.metric("SOs Filled",  snap["safety_orders_filled"])

        oc7, oc8, oc9 = st.columns(3)
        oc7.metric(f"{pnl_sign} Unrealised PnL ({currency})", fmt(pnl),
                   delta=f"{(pnl / snap['total_alloc'] * 100):.2f}% on allocated")
        gap = (snap["tp_price"] / live_price - 1) * 100 if live_price > 0 else 0
        oc8.metric("Gap to TP", f"{gap:.2f}%")
        if snap["next_so_price"]:
            next_gap = (live_price / snap["next_so_price"] - 1) * 100
            oc9.metric("Next SO", f"${snap['next_so_price']:,.2f}",
                       delta=f"{next_gap:.2f}% away", delta_color="inverse")

        with st.expander("📋 Order log for this trade"):
            st.dataframe(pd.DataFrame(snap["orders"]))
    else:
        if st.session_state.running:
            st.info("⏳ Waiting for the next closed candle to open a trade…")

    # ── Equity curve ──────────────────────────────────────────────────────────
    if len(engine.equity_curve) > 1:
        st.subheader("📊 Equity Curve")
        st.line_chart(pd.DataFrame(engine.equity_curve, columns=[f"Balance ({currency})"]))

    # ── Closed trades table ───────────────────────────────────────────────────
    if not df.empty:
        df["entry_time"]   = pd.to_datetime(df["entry_time"])
        df["exit_time"]    = pd.to_datetime(df["exit_time"])
        df["duration_min"] = (df["exit_time"] - df["entry_time"]).dt.total_seconds() / 60

        st.subheader("💰 Closed Trades")
        st.dataframe(
            df[[
                "trade", "entry_time", "exit_time", "entry_price", "avg_price",
                "tp_price", "exit_price", "gross_profit", "fees", "net_profit",
                "safety_orders_filled", "duration_min", "roi_pct", "liquidated",
            ]].rename(columns={
                "duration_min":         "Duration (min)",
                "roi_pct":              "ROI (%)",
                "entry_price":          "Entry Price",
                "avg_price":            "Avg Price",
                "tp_price":             "TP Price",
                "exit_price":           "Exit Price",
                "gross_profit":         f"Gross ({currency})",
                "net_profit":           f"Net ({currency})",
                "fees":                 f"Fees ({currency})",
                "safety_orders_filled": "SOs Filled",
            })
        )

else:
    st.info("Configure your settings in the sidebar and press **▶ Start** to begin paper trading.")

# ── Auto-refresh every second while running ───────────────────────────────────
if st.session_state.running:
    time.sleep(1)
    st.rerun()
