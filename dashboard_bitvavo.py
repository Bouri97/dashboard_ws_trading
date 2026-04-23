"""
dashboard_bitvavo.py — Live DCA Spot Paper Trading Dashboard (Bitvavo)
───────────────────────────────────────────────────────────────────────
Spot DCA bot paper trading on Bitvavo prices.
No leverage, no liquidation — purely buying and selling coins with EUR.

Supported pairs: BTC-EUR, ETH-EUR, XRP-EUR, SOL-EUR

Architecture
─────────────
  Background thread  →  Bitvavo WebSocket (candles 1m + ticker)
                      →  candle_q (closed 1m candles)
  Streamlit loop     ←  drains candle_q every second
                      →  DCASpotEngine.on_candle()
                      →  re-renders UI

DCA Spot logic
───────────────
  - Base order buys coins with EUR at market open price
  - Safety orders buy more coins at lower prices (price deviation %)
  - Average price = total EUR spent / total coins held
  - Take profit = sell all coins when price >= avg × (1 + tp%)
  - P&L = coins × exit_price − total EUR spent − fees
  - No liquidation (spot = you own the coins)
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
# Shared state via st.cache_resource
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_resource
def _get_state() -> dict:
    return {
        "lock":           threading.Lock(),
        "candle_q":       queue.Queue(),
        "ticker":         {"price": 0.0, "ts": "—"},
        "ws_info":        {"connected": False, "ws": None, "thread": None, "error": ""},
        "current_candle": {"ts": None, "open": 0.0, "high": 0.0, "low": 0.0, "close": 0.0},
    }


# ── WebSocket callbacks ───────────────────────────────────────────────────────
def _on_message(ws, raw):
    state = _get_state()
    msg   = json.loads(raw)
    event = msg.get("event", "")

    if event == "candle":
        candle_data = msg["candle"][0]           # [ts, open, high, low, close, vol]
        ts    = int(candle_data[0])
        open_ = float(candle_data[1])
        high  = float(candle_data[2])
        low   = float(candle_data[3])
        close = float(candle_data[4])

        with state["lock"]:
            prev = state["current_candle"]
            if prev["ts"] is not None and ts != prev["ts"]:
                # New candle arrived → previous candle is now closed
                state["candle_q"].put({
                    "time":  prev["ts"],
                    "open":  prev["open"],
                    "high":  prev["high"],
                    "low":   prev["low"],
                    "close": prev["close"],
                })
            # Update current candle
            state["current_candle"] = {
                "ts": ts, "open": open_, "high": high, "low": low, "close": close
            }
            state["ticker"]["price"] = close
            state["ticker"]["ts"]    = datetime.fromtimestamp(ts / 1000).strftime("%H:%M:%S UTC")

    elif event == "ticker":
        last = msg.get("lastPrice")
        if last:
            with state["lock"]:
                state["ticker"]["price"] = float(last)
                state["ticker"]["ts"]    = datetime.now().strftime("%H:%M:%S UTC")

    elif event == "subscribed":
        pass  # confirmation message, ignore

    elif event == "error":
        with state["lock"]:
            state["ws_info"]["error"] = msg.get("error", str(msg))


def _on_open(ws):
    pass  # subscription is sent after open in the thread function


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


def _ws_thread_fn(market: str):
    state = _get_state()

    def on_open(ws):
        with state["lock"]:
            state["ws_info"]["connected"] = True
            state["ws_info"]["error"]     = ""
        ws.send(json.dumps({
            "action": "subscribe",
            "channels": [
                {"name": "candles", "interval": ["1m"], "markets": [market]},
                {"name": "ticker",  "markets": [market]},
            ],
        }))

    ws = websocket.WebSocketApp(
        "wss://ws.bitvavo.com/v2/",
        on_message=_on_message,
        on_open=on_open,
        on_close=_on_close,
        on_error=_on_error,
    )
    with state["lock"]:
        state["ws_info"]["ws"] = ws
    ws.run_forever()


def ws_start(market: str):
    state = _get_state()
    with state["lock"]:
        t = state["ws_info"].get("thread")
        if t and t.is_alive():
            return
    thread = threading.Thread(target=_ws_thread_fn, args=(market,), daemon=True)
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
        state["current_candle"] = {"ts": None, "open": 0.0, "high": 0.0, "low": 0.0, "close": 0.0}


# ─────────────────────────────────────────────────────────────────────────────
# DCA Spot Engine
# ─────────────────────────────────────────────────────────────────────────────
class DCASpotEngine:
    """
    Spot DCA engine. No leverage, no liquidation.
    Balance and P&L are in EUR.
    """

    def __init__(self, params: dict):
        self.balance         = params["initial_balance"]
        self.initial_balance = params["initial_balance"]
        self.deviation_pct   = params["deviation_pct"]
        self.step_multiplier = params["step_multiplier"]
        self.volume_scale    = params["volume_scale"]
        self.take_profit_pct = params["take_profit_pct"]
        self.max_so          = params["max_safety_orders"]
        self.order_mode      = params["order_mode"]
        self.base_value      = params["base_order"]
        self.so_value        = params["safety_order"]
        self.compounding     = params["compounding"]
        self.fee_rate        = params["fee_rate"]

        self.trades:       list        = []
        self.equity_curve: list        = [self.balance]
        self.open_trade:   dict | None = None
        self._open_next:   bool        = True

    def _get_order_sizes(self):
        if self.order_mode == "Percentage":
            return (self.balance * self.base_value / 100,
                    self.balance * self.so_value  / 100)
        return self.base_value, self.so_value

    def _build_ladder(self, entry_price: float, so_base_eur: float) -> list:
        """Returns list of (price, eur_alloc) for each safety order."""
        ladder, cum, alloc = [], 0.0, so_base_eur
        for n in range(self.max_so):
            cum   += (self.deviation_pct / 100) * (self.step_multiplier ** n)
            price  = entry_price * (1 - cum)   # spot: always long (buy low)
            ladder.append((price, alloc))
            alloc *= self.volume_scale
        return ladder

    def _buy(self, eur_alloc: float, price: float):
        """Returns (coins_bought, fee_eur)."""
        fee   = eur_alloc * self.fee_rate
        coins = (eur_alloc - fee) / price
        return coins, fee

    def _sell_fee(self, coins: float, price: float) -> float:
        return coins * price * self.fee_rate

    def on_candle(self, candle: dict):
        high  = candle["high"]
        low   = candle["low"]
        close = candle["close"]
        ts    = datetime.fromtimestamp(candle["time"] / 1000)

        # Open new trade on first candle after previous close
        if self._open_next and self.open_trade is None:
            base_eur, so_base_eur = self._get_order_sizes()
            if base_eur <= 0 or base_eur > self.balance:
                return
            coins, fee = self._buy(base_eur, close)
            self.balance  -= base_eur
            self.open_trade = {
                "trade_number": len(self.trades) + 1,
                "entry_time":   ts,
                "entry_price":  close,
                "coins":        coins,
                "total_eur":    base_eur,
                "avg":          close,
                "fees_paid":    fee,
                "idx":          0,
                "ladder":       self._build_ladder(close, so_base_eur),
                "orders":       [{"type": "Base Order", "time": ts,
                                  "price": close, "eur": base_eur, "coins": coins}],
            }
            self._open_next = False
            return

        if self.open_trade is None:
            return

        t = self.open_trade

        # Fill triggered safety orders
        while t["idx"] < len(t["ladder"]):
            so_price, so_eur = t["ladder"][t["idx"]]
            if low > so_price:
                break
            if so_eur > self.balance:
                t["idx"] += 1
                continue
            coins, fee = self._buy(so_eur, so_price)
            self.balance   -= so_eur
            t["coins"]     += coins
            t["total_eur"] += so_eur
            t["avg"]        = t["total_eur"] / t["coins"]  # weighted avg in EUR/coin
            t["fees_paid"] += fee
            t["orders"].append({"type": f"Safety {t['idx'] + 1}",
                                "time": ts, "price": so_price,
                                "eur": so_eur, "coins": coins})
            t["idx"] += 1

        # Check take profit
        tp_price = t["avg"] * (1 + self.take_profit_pct / 100)
        if high >= tp_price:
            sell_eur  = t["coins"] * tp_price
            sell_fee  = self._sell_fee(t["coins"], tp_price)
            net_eur   = sell_eur - sell_fee - t["fees_paid"]  # net vs EUR spent
            gross_eur = sell_eur - t["total_eur"]             # gross gain

            if self.compounding:
                self.balance += t["total_eur"] + net_eur      # return capital + profit
            else:
                self.balance += t["total_eur"] + net_eur

            self.equity_curve.append(self.balance)
            self.trades.append({
                "trade":                 len(self.trades) + 1,
                "entry_time":            t["entry_time"],
                "exit_time":             ts,
                "entry_price":           t["entry_price"],
                "avg_price":             t["avg"],
                "tp_price":              tp_price,
                "exit_price":            tp_price,
                "gross_profit_eur":      gross_eur,
                "fees_eur":              t["fees_paid"] + sell_fee,
                "net_profit_eur":        net_eur,
                "safety_orders_filled":  t["idx"],
                "roi_pct":               (net_eur / t["total_eur"]) * 100,
            })
            self.open_trade = None
            self._open_next = True

    def open_trade_snapshot(self, current_price: float) -> dict | None:
        t = self.open_trade
        if t is None or current_price <= 0:
            return None
        unrealized_eur = t["coins"] * current_price - t["total_eur"]
        tp_price       = t["avg"] * (1 + self.take_profit_pct / 100)
        return {
            "trade_number":          t["trade_number"],
            "entry_time":            t["entry_time"],
            "entry_price":           t["entry_price"],
            "avg_price":             t["avg"],
            "tp_price":              tp_price,
            "current_price":         current_price,
            "coins":                 t["coins"],
            "total_eur":             t["total_eur"],
            "unrealized_eur":        unrealized_eur,
            "fees_so_far":           t["fees_paid"],
            "safety_orders_filled":  t["idx"],
            "next_so_price":         t["ladder"][t["idx"]][0] if t["idx"] < len(t["ladder"]) else None,
            "orders":                t["orders"],
        }


# ─────────────────────────────────────────────────────────────────────────────
# Streamlit UI
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(layout="wide")
st.title("🇧🇪 DCA Spot Paper Trading Dashboard (Bitvavo)")

# ── Sidebar ───────────────────────────────────────────────────────────────────
st.sidebar.header("⚙️ Strategy Settings")

trading_pair = st.sidebar.selectbox(
    "Trading Pair",
    ["BTC-EUR", "ETH-EUR", "XRP-EUR", "SOL-EUR"],
)

initial_balance = st.sidebar.number_input(
    "Initial Balance (EUR)", value=1000.0, min_value=1.0, step=100.0
)

order_mode = st.sidebar.selectbox("Order Size Mode", ["Percentage", "Fixed EUR"])
if order_mode == "Percentage":
    base_order_value   = st.sidebar.number_input("Base Order (%)",   value=10.0, min_value=0.1, step=0.1)
    safety_order_value = st.sidebar.number_input("Safety Order (%)", value=10.0, min_value=0.1, step=0.1)
else:
    base_order_value   = st.sidebar.number_input("Base Order (EUR)",   value=100.0, min_value=1.0, step=10.0)
    safety_order_value = st.sidebar.number_input("Safety Order (EUR)", value=100.0, min_value=1.0, step=10.0)

deviation_pct     = st.sidebar.number_input("Price Deviation (%)", value=2.0,  min_value=0.1, step=0.1)
step_multiplier   = st.sidebar.number_input("Step Multiplier",     value=1.0,  min_value=0.1, step=0.1)
volume_scale      = st.sidebar.number_input("Volume Scale",        value=1.28, min_value=1.0, step=0.01)
take_profit_pct   = st.sidebar.number_input("Take Profit (%)",     value=1.0,  min_value=0.01, step=0.1)
max_safety_orders = st.sidebar.slider("Max Safety Orders", 1, 30, 10)
fee_rate          = st.sidebar.number_input(
    "Taker Fee (%)", value=0.25, min_value=0.0, step=0.05, format="%.2f"
) / 100
compounding = st.sidebar.checkbox("Enable Compounding", value=True)

# ── Session state ─────────────────────────────────────────────────────────────
if "running" not in st.session_state:
    st.session_state.running = False
if "engine" not in st.session_state:
    st.session_state.engine = None
if "candles_processed" not in st.session_state:
    st.session_state.candles_processed = 0

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
    }
    if st.session_state.engine is None:
        st.session_state.engine = DCASpotEngine(params)
    ws_start(trading_pair)
    st.session_state.running = True
    st.rerun()

if stop_clicked:
    ws_stop()
    st.session_state.running = False
    st.rerun()

if reset_clicked:
    ws_stop()
    st.session_state.running           = False
    st.session_state.engine            = None
    st.session_state.candles_processed = 0
    state = _get_state()
    while not state["candle_q"].empty():
        try:
            state["candle_q"].get_nowait()
        except queue.Empty:
            break
    st.rerun()

# ── Process new closed candles ────────────────────────────────────────────────
state = _get_state()
if st.session_state.running and st.session_state.engine is not None:
    engine    = st.session_state.engine
    processed = 0
    while not state["candle_q"].empty():
        try:
            candle = state["candle_q"].get_nowait()
            engine.on_candle(candle)
            processed += 1
        except queue.Empty:
            break
    st.session_state.candles_processed += processed

# ── Status bar ────────────────────────────────────────────────────────────────
with state["lock"]:
    connected  = state["ws_info"]["connected"]
    ws_error   = state["ws_info"]["error"]
    live_price = state["ticker"]["price"]
    live_ts    = state["ticker"]["ts"]

status_color = "🟢" if connected else "🔴"
status_text  = "Connected" if connected else "Disconnected"

s1, s2, s3 = st.columns(3)
s1.markdown(f"**WebSocket:** {status_color} {status_text}")
s2.markdown(f"**Live Price ({trading_pair}):** `€{live_price:,.4f}`  _{live_ts}_")
s3.markdown(f"**Candles processed:** `{st.session_state.candles_processed:,}`")

if ws_error:
    st.error(f"WebSocket error: {ws_error}")

st.divider()

# ── Metrics ───────────────────────────────────────────────────────────────────
engine = st.session_state.engine

if engine is not None:
    df = pd.DataFrame(engine.trades)

    total_net    = df["net_profit_eur"].sum()  if not df.empty else 0.0
    total_fees   = df["fees_eur"].sum()        if not df.empty else 0.0
    win_rate     = (df["net_profit_eur"] > 0).mean() * 100 if not df.empty else 0.0
    roi_pct      = (engine.balance - engine.initial_balance) / engine.initial_balance * 100

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Closed Trades",    len(df))
    c2.metric("Win Rate",         f"{win_rate:.1f}%")
    c3.metric("Net Profit (EUR)", f"€{total_net:,.2f}")
    c4.metric("Total Fees (EUR)", f"€{total_fees:,.2f}")
    c5.metric("Balance (EUR)",    f"€{engine.balance:,.2f}", delta=f"{roi_pct:+.2f}%")

    # ── Open trade ────────────────────────────────────────────────────────────
    snap = engine.open_trade_snapshot(live_price)
    if snap:
        pnl      = snap["unrealized_eur"]
        pnl_sign = "🟢" if pnl >= 0 else "🔴"
        st.subheader(f"🔓 Open Trade #{snap['trade_number']}")

        oc1, oc2, oc3, oc4, oc5, oc6 = st.columns(6)
        oc1.metric("Opened",        str(snap["entry_time"])[:16])
        oc2.metric("Entry Price",   f"€{snap['entry_price']:,.4f}")
        oc3.metric("Avg Price",     f"€{snap['avg_price']:,.4f}")
        oc4.metric("TP Price",      f"€{snap['tp_price']:,.4f}")
        oc5.metric("Current Price", f"€{snap['current_price']:,.4f}")
        oc6.metric("SOs Filled",    snap["safety_orders_filled"])

        oc7, oc8, oc9 = st.columns(3)
        oc7.metric(f"{pnl_sign} Unrealised P&L (EUR)", f"€{pnl:,.2f}",
                   delta=f"{(pnl / snap['total_eur'] * 100):.2f}% on invested")
        gap = (snap["tp_price"] / live_price - 1) * 100 if live_price > 0 else 0
        oc8.metric("Gap to TP", f"{gap:.2f}%")
        if snap["next_so_price"]:
            next_gap = (live_price / snap["next_so_price"] - 1) * 100
            oc9.metric("Next SO", f"€{snap['next_so_price']:,.4f}",
                       delta=f"{next_gap:.2f}% away", delta_color="inverse")

        with st.expander("📋 Order log for this trade"):
            st.dataframe(pd.DataFrame(snap["orders"]))
    else:
        if st.session_state.running:
            st.info("⏳ Waiting for the next closed candle to open a trade…")

    # ── Equity curve ──────────────────────────────────────────────────────────
    if len(engine.equity_curve) > 1:
        st.subheader("📊 Equity Curve")
        st.line_chart(pd.DataFrame(engine.equity_curve, columns=["Balance (EUR)"]))

    # ── Closed trades table ───────────────────────────────────────────────────
    if not df.empty:
        df["entry_time"]   = pd.to_datetime(df["entry_time"])
        df["exit_time"]    = pd.to_datetime(df["exit_time"])
        df["duration_min"] = (df["exit_time"] - df["entry_time"]).dt.total_seconds() / 60

        st.subheader("💰 Closed Trades")
        st.dataframe(
            df[[
                "trade", "entry_time", "exit_time", "entry_price", "avg_price",
                "tp_price", "exit_price", "gross_profit_eur", "fees_eur",
                "net_profit_eur", "safety_orders_filled", "duration_min", "roi_pct",
            ]].rename(columns={
                "entry_price":          "Entry Price",
                "avg_price":            "Avg Price",
                "tp_price":             "TP Price",
                "exit_price":           "Exit Price",
                "gross_profit_eur":     "Gross (EUR)",
                "fees_eur":             "Fees (EUR)",
                "net_profit_eur":       "Net (EUR)",
                "safety_orders_filled": "SOs Filled",
                "duration_min":         "Duration (min)",
                "roi_pct":              "ROI (%)",
            })
        )

else:
    st.info("Configure your settings in the sidebar and press **▶ Start** to begin paper trading.")

# ── Auto-refresh every second while running ───────────────────────────────────
if st.session_state.running:
    time.sleep(1)
    st.rerun()
