"""
Bot Scalping v22.1 PAPER — DYNAMIC INVERT TOGGLE (Binance Futures, fokus 1 koin)
===============================================================================
LOGIKA:
- Mode awal: NORMAL (sinyal LONG -> eksekusi LONG, SHORT -> SHORT)
- Posisi MINUS (SL atau TIME_LIMIT dengan PnL bersih < 0) -> toggle mode
  (NORMAL -> INVERTED, INVERTED -> NORMAL)
- Posisi PROFIT (TP atau TIME_LIMIT dengan PnL bersih >= 0) -> mode TETAP
- MAX_POSITIONS = 1, margin 3 USDT / posisi, tanpa ban simbol, tanpa signal flip

PERBAIKAN DARI v22.0 (ringkas):
 1. Off-by-one candle: candle yang masih berjalan ikut terbaca -> sinyal basi 5-10 menit.
    Sekarang hanya candle CLOSED dan indikator memakai iloc[-1] (candle tertutup terakhir).
 2. BTC tidak di-stream kline -> regime BTC basi kalau koin fokus bukan BTC. Sekarang BTC selalu di-stream.
 3. Bobot adaptif mati (nama sinyal != key bobot) dan tercemar mode INVERTED. Sudah diperbaiki.
 4. Re-entry berulang pada sinyal candle yang sama (COOLDOWN=0). Sekarang 1 entry per candle.
 5. Reservasi slot bisa bocor (posisi "_r" nyangkut) bila ada exception. Sekarang pakai try/except.
 6. REST blok 403/429 membekukan semua thread (sleep di dalam lock). Sekarang langsung raise.
 7. `except: pass` telanjang menyembunyikan error (penyebab posisi nyangkut). Semua diganti log.
 8. Presisi qty di-hardcode 2 desimal (BTC -> qty 0). Sekarang dari exchangeInfo (stepSize/minNotional).
 9. Deteksi gap kline WS + data basi -> re-bootstrap otomatis.
10. TP/SL default untuk koin major (2.5% terlalu lebar), slippage paper, log CSV, simpan mode ke file.
"""

import sys
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

import os
import csv
import json
import time
import math
import inspect
import threading
import numpy as np
import pandas as pd
from collections import deque, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Optional, Tuple, List, Dict, Any

from dotenv import load_dotenv
from binance.client import Client
from binance import ThreadedWebsocketManager
import ta

load_dotenv()
api_key = os.getenv("API_KEY")
api_secret = os.getenv("API_SECRET")


def _make_client():
    rp = {"timeout": 15}
    try:
        return Client(api_key, api_secret, requests_params=rp, ping=False)
    except TypeError:
        return Client(api_key, api_secret, requests_params=rp)


client = _make_client()
# Paper trading: hanya membaca data pasar mainnet, TIDAK ada order yang dikirim.
client.FUTURES_URL = "https://fapi.binance.com/fapi"


def _create_twm():
    kwargs = {"api_key": api_key, "api_secret": api_secret}
    try:
        params = inspect.signature(ThreadedWebsocketManager.__init__).parameters
        if "max_queue_size" in params:
            kwargs["max_queue_size"] = 2000
    except Exception:
        pass
    try:
        return ThreadedWebsocketManager(**kwargs)
    except TypeError:
        kwargs.pop("max_queue_size", None)
        return ThreadedWebsocketManager(**kwargs)


twm = _create_twm()

# ═══════════════════════════════════════════════════════════════════════════
#  KONFIGURASI
# ═══════════════════════════════════════════════════════════════════════════

SYMBOLS       = ["ETHUSDT"]     # fokus 1 koin (bisa ganti BTCUSDT / SOLUSDT)
LEVERAGE      = 20
ORDER_USDT    = 3.0             # margin per posisi
MAX_POSITIONS = 1

SCAN_LOOP_SEC = 0.5             # seberapa sering cek sinyal (sinyal hanya berubah per candle 5m)
MONITOR_INT   = 0.2
BATCH_SIZE    = 15
MAX_WORKERS   = 4

KLINE_INTERVAL = Client.KLINE_INTERVAL_5MINUTE
KLINE_MS       = 5 * 60 * 1000

# Kualitas sinyal
MIN_SCORE            = 55
SLIPPAGE_GUARD       = 0.0015   # batal entry jika harga live geser > 0.15% dari close candle sinyal
SIGNAL_MAX_AGE_SEC   = 90       # sinyal hanya valid max 90 detik setelah candle tutup
ONE_ENTRY_PER_CANDLE = True     # tidak re-entry pada candle sinyal yang sama
STALE_DATA_SEC       = 360      # data candle lebih tua dari ini = dianggap basi -> re-bootstrap

# Risk (dikalibrasi untuk koin major seperti ETH; ATR 5m ETH ~0.15-0.3%)
ATR_TP_MULTIPLIER = 3.5
ATR_SL_MULTIPLIER = 1.8
MIN_TP_PCT        = 0.006
MAX_TP_PCT        = 0.012
MIN_SL_PCT        = 0.004
MAX_SL_PCT        = 0.008
MAX_HOLD_SECONDS  = 1800        # 30 menit

# Biaya paper
TAKER_FEE       = 0.0005        # per sisi
PAPER_SLIPPAGE  = 0.0002        # 0.02% merugikan di entry & exit SL/TIME_LIMIT (set 0 untuk mematikan)
ENFORCE_MIN_NOTIONAL = True     # tolak entry jika notional < minNotional exchange

# Mikrostruktur
WALL_RATIO_THRESHOLD  = 2.5
WALL_DEPTH_PCT        = 0.35
WALL_PROXIMITY_PCT    = 0.005
IMBALANCE_STRONG_BULL = 0.25
IMBALANCE_STRONG_BEAR = -0.25
SPOOF_DROP_THRESHOLD  = 0.40
DEPTH_SOCKET_CHUNK    = 8

# BTC makro
BTC_CRASH_THRESHOLD  = -0.003
BTC_PUMP_THRESHOLD   = 0.003
BTC_WINDOW_SEC       = 8.0
BTC_BREAKER_COOLDOWN = 120.0

# Kill switch (pause, bukan ban simbol)
DAILY_LOSS   = -20.0
CONSEC_MAX   = 15
CONSEC_PAUSE = 10

# REST
REST_MIN_INTERVAL = 0.20
REST_403_COOLDOWN = 300.0
REST_429_COOLDOWN = 60.0
REST_418_COOLDOWN = 900.0

# Learning
LEARNING_WINDOW       = 200
MIN_TRADES_FOR_WEIGHT = 20

MARKPRICE_FRESH_SEC = 10
WS_STALE_SEC        = 30

STATE_FILE = "state_v22_1.json"
CSV_FILE   = "trades_v22_1.csv"

# ═══════════════════════════════════════════════════════════════════════════
#  GLOBAL STATE
# ═══════════════════════════════════════════════════════════════════════════

_lock            = threading.Lock()
_executor        = ThreadPoolExecutor(max_workers=MAX_WORKERS)
_ws_mark_price   = {}
_kline_cache     = {}
_kline_lock      = threading.Lock()
_ws_last_msg_ts  = time.time()
_symbol_rules    = {}
_last_entry_candle = {}

_macro     = {"btc": "UNKNOWN"}
_btc_macro = {"regime": "UNKNOWN"}
_ks        = {"active": False, "reason": "", "resume": 0, "consec": 0, "daily": 0.0, "day_reset": 0}
_stats = {
    "trades": 0, "wins": 0, "losses": 0, "pnl": 0.0, "best": 0.0, "worst": 0.0, "ath_pnl": 0.0,
    "hard_sl": 0, "tp_exit": 0, "time_limit_exit": 0, "regime_block": 0,
    "wall_veto": 0, "btc_breaker_veto": 0, "spoof_veto": 0, "absorb_entries": 0,
    "hist": deque(maxlen=200), "start": time.time(),
}

# False = NORMAL (LONG->LONG), True = INVERTED (LONG->SHORT)
is_logic_inverted = False

live_positions = {}
trade_log      = []

_last_err_print = defaultdict(float)
_rest_lock        = threading.Lock()
_rest_last_ts     = 0.0
_rest_block_until = 0.0
_rest_price_cache = {}

BASE_COLS = ["time", "open", "high", "low", "close", "volume", "ct", "qv", "trades", "tbbase", "tbquote", "ignore"]


def _log_err(tag, e, cooldown=10):
    now = time.time()
    if now - _last_err_print[tag] > cooldown:
        print(f"  ⚠️ [{tag}] {type(e).__name__}: {e}")
        _last_err_print[tag] = now


def _log_warn(tag, msg, cooldown=10):
    now = time.time()
    if now - _last_err_print[tag] > cooldown:
        print(f"  ⚠️ [{tag}] {msg}")
        _last_err_print[tag] = now


def mode_str():
    return "INVERTED" if is_logic_inverted else "NORMAL"


def save_state():
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump({"inverted": is_logic_inverted, "saved": time.time()}, f)
    except Exception as e:
        _log_err("save_state", e)


def load_state():
    global is_logic_inverted
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                is_logic_inverted = bool(json.load(f).get("inverted", False))
            print(f"  💾 State dimuat: mode terakhir = {mode_str()}")
    except Exception as e:
        _log_err("load_state", e)


def log_trade_csv(row: dict):
    try:
        new = not os.path.exists(CSV_FILE)
        with open(CSV_FILE, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(row.keys()))
            if new:
                w.writeheader()
            w.writerow(row)
    except Exception as e:
        _log_err("csv", e)


# ═══════════════════════════════════════════════════════════════════════════
#  REST (aman: tidak tidur di dalam lock, raise cepat saat diblokir)
# ═══════════════════════════════════════════════════════════════════════════

def _rest_call(tag, fn, *args, retries=1, **kwargs):
    global _rest_last_ts, _rest_block_until
    last_exc = None
    for attempt in range(retries + 1):
        now = time.time()
        if now < _rest_block_until:
            raise RuntimeError(f"REST diblokir {_rest_block_until - now:.0f}s lagi ({tag})")
        with _rest_lock:
            gap = time.time() - _rest_last_ts
            if gap < REST_MIN_INTERVAL:
                time.sleep(REST_MIN_INTERVAL - gap)
            _rest_last_ts = time.time()
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            last_exc = e
            status = getattr(e, "status_code", None)
            msg = str(e).upper()
            now = time.time()
            if status == 418 or "418" in msg:
                _rest_block_until = max(_rest_block_until, now + REST_418_COOLDOWN)
                break
            if status == 429 or "429" in msg:
                _rest_block_until = max(_rest_block_until, now + REST_429_COOLDOWN)
                break
            if status == 403 or "403" in msg or "REQUEST BLOCKED" in msg:
                _rest_block_until = max(_rest_block_until, now + REST_403_COOLDOWN)
                break
            if attempt < retries:
                time.sleep(min(2.0, 0.5 * (2 ** attempt)))
    if last_exc is not None:
        raise last_exc
    raise RuntimeError(f"REST call failed: {tag}")


def price_live(symbol, max_stale=MARKPRICE_FRESH_SEC):
    now = time.time()
    cached = _ws_mark_price.get(symbol)
    if cached:
        px, ts = cached
        if px > 0 and (now - ts) < max_stale:
            return px

    old = _rest_price_cache.get(symbol)
    if old and (now - old[1]) < 1.0:
        return old[0]

    try:
        px = float(_rest_call(f"price_live_{symbol}", client.futures_symbol_ticker, symbol=symbol)["price"])
        _rest_price_cache[symbol] = (px, time.time())
        return px
    except Exception as e:
        _log_err(f"price_live_{symbol}", e)
        return 0.0


def calc_qty(symbol, price) -> Tuple[float, str]:
    rules = _symbol_rules.get(symbol, {"step": 0.001, "min_qty": 0.001, "min_notional": 5.0})
    raw = (ORDER_USDT * LEVERAGE) / price
    step = rules["step"]
    q = round(math.floor(raw / step + 1e-9) * step, 8)
    if q < rules["min_qty"] or q <= 0:
        return 0.0, f"qty {q} < minQty {rules['min_qty']}"
    if ENFORCE_MIN_NOTIONAL and q * price < rules["min_notional"]:
        return 0.0, f"notional {q * price:.2f} < minNotional {rules['min_notional']}"
    return q, ""


# ═══════════════════════════════════════════════════════════════════════════
#  ORDER BOOK ENGINE
# ═══════════════════════════════════════════════════════════════════════════

class OrderBookEngine:
    def __init__(self):
        self._cache = {}
        self._history = defaultdict(lambda: deque(maxlen=10))
        self._lock = threading.Lock()

    def update(self, symbol, bids_raw, asks_raw, ts=None):
        if ts is None:
            ts = time.time()
        try:
            bids = [(float(p), float(q)) for p, q in bids_raw]
            asks = [(float(p), float(q)) for p, q in asks_raw]
            bids.sort(key=lambda x: x[0], reverse=True)
            asks.sort(key=lambda x: x[0])
            bid_vol = sum(q for _, q in bids)
            ask_vol = sum(q for _, q in asks)
            imbalance = (bid_vol - ask_vol) / (bid_vol + ask_vol + 1e-9)
            best_bid = bids[0][0] if bids else 0.0
            best_ask = asks[0][0] if asks else 0.0
            with self._lock:
                self._cache[symbol] = {
                    "bids": bids, "asks": asks, "bid_vol": bid_vol, "ask_vol": ask_vol,
                    "imbalance": imbalance, "best_bid": best_bid, "best_ask": best_ask, "ts": ts,
                }
                self._history[symbol].append((ts, bid_vol, ask_vol, best_bid, best_ask))
        except Exception as e:
            _log_err("ob_update", e)

    def get_book(self, symbol):
        with self._lock:
            return self._cache.get(symbol)

    def get_imbalance(self, symbol) -> float:
        book = self.get_book(symbol)
        return book["imbalance"] if book else 0.0

    def check_walls(self, symbol, current_price, side):
        book = self.get_book(symbol)
        if not book:
            return False, "NO_DATA", 0.0, 0.0, 0.0
        if side == "LONG":
            levels, tot = book["asks"], book["ask_vol"]
            if not levels or tot <= 0:
                return False, "OK", 0.0, 0.0, 0.0
            avg = tot / len(levels)
            for px, q in levels:
                if px >= current_price and (px - current_price) / current_price <= WALL_PROXIMITY_PCT:
                    if q >= WALL_RATIO_THRESHOLD * avg or q >= WALL_DEPTH_PCT * tot:
                        return True, "SELL_WALL", px, q, q / avg if avg > 0 else 0.0
        else:
            levels, tot = book["bids"], book["bid_vol"]
            if not levels or tot <= 0:
                return False, "OK", 0.0, 0.0, 0.0
            avg = tot / len(levels)
            for px, q in levels:
                if px <= current_price and (current_price - px) / current_price <= WALL_PROXIMITY_PCT:
                    if q >= WALL_RATIO_THRESHOLD * avg or q >= WALL_DEPTH_PCT * tot:
                        return True, "BUY_WALL", px, q, q / avg if avg > 0 else 0.0
        return False, "OK", 0.0, 0.0, 0.0

    def detect_spoofing(self, symbol, side):
        with self._lock:
            hist = list(self._history.get(symbol, []))
        if len(hist) < 3:
            return False, ""
        curr_ts, curr_b, curr_a, _, _ = hist[-1]
        for ts, b_vol, a_vol, _, _ in hist[:-1]:
            if 0.5 <= (curr_ts - ts) <= 2.5:
                if side == "LONG" and b_vol > 0 and curr_b < b_vol * (1 - SPOOF_DROP_THRESHOLD):
                    return True, f"Bid liquidity pulled ({(1 - curr_b / b_vol) * 100:.0f}%)"
                if side == "SHORT" and a_vol > 0 and curr_a < a_vol * (1 - SPOOF_DROP_THRESHOLD):
                    return True, f"Ask liquidity pulled ({(1 - curr_a / a_vol) * 100:.0f}%)"
        return False, ""


order_book = OrderBookEngine()

# ═══════════════════════════════════════════════════════════════════════════
#  BTC MACRO ENGINE
# ═══════════════════════════════════════════════════════════════════════════

class BTCMacroEngine:
    def __init__(self):
        self.tick_history = deque(maxlen=300)
        self.breaker = {"active": False, "type": "NONE", "until": 0.0, "delta": 0.0}
        self.last_price = 0.0
        self.lock = threading.Lock()

    def update_tick(self, price, ts=None):
        if ts is None:
            ts = time.time()
        with self.lock:
            self.last_price = price
            self.tick_history.append((ts, price))
            cutoff = ts - BTC_WINDOW_SEC
            baseline = None
            for t_ts, t_px in self.tick_history:
                if t_ts >= cutoff:
                    baseline = t_px
                    break
            if baseline and baseline > 0:
                delta = (price - baseline) / baseline
                b = self.breaker
                if delta <= BTC_CRASH_THRESHOLD and not (b["active"] and b["type"] == "CRASH"):
                    self.breaker = {"active": True, "type": "CRASH", "until": ts + BTC_BREAKER_COOLDOWN, "delta": delta}
                    print(f"\n  🚨 [BTC FLASH CRASH] {delta*100:+.2f}% | LONG dikunci {BTC_BREAKER_COOLDOWN:.0f}s")
                elif delta >= BTC_PUMP_THRESHOLD and not (b["active"] and b["type"] == "PUMP"):
                    self.breaker = {"active": True, "type": "PUMP", "until": ts + BTC_BREAKER_COOLDOWN, "delta": delta}
                    print(f"\n  🚀 [BTC FLASH PUMP] {delta*100:+.2f}% | SHORT dikunci {BTC_BREAKER_COOLDOWN:.0f}s")

    def check_veto(self, side, now=None):
        if now is None:
            now = time.time()
        with self.lock:
            b = self.breaker
            if b["active"]:
                if now < b["until"]:
                    rem = b["until"] - now
                    if b["type"] == "CRASH" and side == "LONG":
                        return True, f"BTC Flash Crash ({rem:.0f}s)"
                    if b["type"] == "PUMP" and side == "SHORT":
                        return True, f"BTC Flash Pump ({rem:.0f}s)"
                else:
                    b["active"] = False
                    b["type"] = "NONE"
        return False, "OK"


btc_macro = BTCMacroEngine()

# ═══════════════════════════════════════════════════════════════════════════
#  ABSORPTION (memakai candle closed terakhir = iloc[-1])
# ═══════════════════════════════════════════════════════════════════════════

class AbsorptionDetector:
    @staticmethod
    def detect(df):
        if df is None or len(df) < 25:
            return False, False, ""
        row = df.iloc[-1]
        vol_spike = row.get("vr", 1.0) >= 1.4
        rng = row.get("rng", 1.0)
        low, high, close = row.get("low", 0.0), row.get("high", 0.0), row.get("close", 0.0)
        delta_ratio = row.get("delta_ratio", 0.0)
        buy_ratio = row.get("br", 0.5)
        lw, uw = row.get("lower_wick_ratio", 0.0), row.get("upper_wick_ratio", 0.0)

        heavy_seller = (delta_ratio < -0.20) or (buy_ratio < 0.40)
        bull = bool(vol_spike and heavy_seller and (lw >= 0.38 or close >= (low + 0.45 * rng)))
        heavy_buyer = (delta_ratio > 0.20) or (buy_ratio > 0.60)
        bear = bool(vol_spike and heavy_buyer and (uw >= 0.38 or close <= (high - 0.45 * rng)))

        details = []
        if bull: details.append("BullAbsorb")
        if bear: details.append("BearAbsorb")
        return bull, bear, " ".join(details)

# ═══════════════════════════════════════════════════════════════════════════
#  RISK
# ═══════════════════════════════════════════════════════════════════════════

class DynamicRiskManager:
    @staticmethod
    def calculate_levels(entry_price, execution_side, atr):
        atr_pct = (atr / entry_price) if entry_price > 0 else 0.004
        tp_pct = max(MIN_TP_PCT, min(MAX_TP_PCT, ATR_TP_MULTIPLIER * atr_pct))
        sl_pct = max(MIN_SL_PCT, min(MAX_SL_PCT, ATR_SL_MULTIPLIER * atr_pct))
        if execution_side == "LONG":
            tp_price, sl_price = entry_price * (1 + tp_pct), entry_price * (1 - sl_pct)
        else:
            tp_price, sl_price = entry_price * (1 - tp_pct), entry_price * (1 + sl_pct)
        return {"tp_pct": tp_pct, "sl_pct": sl_pct, "tp_price": tp_price, "sl_price": sl_price, "atr_pct": atr_pct}

# ═══════════════════════════════════════════════════════════════════════════
#  REGIME
# ═══════════════════════════════════════════════════════════════════════════

class MarketRegime:
    REGIME_TRENDING_BULL = "TRENDING_BULL"
    REGIME_TRENDING_BEAR = "TRENDING_BEAR"
    REGIME_RANGE         = "RANGE"
    REGIME_VOLATILE      = "VOLATILE"
    REGIME_EXHAUSTION    = "EXHAUSTION"

    @staticmethod
    def detect(df):
        if df is None or len(df) < 55:
            return MarketRegime.REGIME_RANGE, 0, 0
        row, prev = df.iloc[-1], df.iloc[-2]
        close = row["close"]
        e5, e9, e21, e50 = row["e5"], row["e9"], row["e21"], row["e50"]
        atr, atr_prev, adx = row["atr"], prev["atr"], row["adx"]
        m5, m5_prev = row["m5"], prev["m5"]
        if any(np.isnan(v) for v in (e5, e9, e21, e50, atr, adx)):
            return MarketRegime.REGIME_RANGE, 0, 0

        bull_stack = close > e5 > e9 > e21 > e50
        bear_stack = close < e5 < e9 < e21 < e50
        mild_bull = close > e9 > e21
        mild_bear = close < e9 < e21
        strong_trend = adx > 25
        very_strong = adx > 35
        atr_expand = (atr / atr_prev) > 1.2 if atr_prev and atr_prev > 0 else False
        atr_collapse = (atr / atr_prev) < 0.8 if atr_prev and atr_prev > 0 else False
        decel = (abs(m5) < abs(m5_prev)) if not (np.isnan(m5) or np.isnan(m5_prev)) else False

        if very_strong and bull_stack: return MarketRegime.REGIME_TRENDING_BULL, min(adx, 100), 1.0
        if very_strong and bear_stack: return MarketRegime.REGIME_TRENDING_BEAR, min(adx, 100), -1.0
        if strong_trend and (bull_stack or mild_bull): return MarketRegime.REGIME_TRENDING_BULL, min(adx, 80), 0.7
        if strong_trend and (bear_stack or mild_bear): return MarketRegime.REGIME_TRENDING_BEAR, min(adx, 80), -0.7
        if atr_expand and adx < 20: return MarketRegime.REGIME_VOLATILE, 50, 0
        if (atr_collapse and decel) or (20 < adx < 35 and decel):
            return MarketRegime.REGIME_EXHAUSTION, 40, (1 if m5 > 0 else -1)
        return MarketRegime.REGIME_RANGE, 30, 0

# ═══════════════════════════════════════════════════════════════════════════
#  SCORING (sinyal membawa key bobot: "key:label[w]")
# ═══════════════════════════════════════════════════════════════════════════

class SignalWeights:
    def __init__(self):
        self.weights = {
            "ema_bull_stack": 30, "ema_mild_bull": 20, "ema_weak_bull": 12,
            "mom_strong": 25, "mom_moderate": 15,
            "macd_cross_up": 22, "macd_strengthen": 15,
            "orderflow_delta_bull": 25, "orderflow_buy_high": 15,
            "absorption_bull": 35, "orderbook_imbalance_bull": 20,
            "rsi_bull_flow": 15, "rsi_extreme_ob": 10,
            "ema_bear_stack": 30, "ema_mild_bear": 20, "ema_weak_bear": 12,
            "mom_strong_neg": 25, "mom_moderate_neg": 15,
            "macd_cross_down": 22, "macd_strengthen_neg": 15,
            "orderflow_delta_bear": 25, "orderflow_sell_high": 15,
            "absorption_bear": 35, "orderbook_imbalance_bear": 20,
            "rsi_bear_flow": 15, "rsi_extreme_os": 10,
        }
        self.history = defaultdict(list)
        self.adaptive_enabled = True

    def record_outcome(self, signals, signal_won):
        for sig in signals:
            key = sig.split(":", 1)[0]
            if key in self.weights:
                self.history[key].append(1 if signal_won else 0)
                if len(self.history[key]) > LEARNING_WINDOW:
                    self.history[key] = self.history[key][-LEARNING_WINDOW:]

    def get_adjusted_weight(self, key):
        base = self.weights.get(key, 10)
        if not self.adaptive_enabled:
            return base
        hist = self.history.get(key, [])
        if len(hist) < MIN_TRADES_FOR_WEIGHT:
            return base
        return base * max(0.5, min(1.5, 0.5 + sum(hist) / len(hist)))


class SignalScorer:
    def __init__(self, signal_weights):
        self.weights = signal_weights

    def _add(self, sigs, key, label):
        w = self.weights.get_adjusted_weight(key)
        sigs.append(f"{key}:{label}[{w:.0f}]")
        return w

    def get_signal(self, df, symbol=None):
        if df is None or len(df) < 55:
            return None, 0, [], 0.0, "UNKNOWN", 0.0

        regime, _, bias = MarketRegime.detect(df)
        long_score, long_sigs = self._score_long(df, symbol)
        short_score, short_sigs = self._score_short(df, symbol)
        atr = df["atr"].iloc[-1]
        bull_absorb, bear_absorb, _ = AbsorptionDetector.detect(df)

        btc_reg = _btc_macro.get("regime", "UNKNOWN")
        if btc_reg == MarketRegime.REGIME_TRENDING_BULL:
            long_score += 10; long_sigs.append("btc_trend:BTC_BullTrend[+10]")
            short_score -= 20
        elif btc_reg == MarketRegime.REGIME_TRENDING_BEAR:
            short_score += 10; short_sigs.append("btc_trend:BTC_BearTrend[+10]")
            long_score -= 20

        best = max(long_score, short_score)
        if regime == MarketRegime.REGIME_TRENDING_BULL:
            if long_score >= MIN_SCORE: return "LONG", long_score, long_sigs, atr, regime, bias
            return None, best, [], atr, regime, bias
        if regime == MarketRegime.REGIME_TRENDING_BEAR:
            if short_score >= MIN_SCORE: return "SHORT", short_score, short_sigs, atr, regime, bias
            return None, best, [], atr, regime, bias
        if regime in (MarketRegime.REGIME_RANGE, MarketRegime.REGIME_EXHAUSTION):
            if bull_absorb and long_score >= MIN_SCORE:
                return "LONG", long_score, long_sigs, atr, f"{regime}_ABSORB", bias
            if bear_absorb and short_score >= MIN_SCORE:
                return "SHORT", short_score, short_sigs, atr, f"{regime}_ABSORB", bias
        _stats["regime_block"] += 1
        return None, best, [], atr, regime, bias

    def _score_long(self, df, symbol):
        row, prev, prev2 = df.iloc[-1], df.iloc[-2], df.iloc[-3]
        score, sigs = 0.0, []
        p, e5, e9, e21, e50 = row["close"], row["e5"], row["e9"], row["e21"], row["e50"]

        if p > e5 > e9 > e21 > e50: score += self._add(sigs, "ema_bull_stack", "EMA5↑")
        elif p > e5 > e9 > e21:     score += self._add(sigs, "ema_mild_bull", "EMA4↑")
        elif p > e5 > e9:           score += self._add(sigs, "ema_weak_bull", "EMA3↑")

        m5 = row["m5"]
        if m5 > 0.003:    score += self._add(sigs, "mom_strong", f"Mom+{m5*100:.1f}%")
        elif m5 > 0.0015: score += self._add(sigs, "mom_moderate", f"Mom+{m5*100:.1f}%")

        if prev["mh"] <= 0 and row["mh"] > 0: score += self._add(sigs, "macd_cross_up", "MACD_X↑")
        elif row["mh"] > 0 and row["mh"] > prev["mh"] > prev2["mh"]: score += self._add(sigs, "macd_strengthen", "MACD↑↑")

        dr, br = row.get("delta_ratio", 0.0), row.get("br", 0.5)
        if dr > 0.20:    score += self._add(sigs, "orderflow_delta_bull", f"ΔBuy+{dr*100:.0f}%")
        elif br > 0.55:  score += self._add(sigs, "orderflow_buy_high", f"TakerBuy{br*100:.0f}%")

        bull_abs, _, _ = AbsorptionDetector.detect(df)
        if bull_abs: score += self._add(sigs, "absorption_bull", "BullAbsorb")

        if symbol:
            imb = order_book.get_imbalance(symbol)
            if imb > IMBALANCE_STRONG_BULL:
                score += self._add(sigs, "orderbook_imbalance_bull", f"BAI+{imb*100:.0f}%")

        rsi = row["rsi"]
        if 48 <= rsi <= 68: score += self._add(sigs, "rsi_bull_flow", f"RSI{rsi:.0f}")
        elif rsi > 68:      score += self._add(sigs, "rsi_extreme_ob", f"RSI{rsi:.0f}OB")
        return score, sigs

    def _score_short(self, df, symbol):
        row, prev, prev2 = df.iloc[-1], df.iloc[-2], df.iloc[-3]
        score, sigs = 0.0, []
        p, e5, e9, e21, e50 = row["close"], row["e5"], row["e9"], row["e21"], row["e50"]

        if p < e5 < e9 < e21 < e50: score += self._add(sigs, "ema_bear_stack", "EMA5↓")
        elif p < e5 < e9 < e21:     score += self._add(sigs, "ema_mild_bear", "EMA4↓")
        elif p < e5 < e9:           score += self._add(sigs, "ema_weak_bear", "EMA3↓")

        m5 = row["m5"]
        if m5 < -0.003:    score += self._add(sigs, "mom_strong_neg", f"Mom{m5*100:.1f}%")
        elif m5 < -0.0015: score += self._add(sigs, "mom_moderate_neg", f"Mom{m5*100:.1f}%")

        if prev["mh"] >= 0 and row["mh"] < 0: score += self._add(sigs, "macd_cross_down", "MACD_X↓")
        elif row["mh"] < 0 and row["mh"] < prev["mh"] < prev2["mh"]: score += self._add(sigs, "macd_strengthen_neg", "MACD↓↓")

        dr, br = row.get("delta_ratio", 0.0), row.get("br", 0.5)
        if dr < -0.20:   score += self._add(sigs, "orderflow_delta_bear", f"ΔSell{dr*100:.0f}%")
        elif br < 0.45:  score += self._add(sigs, "orderflow_sell_high", f"TakerSell{(1-br)*100:.0f}%")

        _, bear_abs, _ = AbsorptionDetector.detect(df)
        if bear_abs: score += self._add(sigs, "absorption_bear", "BearAbsorb")

        if symbol:
            imb = order_book.get_imbalance(symbol)
            if imb < IMBALANCE_STRONG_BEAR:
                score += self._add(sigs, "orderbook_imbalance_bear", f"BAI{imb*100:.0f}%")

        rsi = row["rsi"]
        if 32 <= rsi <= 52: score += self._add(sigs, "rsi_bear_flow", f"RSI{rsi:.0f}")
        elif rsi < 32:      score += self._add(sigs, "rsi_extreme_os", f"RSI{rsi:.0f}OS")
        return score, sigs

# ═══════════════════════════════════════════════════════════════════════════
#  TRADE RECORD & LEARNING
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class TradeRecord:
    symbol: str
    direction: str
    entry_price: float
    exit_price: float
    pnl: float
    won: bool
    regime: str
    signals: List[str]
    score: float
    atr_entry: float
    hold_seconds: float
    exit_reason: str
    peak_pct: float
    inverted: bool = False
    timestamp: float = field(default_factory=time.time)


class LearningLayer:
    def __init__(self, signal_weights):
        self.signal_weights = signal_weights
        self.trades = []
        self.stats_by_regime = defaultdict(lambda: {"wins": 0, "losses": 0, "pnl": 0.0})

    def add_trade(self, trade, signal_won):
        """signal_won = apakah ARAH SINYAL ASLI benar (di mode INVERTED hasil dibalik)."""
        self.trades.append(trade)
        r = self.stats_by_regime[trade.regime]
        r["wins"] += 1 if trade.won else 0
        r["losses"] += 0 if trade.won else 1
        r["pnl"] += trade.pnl
        self.signal_weights.record_outcome(trade.signals, signal_won)
        if len(self.trades) > 1000:
            self.trades = self.trades[-500:]


signal_weights = SignalWeights()
scorer = SignalScorer(signal_weights)
learning = LearningLayer(signal_weights)

# ═══════════════════════════════════════════════════════════════════════════
#  INDIKATOR & KLINE (hanya candle CLOSED)
# ═══════════════════════════════════════════════════════════════════════════

def _compute_indicators(df):
    close, high, low = df["close"], df["high"], df["low"]
    volume = df["volume"].replace(0, 1e-9)
    tbbase = df["tbbase"]

    df["rsi"] = ta.momentum.RSIIndicator(close, 14).rsi()
    df["mh"]  = ta.trend.MACD(close, 12, 26, 9).macd_diff()
    df["e5"]  = ta.trend.EMAIndicator(close, 5).ema_indicator()
    df["e9"]  = ta.trend.EMAIndicator(close, 9).ema_indicator()
    df["e21"] = ta.trend.EMAIndicator(close, 21).ema_indicator()
    df["e50"] = ta.trend.EMAIndicator(close, 50).ema_indicator()
    df["atr"] = ta.volatility.AverageTrueRange(high, low, close, 14).average_true_range()
    df["adx"] = ta.trend.ADXIndicator(high, low, close, 14).adx()

    df["vm"] = volume.rolling(20).mean()
    df["vr"] = volume / df["vm"].replace(0, 1e-9)

    taker_buy = tbbase
    taker_sell = (volume - taker_buy).clip(lower=0)
    df["delta"] = taker_buy - taker_sell
    df["delta_ratio"] = df["delta"] / volume
    df["br"] = taker_buy / volume
    df["cvd"] = df["delta"].rolling(10).sum()

    df["rng"] = (high - low).replace(0, 1e-9)
    df["upper_wick"] = high - df[["close", "open"]].max(axis=1)
    df["lower_wick"] = df[["close", "open"]].min(axis=1) - low
    df["body"] = (close - df["open"]).abs()
    df["lower_wick_ratio"] = df["lower_wick"] / df["rng"]
    df["upper_wick_ratio"] = df["upper_wick"] / df["rng"]
    df["m5"] = (close - close.shift(5)) / close.shift(5)
    return df


def _bootstrap_klines(symbol, interval=KLINE_INTERVAL, limit=150):
    try:
        kl = _rest_call(f"bootstrap_{symbol}", client.futures_klines, symbol=symbol, interval=interval, limit=limit)
        df = pd.DataFrame(kl, columns=BASE_COLS)
        for c in ["open", "high", "low", "close", "volume", "qv", "tbbase", "tbquote"]:
            df[c] = df[c].astype(float)
        df["time"] = df["time"].astype("int64")
        df["ct"] = df["ct"].astype("int64")
        # buang candle yang masih berjalan -> df HANYA berisi candle closed
        df = df[df["ct"] < int(time.time() * 1000)].reset_index(drop=True)
        df = _compute_indicators(df)
        with _kline_lock:
            _kline_cache[symbol] = df
        return df
    except Exception as e:
        _log_err(f"bootstrap_{symbol}", e)
        return None


def _append_kline_from_ws(symbol, k):
    need_boot = False
    try:
        new_row = {
            "time": int(k["t"]), "open": float(k["o"]), "high": float(k["h"]), "low": float(k["l"]),
            "close": float(k["c"]), "volume": float(k["v"]), "ct": int(k["T"]), "qv": float(k.get("q", 0)),
            "trades": int(k.get("n", 0)), "tbbase": float(k.get("V", 0)), "tbquote": float(k.get("Q", 0)),
            "ignore": 0,
        }
        with _kline_lock:
            df = _kline_cache.get(symbol)
            if df is None:
                return
            base = df[BASE_COLS]
            last_t = int(base.iloc[-1]["time"]) if len(base) else 0
            if new_row["time"] < last_t:
                return
            if new_row["time"] == last_t:
                base = base.iloc[:-1]
            elif last_t and new_row["time"] - last_t > KLINE_MS * 1.5:
                need_boot = True   # ada candle yang terlewat -> ambil ulang dari REST
            if not need_boot:
                base = pd.concat([base, pd.DataFrame([new_row])], ignore_index=True)
                if len(base) > 300:
                    base = base.iloc[-300:].reset_index(drop=True)
                _kline_cache[symbol] = _compute_indicators(base)
        if need_boot:
            _log_warn(f"gap_{symbol}", "Gap kline terdeteksi, re-bootstrap")
            _bootstrap_klines(symbol)
    except Exception as e:
        _log_err(f"append_kline_{symbol}", e)


def ohlcv(symbol):
    with _kline_lock:
        df = _kline_cache.get(symbol)
    if df is not None:
        return df
    return _bootstrap_klines(symbol)

# ═══════════════════════════════════════════════════════════════════════════
#  KILL SWITCH
# ═══════════════════════════════════════════════════════════════════════════

def ks_check():
    k, now = _ks, time.time()
    if k["active"] and now >= k["resume"]:
        k["active"], k["consec"] = False, 0
    if k["active"]:
        return True, k["reason"]
    day = now - (now % 86400)
    if day > k["day_reset"]:
        k["daily"], k["day_reset"] = 0.0, day
    if k["daily"] <= DAILY_LOSS:
        k["active"], k["reason"], k["resume"] = True, f"daily({k['daily']:.2f})", day + 86400
        return True, k["reason"]
    if k["consec"] >= CONSEC_MAX:
        k["active"], k["reason"], k["resume"] = True, f"consec({k['consec']})", now + CONSEC_PAUSE
        return True, k["reason"]
    return False, ""


def ks_upd(pnl):
    _ks["daily"] += pnl
    _ks["consec"] = 0 if pnl >= 0 else _ks["consec"] + 1

# ═══════════════════════════════════════════════════════════════════════════
#  OPEN / CLOSE
# ═══════════════════════════════════════════════════════════════════════════

def live_open(c: dict):
    sym = c["sym"]
    execution_side = c["execution_side"]
    inverted = c["inverted"]

    with _lock:
        if sym in live_positions or len(live_positions) >= MAX_POSITIONS:
            return
        live_positions[sym] = {"_r": True}          # reservasi slot
        _last_entry_candle[sym] = c["candle_time"]

    try:
        price = price_live(sym) or c["price"]
        if price <= 0:
            raise ValueError("harga live tidak tersedia")
        entry = price * (1 + PAPER_SLIPPAGE) if execution_side == "LONG" else price * (1 - PAPER_SLIPPAGE)

        q_val, err = calc_qty(sym, entry)
        if q_val <= 0:
            raise ValueError(err or "quantity <= 0")

        risk = DynamicRiskManager.calculate_levels(entry, execution_side, c["atr"])
        pos = {
            "side": execution_side, "orig_signal": c["orig_direction"], "inverted": inverted,
            "entry": entry, "qty": q_val, "open_time": time.time(),
            "score": c["score"], "sigs": c["sigs"], "atr": c["atr"],
            "regime": c["regime"], "bias": c["bias"],
            "tp_pct": risk["tp_pct"], "sl_pct": risk["sl_pct"],
            "tp_price": risk["tp_price"], "sl_price": risk["sl_price"],
            "peak_pct": 0.0, "paper": True,
        }
        with _lock:
            live_positions[sym] = pos
    except Exception as e:
        _log_err(f"open_{sym}", e)
        with _lock:
            live_positions.pop(sym, None)           # lepas reservasi -> slot tidak nyangkut
        return

    d = "🟢" if execution_side == "LONG" else "🔴"
    print(
        f"\n  {d} [PAPER TRADE] {sym} EXEC:{execution_side} (Sinyal:{c['orig_direction']} | Mode:{'INVERTED' if inverted else 'NORMAL'}) "
        f"@{entry:.6g} | QTY:{q_val:.8g} | TP:{risk['tp_pct']*100:.2f}% SL:{risk['sl_pct']*100:.2f}% | Score:{c['score']:.0f} {c['regime']}"
    )
    _stats["trades"] += 1
    if any("Absorb" in s for s in c["sigs"]):
        _stats["absorb_entries"] += 1


def live_close(sym, reason, price=None):
    global is_logic_inverted

    with _lock:
        pos = live_positions.pop(sym, None)
    if pos is None or pos.get("_r"):
        return

    if price is None or price <= 0:
        price = price_live(sym) or price_live(sym, max_stale=120)
    if price <= 0:
        with _lock:
            live_positions[sym] = pos               # gagal dapat harga -> coba lagi nanti
        return

    side = pos["side"]
    if PAPER_SLIPPAGE > 0 and reason in ("SL", "TIME_LIMIT"):
        price = price * (1 - PAPER_SLIPPAGE) if side == "LONG" else price * (1 + PAPER_SLIPPAGE)

    entry, q_val = pos["entry"], pos["qty"]
    gross = (price - entry) * q_val if side == "LONG" else (entry - price) * q_val
    fee = (entry * q_val + price * q_val) * TAKER_FEE
    pnl = gross - fee
    pct = ((price - entry) / entry * 100) if side == "LONG" else ((entry - price) / entry * 100)
    hold = time.time() - pos["open_time"]
    won = pnl >= 0
    was_inverted = pos.get("inverted", False)

    # ── TOGGLE: minus -> balik mode, profit -> tetap ──
    if not won:
        is_logic_inverted = not is_logic_inverted
        print(f"  🔄 [LOGIC TOGGLE] Posisi MINUS ({reason}) -> mode sekarang: {mode_str()}")
    else:
        print(f"  ✅ [LOGIC STABLE] Posisi PROFIT ({reason}) -> mode tetap: {mode_str()}")
    save_state()

    print(
        f"  {'🟢' if won else '🔴'} [PAPER EXIT] {sym} {side} — {reason} | "
        f"{entry:.6g}→{price:.6g} ({pct:+.3f}%) hold:{hold:.0f}s | PnL:{pnl:+.5f}U (fee {fee:.5f})"
    )

    trade = TradeRecord(
        symbol=sym, direction=side, entry_price=entry, exit_price=price, pnl=pnl, won=won,
        regime=pos.get("regime", "UNKNOWN"), signals=pos.get("sigs", []), score=pos.get("score", 0),
        atr_entry=pos.get("atr", 0), hold_seconds=hold, exit_reason=reason,
        peak_pct=pos.get("peak_pct", 0.0), inverted=was_inverted,
    )
    # Arah sinyal asli "benar" jika: mode NORMAL & menang, ATAU mode INVERTED & kalah
    signal_won = (not won) if was_inverted else won
    learning.add_trade(trade, signal_won)

    _stats["pnl"] += pnl
    _stats["hist"].append(pnl)
    if _stats["pnl"] > _stats["ath_pnl"]:
        _stats["ath_pnl"] = _stats["pnl"]
    ks_upd(pnl)

    if won:
        _stats["wins"] += 1
        _stats["best"] = max(_stats["best"], pnl)
    else:
        _stats["losses"] += 1
        _stats["worst"] = min(_stats["worst"], pnl)

    if reason == "SL": _stats["hard_sl"] += 1
    elif reason == "TP": _stats["tp_exit"] += 1
    elif reason == "TIME_LIMIT": _stats["time_limit_exit"] += 1

    trade_log.append({"sym": sym, "side": side, "entry": round(entry, 7), "exit": round(price, 7),
                      "pnl": round(pnl, 5), "reason": reason, "hold": int(hold)})
    log_trade_csv({
        "time": time.strftime("%Y-%m-%d %H:%M:%S"), "symbol": sym,
        "mode_at_entry": "INVERTED" if was_inverted else "NORMAL",
        "signal": pos.get("orig_signal", ""), "exec_side": side,
        "entry": round(entry, 7), "exit": round(price, 7), "pnl": round(pnl, 5),
        "reason": reason, "hold_s": int(hold), "regime": pos.get("regime", ""),
        "score": round(pos.get("score", 0), 1), "peak_pct": round(pos.get("peak_pct", 0.0), 3),
        "next_mode": mode_str(),
    })
    print_inline()


def monitor_positions():
    for sym in list(live_positions.keys()):
        pos = live_positions.get(sym)
        if pos is None or pos.get("_r"):
            continue

        hold_time = time.time() - pos["open_time"]
        px = price_live(sym) or price_live(sym, max_stale=120)
        if px <= 0:
            _log_warn(f"noprice_{sym}", f"{sym}: harga tidak tersedia, posisi tetap dipantau")
            continue

        side, tp_px, sl_px, entry = pos["side"], pos["tp_price"], pos["sl_price"], pos["entry"]
        fav = ((px - entry) / entry * 100) if side == "LONG" else ((entry - px) / entry * 100)
        if fav > pos.get("peak_pct", 0.0):
            pos["peak_pct"] = fav

        if side == "LONG":
            if px >= tp_px: live_close(sym, "TP", tp_px); continue
            if px <= sl_px: live_close(sym, "SL", sl_px); continue
        else:
            if px <= tp_px: live_close(sym, "TP", tp_px); continue
            if px >= sl_px: live_close(sym, "SL", sl_px); continue

        if hold_time > MAX_HOLD_SECONDS:
            print(f"  ⏰ {sym}: MAX_HOLD {hold_time:.0f}s terlampaui — TIME_LIMIT")
            live_close(sym, "TIME_LIMIT", px)

# ═══════════════════════════════════════════════════════════════════════════
#  SCANNER
# ═══════════════════════════════════════════════════════════════════════════

def scan_one(sym):
    try:
        df = ohlcv(sym)
        if df is None or len(df) < 55:
            return None

        last = df.iloc[-1]
        candle_time = int(last["time"])
        age = (time.time() * 1000 - int(last["ct"])) / 1000.0

        if age > STALE_DATA_SEC:                     # WS mati / candle terlewat
            _log_warn(f"stale_{sym}", f"Data {sym} basi ({age:.0f}s) -> re-bootstrap")
            _bootstrap_klines(sym)
            return None
        if age > SIGNAL_MAX_AGE_SEC:                 # sinyal candle ini sudah terlalu tua
            return None
        if ONE_ENTRY_PER_CANDLE and _last_entry_candle.get(sym) == candle_time:
            return None

        px_candle, atr_val = float(last["close"]), float(last["atr"])
        if px_candle <= 0 or np.isnan(atr_val):
            return None

        orig_direction, score, sigs, _, regime, bias = scorer.get_signal(df, sym)
        if orig_direction not in ("LONG", "SHORT"):
            return None

        inverted = is_logic_inverted
        execution_side = ("SHORT" if orig_direction == "LONG" else "LONG") if inverted else orig_direction

        px_live = price_live(sym)
        if px_live <= 0:
            return None
        if abs(px_live - px_candle) / px_candle > SLIPPAGE_GUARD:
            return None

        vetoed, _ = btc_macro.check_veto(execution_side)
        if vetoed:
            _stats["btc_breaker_veto"] += 1
            return None

        has_wall, _, _, _, _ = order_book.check_walls(sym, px_live, execution_side)
        if has_wall:
            _stats["wall_veto"] += 1
            return None

        is_spoof, _ = order_book.detect_spoofing(sym, execution_side)
        if is_spoof:
            _stats["spoof_veto"] += 1
            return None

        imb = order_book.get_imbalance(sym)
        if execution_side == "LONG" and imb < -0.40: return None
        if execution_side == "SHORT" and imb > 0.40: return None

        return {
            "sym": sym, "orig_direction": orig_direction, "execution_side": execution_side,
            "inverted": inverted, "score": score, "sigs": sigs, "price": px_live,
            "atr": atr_val, "regime": regime, "bias": bias, "candle_time": candle_time,
        }
    except Exception as e:
        _log_err(f"scan_one_{sym}", e)
        return None


def scan_batch(syms):
    res = []
    futs = [_executor.submit(scan_one, s) for s in syms[:BATCH_SIZE]]
    try:
        for f in as_completed(futs, timeout=8):
            try:
                r = f.result()
                if r:
                    res.append(r)
            except Exception as e:
                _log_err("scan_batch", e)
    except Exception as e:
        _log_warn("scan_batch_timeout", f"{type(e).__name__}: {e}")
    return res


def t_monitor():
    while True:
        try:
            if live_positions:
                monitor_positions()
        except Exception as e:
            _log_err("t_monitor", e)
        time.sleep(MONITOR_INT)


def t_slot_filler(syms):
    scan_idx = 0
    n_bat = max(1, math.ceil(len(syms) / BATCH_SIZE))
    while True:
        try:
            if len(live_positions) >= MAX_POSITIONS or ks_check()[0]:
                time.sleep(SCAN_LOOP_SEC)
                continue

            with _lock:
                valid = [s for s in syms if s not in live_positions]
            bs = scan_idx * BATCH_SIZE
            batch = valid[bs:bs + BATCH_SIZE] or valid[:BATCH_SIZE]
            scan_idx = (scan_idx + 1) % n_bat
            if not batch:
                time.sleep(SCAN_LOOP_SEC)
                continue

            res = scan_batch(batch)
            if res:
                res.sort(key=lambda x: x["score"], reverse=True)
                for cand in res:
                    if len(live_positions) >= MAX_POSITIONS:
                        break
                    live_open(cand)
        except Exception as e:
            _log_err("t_slot_filler", e)
        time.sleep(SCAN_LOOP_SEC)


def t_macro():
    while True:
        try:
            df_btc = ohlcv("BTCUSDT")
            if df_btc is not None and len(df_btc) >= 55:
                regime, _, _ = MarketRegime.detect(df_btc)
                _macro["btc"] = regime
                _btc_macro["regime"] = regime
        except Exception as e:
            _log_err("t_macro", e)
        time.sleep(10)

# ═══════════════════════════════════════════════════════════════════════════
#  WEBSOCKET HANDLERS
# ═══════════════════════════════════════════════════════════════════════════

def handle_mark_price(msg):
    global _ws_last_msg_ts
    try:
        _ws_last_msg_ts = time.time()
        data = msg.get("data", msg) if isinstance(msg, dict) else msg
        arr = data if isinstance(data, list) else [data]
        now = time.time()
        for d in arr:
            if isinstance(d, dict) and d.get("s") and d.get("p"):
                _ws_mark_price[d["s"]] = (float(d["p"]), now)
    except Exception as e:
        _log_err("handle_mark_price", e)


def handle_kline_multiplex(msg):
    global _ws_last_msg_ts
    try:
        _ws_last_msg_ts = time.time()
        data = msg.get("data", msg)
        k = data.get("k")
        if k and k.get("x"):
            sym = data.get("s") or k.get("s")
            if sym:
                _append_kline_from_ws(sym, k)
    except Exception as e:
        _log_err("handle_kline_multiplex", e)


def handle_btc_aggtrade(msg):
    global _ws_last_msg_ts
    try:
        _ws_last_msg_ts = time.time()
        data = msg.get("data", msg)
        if isinstance(data, dict) and data.get("p"):
            ts = float(data.get("T", time.time() * 1000)) / 1000.0
            btc_macro.update_tick(float(data["p"]), ts)
    except Exception as e:
        _log_err("handle_btc_aggtrade", e)


def handle_depth_multiplex(msg):
    global _ws_last_msg_ts
    try:
        _ws_last_msg_ts = time.time()
        data = msg.get("data", msg)
        if isinstance(data, dict) and data.get("s"):
            order_book.update(data["s"], data.get("b", []), data.get("a", []))
    except Exception as e:
        _log_err("handle_depth_multiplex", e)

# ═══════════════════════════════════════════════════════════════════════════
#  DASHBOARD
# ═══════════════════════════════════════════════════════════════════════════

def print_inline():
    n = _stats["wins"] + _stats["losses"]
    wr = _stats["wins"] / n * 100 if n else 0
    print(f"       ┌ [PAPER v22.1 - MODE: {mode_str()}] {n}T WR:{wr:.0f}% W:{_stats['wins']} L:{_stats['losses']} PnL:{_stats['pnl']:+.4f}U")
    print(f"       └ TP:{_stats['tp_exit']} SL:{_stats['hard_sl']} TIME_LIMIT:{_stats['time_limit_exit']}")


def print_full():
    n = _stats["wins"] + _stats["losses"]
    wr = _stats["wins"] / n * 100 if n else 0
    print(f"\n  {'─'*72}")
    print(f"    🔔 DASHBOARD v22.1 (MODE: {mode_str()})")
    print(f"    🎯 {n}T WR:{wr:.0f}% W:{_stats['wins']} L:{_stats['losses']} | Best:{_stats['best']:+.4f} Worst:{_stats['worst']:+.4f}")
    print(f"    PnL Net:{_stats['pnl']:+.5f}U | ATH:{_stats['ath_pnl']:+.5f}U")
    print(f"    📈 Exit: TP:{_stats['tp_exit']} | SL:{_stats['hard_sl']} | TimeLimit:{_stats['time_limit_exit']}")
    print(f"    🛡️ Veto: BTC:{_stats['btc_breaker_veto']} Wall:{_stats['wall_veto']} Spoof:{_stats['spoof_veto']} RegimeBlock:{_stats['regime_block']}")
    print(f"  {'─'*72}")


def print_bep_info():
    fee = 2 * TAKER_FEE + 2 * PAPER_SLIPPAGE
    for label, tp, sl in (("min", MIN_TP_PCT, MIN_SL_PCT), ("max", MAX_TP_PCT, MAX_SL_PCT)):
        bep = (sl + fee) / (tp + sl) * 100
        print(f"  📐 RR {label}: TP {tp*100:.2f}% / SL {sl*100:.2f}% | biaya round-trip {fee*100:.2f}% | BEP win-rate ≈ {bep:.0f}%")

# ═══════════════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════════════

def run_bot():
    print("╔════════════════════════════════════════════════════════════════════╗")
    print("║  💎 BOT SCALPING v22.1 PAPER — DYNAMIC INVERT TOGGLE               ║")
    print("║  Minus (SL/TIME_LIMIT<0) -> toggle NORMAL/INVERTED                 ║")
    print("║  Profit (TP/TIME_LIMIT>=0) -> mode tetap                           ║")
    print("║  Margin 3 USDT | Max 1 posisi | Tanpa ban | Tanpa signal flip      ║")
    print("╚════════════════════════════════════════════════════════════════════╝")

    info = _rest_call("startup_exchange_info", client.futures_exchange_info, retries=1)
    valid = set()
    for s in info["symbols"]:
        if s["status"] != "TRADING":
            continue
        valid.add(s["symbol"])
        rules = {"step": 0.001, "min_qty": 0.001, "min_notional": 5.0}
        for f in s.get("filters", []):
            t = f.get("filterType")
            if t == "LOT_SIZE":
                rules["step"], rules["min_qty"] = float(f["stepSize"]), float(f["minQty"])
            elif t == "MIN_NOTIONAL":
                rules["min_notional"] = float(f.get("notional", f.get("minNotional", 5.0)))
        _symbol_rules[s["symbol"]] = rules

    syms = list(dict.fromkeys([s for s in SYMBOLS if s in valid]))
    if not syms:
        raise RuntimeError(f"Tidak ada simbol valid di SYMBOLS: {SYMBOLS}")
    print(f"  🎯 Simbol aktif: {', '.join(syms)}")
    for s in syms:
        print(f"     {s}: {_symbol_rules[s]}")
    print_bep_info()
    load_state()

    # Bootstrap semua kline (termasuk BTC untuk makro) SEBELUM websocket & thread jalan
    kline_syms = list(dict.fromkeys(syms + ["BTCUSDT"]))
    for s in kline_syms:
        if _bootstrap_klines(s) is None:
            raise RuntimeError(f"Gagal bootstrap kline {s}")

    twm.start()
    twm.start_futures_multiplex_socket(callback=handle_mark_price, streams=[f"{s.lower()}@markPrice@1s" for s in kline_syms])
    twm.start_futures_multiplex_socket(callback=handle_kline_multiplex, streams=[f"{s.lower()}@kline_5m" for s in kline_syms])
    twm.start_futures_multiplex_socket(callback=handle_btc_aggtrade, streams=["btcusdt@aggTrade"])
    for i in range(0, len(syms), DEPTH_SOCKET_CHUNK):
        chunk = syms[i:i + DEPTH_SOCKET_CHUNK]
        twm.start_futures_multiplex_socket(callback=handle_depth_multiplex, streams=[f"{s.lower()}@depth10" for s in chunk])
        time.sleep(0.15)

    threading.Thread(target=t_monitor, daemon=True).start()
    threading.Thread(target=t_slot_filler, args=(syms,), daemon=True).start()
    threading.Thread(target=t_macro, daemon=True).start()
    time.sleep(2)

    cycle = 0
    while True:
        cycle += 1
        slots = MAX_POSITIONS - len(live_positions)
        if cycle % 5 == 1:
            print(f"\n{'═'*68}")
            print(f"  #{cycle} {time.strftime('%H:%M:%S')} BTC_5M:{_macro['btc']} Mode:[{mode_str()}] Pos:({len(live_positions)}/{MAX_POSITIONS}) PnL:{_stats['pnl']:+.4f}U")
            k = ks_check()
            if k[0]: print(f"  🚨 KS:{k[1]}")
            elif slots == 0: print("  ✅ Slot penuh — memantau posisi")
            else: print(f"  🔍 Slot kosong — menunggu sinyal candle baru (Mode: {mode_str()})")
        if time.time() - _ws_last_msg_ts > WS_STALE_SEC:
            _log_warn("ws_stale", f"Tidak ada pesan WebSocket {time.time() - _ws_last_msg_ts:.0f}s — cek koneksi")
        if cycle % 30 == 0:
            print_full()
        time.sleep(2.0)


if __name__ == "__main__":
    try:
        run_bot()
    except KeyboardInterrupt:
        print("\n🛑 Bot dihentikan manual.")
        save_state()
    except Exception as e:
        print(f"\n❌ BOT STOPPED: {type(e).__name__}: {e}")