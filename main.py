import os
import json
import time
import math
import threading
import logging
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import pandas as pd
import requests
from flask import Flask, jsonify
# ============================================================
# CONFIGURATION
# ============================================================
SYMBOLS = ["BTCUSD", "XAUUSD", "EURUSD", "GBPUSD"]
TIMEFRAMES = {
    "M15": "15m",
    "M5": "5m",
    "M1": "1m",
}
BIQUOTE_BASE_URL = os.getenv(
    "BIQUOTE_BASE_URL",
    "https://biquote.io/api"
).rstrip("/")
TELEGRAM_BOT_TOKEN = os.getenv(
    "TELEGRAM_BOT_TOKEN",
    os.getenv("TELEGRAM_TOKEN", "")
).strip()
TELEGRAM_CHAT_ID = os.getenv(
    "TELEGRAM_CHAT_ID",
    ""
).strip()
TELEGRAM_ADMIN_ID = os.getenv(
    "TELEGRAM_ADMIN_ID",
    os.getenv("TELEGRAM_OWNER_ID", "")
).strip()
PORT = int(os.getenv("PORT", "8080"))
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL", "60"))
DATA_TIMEOUT = 5
ATR_PERIOD = 14
# Anti-bruit ATR
ATR_MIN_RATIO = float(os.getenv("ATR_MIN_RATIO", "0.15"))
# CHoCH minimum de force
CHOCH_MIN_STRENGTH = 60.0
# Tolérance de retest de polarité
POLARITY_TOLERANCE_ATR = 0.20
# Sécurité TP3
TP3_SAFETY_MARGIN = 0.0002
# RR minimum
MIN_RR = 3.0
DATA_DIR = os.getenv("DATA_DIR", ".")
PENDING_FILE = os.path.join(DATA_DIR, "pending_opportunities.json")
ACTIVE_FILE = os.path.join(DATA_DIR, "active_trades.json")
HISTORY_FILE = os.path.join(DATA_DIR, "trade_history.json")
PROCESSED_FILE = os.path.join(DATA_DIR, "processed_signals.json")
REPORT_FILE = os.path.join(DATA_DIR, "weekly_reports.json")
app = Flask(__name__)
json_lock = threading.RLock()
state_lock = threading.RLock()
executor = ThreadPoolExecutor(
    max_workers=len(SYMBOLS)
)
session = requests.Session()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("NOVA")
# ============================================================
# JSON UTILITIES
# ============================================================
def ensure_file(path, default):
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    if not os.path.exists(path):
        atomic_write_json(path, default)
def atomic_write_json(path, data):
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    temp_path = f"{path}.tmp"
    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(
            data,
            f,
            ensure_ascii=False,
            indent=2,
            default=str
        )
    os.replace(temp_path, path)
def read_json(path, default):
    ensure_file(path, default)
    with json_lock:
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data
        except Exception:
            return default
def append_json_list(path, item):
    with json_lock:
        data = read_json(path, [])
        if not isinstance(data, list):
            data = []
        data.append(item)
        atomic_write_json(path, data)
def upsert_json_list(path, key, value, item):
    with json_lock:
        data = read_json(path, [])
        if not isinstance(data, list):
            data = []
        replaced = False
        for index, existing in enumerate(data):
            if existing.get(key) == value:
                data[index] = item
                replaced = True
                break
        if not replaced:
            data.append(item)
        atomic_write_json(path, data)
# ============================================================
# INITIAL STATE
# ============================================================
ensure_file(PENDING_FILE, [])
ensure_file(ACTIVE_FILE, [])
ensure_file(HISTORY_FILE, [])
ensure_file(PROCESSED_FILE, [])
ensure_file(REPORT_FILE, [])
# ============================================================
# TIME
# ============================================================
def utc_now():
    return datetime.now(timezone.utc)
def iso_now():
    return utc_now().isoformat()
# ============================================================
# NUMERIC UTILITIES
# ============================================================
def safe_float(value, default=np.nan):
    try:
        return float(value)
    except Exception:
        return default
def normalize_symbol(symbol):
    return symbol.replace("/", "").upper()
# ============================================================
# DATA NORMALIZATION
# ============================================================
def normalize_ohlcv(data):
    if data is None:
        return None
    try:
        if isinstance(data, dict):
            for key in ("data", "candles", "results", "result"):
                if key in data:
                    data = data[key]
                    break
        if not isinstance(data, (list, tuple)):
            return None
        if not data:
            return None
        first = data[0]
        if isinstance(first, dict):
            df = pd.DataFrame(data)
            rename_map = {}
            for column in df.columns:
                normalized = str(column).lower().strip()
                if normalized in ("timestamp", "time", "date", "datetime"):
                    rename_map[column] = "timestamp"
                elif normalized in ("open", "o"):
                    rename_map[column] = "open"
                elif normalized in ("high", "h"):
                    rename_map[column] = "high"
                elif normalized in ("low", "l"):
                    rename_map[column] = "low"
                elif normalized in ("close", "c"):
                    rename_map[column] = "close"
                elif normalized in ("volume", "vol", "v"):
                    rename_map[column] = "volume"
            df = df.rename(columns=rename_map)
        else:
            df = pd.DataFrame(data)
            if df.shape[1] < 5:
                return None
            if df.shape[1] >= 6:
                df = df.iloc[:, :6]
                df.columns = [
                    "timestamp",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume"
                ]
            else:
                df = df.iloc[:, :5]
                df.columns = [
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume"
                ]
        if "timestamp" not in df.columns:
            if isinstance(df.index, pd.DatetimeIndex):
                df["timestamp"] = df.index
            else:
                return None
        required = [
            "open",
            "high",
            "low",
            "close",
            "volume"
        ]
        for column in required:
            if column not in df.columns:
                return None
            df[column] = pd.to_numeric(
                df[column],
                errors="coerce"
            )
        df["timestamp"] = pd.to_datetime(
            df["timestamp"],
            errors="coerce",
            utc=True
        )
        df = df.dropna(
            subset=[
                "timestamp",
                "open",
                "high",
                "low",
                "close"
            ]
        )
        df = df.sort_values("timestamp")
        df = df.drop_duplicates("timestamp")
        df = df.set_index("timestamp")
        df["volume"] = df["volume"].fillna(0.0)
        return df[
            ["open", "high", "low", "close", "volume"]
        ].copy()
    except Exception as exc:
        logger.warning(
            "Normalisation OHLCV impossible: %s",
            exc
        )
        return None
# ============================================================
# BIQUOTE
# ============================================================
def fetch_biquote(symbol, timeframe, limit=500):
    url = f"{BIQUOTE_BASE_URL}/ohlcv"
    params = {
        "symbol": symbol,
        "timeframe": timeframe,
        "limit": max(500, limit)
    }
    try:
        response = session.get(
            url,
            params=params,
            timeout=DATA_TIMEOUT
        )
        response.raise_for_status()
        df = normalize_ohlcv(
            response.json()
        )
        if df is not None and len(df) >= 50:
            logger.info(
                "[BiQuote OK] %s %s | %s candles",
                symbol,
                timeframe,
                len(df)
            )
            return df.tail(limit)
    except Exception as exc:
        logger.warning(
            "[BiQuote FAIL] %s %s | %s",
            symbol,
            timeframe,
            exc
        )
    return None
# ============================================================
# KRAKEN
# ============================================================
def fetch_kraken(symbol, timeframe, limit=500):
    if symbol != "BTCUSD":
        return None
    interval_map = {
        "1m": 1,
        "5m": 5,
        "15m": 15
    }
    interval = interval_map.get(timeframe)
    if interval is None:
        return None
    try:
        response = session.get(
            "https://api.kraken.com/0/public/OHLC",
            params={
                "pair": "XBTUSD",
                "interval": interval
            },
            timeout=DATA_TIMEOUT
        )
        response.raise_for_status()
        payload = response.json()
        result = payload.get("result", {})
        rows = result.get("XXBTZUSD")
        if not rows:
            for value in result.values():
                if isinstance(value, list):
                    rows = value
                    break
        if not rows:
            return None
        converted = []
        for row in rows:
            if len(row) < 7:
                continue
            converted.append([
                row[0],
                row[1],
                row[2],
                row[3],
                row[4],
                row[6]
            ])
        df = normalize_ohlcv(
            converted
        )
        if df is not None:
            logger.info(
                "[Kraken OK] %s %s | %s candles",
                symbol,
                timeframe,
                len(df)
            )
            return df.tail(limit)
    except Exception as exc:
        logger.warning(
            "[Kraken FAIL] %s %s | %s",
            symbol,
            timeframe,
            exc
        )
    return None
# ============================================================
# TIINGO
# ============================================================
def fetch_tiingo(symbol, timeframe, limit=500):
    token = os.getenv("TIINGO_API_KEY", "").strip()
    if not token:
        return None
    mapping = {
        "XAUUSD": "XAUUSD",
        "EURUSD": "EURUSD",
        "GBPUSD": "GBPUSD"
    }
    ticker = mapping.get(symbol)
    if not ticker:
        return None
    try:
        url = (
            "https://api.tiingo.com/tiingo/fx/"
            f"{ticker}/prices"
        )
        response = session.get(
            url,
            params={
                "token": token,
                "resampleFreq": timeframe
            },
            timeout=DATA_TIMEOUT
        )
        response.raise_for_status()
        payload = response.json()
        rows = []
        for row in payload:
            rows.append({
                "timestamp": row.get("date"),
                "open": row.get("open"),
                "high": row.get("high"),
                "low": row.get("low"),
                "close": row.get("close"),
                "volume": row.get("volume", 0)
            })
        df = normalize_ohlcv(rows)
        if df is not None:
            logger.info(
                "[Tiingo OK] %s %s | %s candles",
                symbol,
                timeframe,
                len(df)
            )
            return df.tail(limit)
    except Exception as exc:
        logger.warning(
            "[Tiingo FAIL] %s %s | %s",
            symbol,
            timeframe,
            exc
        )
    return None
# ============================================================
# YAHOO DIRECT HTTP
# ============================================================
def yahoo_symbol(symbol):
    return {
        "BTCUSD": "BTC-USD",
        "XAUUSD": "GC=F",
        "EURUSD": "EURUSD=X",
        "GBPUSD": "GBPUSD=X"
    }.get(symbol)
def yahoo_interval(timeframe):
    return {
        "1m": "1m",
        "5m": "5m",
        "15m": "15m"
    }.get(timeframe)
def fetch_yahoo(symbol, timeframe, limit=500):
    ticker = yahoo_symbol(symbol)
    interval = yahoo_interval(timeframe)
    if not ticker or not interval:
        return None
    try:
        url = (
            "https://query1.finance.yahoo.com/v8/finance/"
            f"chart/{ticker}"
        )
        response = session.get(
            url,
            params={
                "range": "5d",
                "interval": interval
            },
            timeout=DATA_TIMEOUT
        )
        response.raise_for_status()
        payload = response.json()
        result = (
            payload
            .get("chart", {})
            .get("result")
        )
        if not result:
            return None
        result = result[0]
        timestamps = result.get("timestamp", [])
        quote = (
            result
            .get("indicators", {})
            .get("quote", [{}])[0]
        )
        rows = []
        for i, timestamp in enumerate(timestamps):
            try:
                rows.append({
                    "timestamp": pd.to_datetime(
                        timestamp,
                        unit="s",
                        utc=True
                    ),
                    "open": quote["open"][i],
                    "high": quote["high"][i],
                    "low": quote["low"][i],
                    "close": quote["close"][i],
                    "volume": (
                        quote.get("volume", [0] * len(timestamps))[i]
                    )
                })
            except Exception:
                continue
        df = normalize_ohlcv(rows)
        if df is not None:
            logger.info(
                "[Yahoo OK] %s %s | %s candles",
                symbol,
                timeframe,
                len(df)
            )
            return df.tail(limit)
    except Exception as exc:
        logger.warning(
            "[Yahoo FAIL] %s %s | %s",
            symbol,
            timeframe,
            exc
        )
    return None
# ============================================================
# SAFE MARKET DATA
# ============================================================
def fetch_market_data_safe(
    symbol,
    timeframe,
    limit=500
):
    df = fetch_biquote(
        symbol,
        timeframe,
        limit
    )
    if df is not None and len(df) >= 50:
        return df
    if symbol == "BTCUSD":
        df = fetch_kraken(
            symbol,
            timeframe,
            limit
        )
    else:
        df = fetch_tiingo(
            symbol,
            timeframe,
            limit
        )
    if df is not None and len(df) >= 50:
        return df
    df = fetch_yahoo(
        symbol,
        timeframe,
        limit
    )
    if df is not None and len(df) >= 50:
        return df
    logger.error(
        "[DATA FAIL] %s %s | aucune source disponible",
        symbol,
        timeframe
    )
    return None
# ============================================================
# INDICATORS
# ============================================================
def calculate_atr(df, period=ATR_PERIOD):
    high = df["high"]
    low = df["low"]
    close = df["close"]
    previous_close = close.shift(1)
    tr = pd.concat(
        [
            high - low,
            (high - previous_close).abs(),
            (low - previous_close).abs()
        ],
        axis=1
    ).max(axis=1)
    return tr.rolling(
        period,
        min_periods=period
    ).mean()
def add_indicators(df):
    df = df.copy()
    df["atr"] = calculate_atr(
        df,
        ATR_PERIOD
    )
    return df
# ============================================================
# FILTRE ATR ANTI-BRUIT
# ============================================================
def atr_noise_filter(
    df,
    candle_index=-1,
    minimum_ratio=ATR_MIN_RATIO
):
    if df is None or len(df) < ATR_PERIOD + 2:
        return False
    try:
        candle = df.iloc[candle_index]
        atr = safe_float(
            candle["atr"]
        )
        candle_range = (
            safe_float(candle["high"])
            -
            safe_float(candle["low"])
        )
        if not np.isfinite(atr):
            return False
        if atr <= 0:
            return False
        if candle_range <= 0:
            return False
        ratio = candle_range / atr
        return ratio >= minimum_ratio
    except Exception:
        return False
def atr_displacement_strength(
    df,
    level,
    direction,
    candle_index=-1
):
    if df is None or len(df) < ATR_PERIOD + 2:
        return 0.0
    candle = df.iloc[candle_index]
    atr = safe_float(
        candle["atr"]
    )
    close = safe_float(
        candle["close"]
    )
    if not np.isfinite(atr) or atr <= 0:
        return 0.0
    if direction == "BUY":
        displacement = close - level
    else:
        displacement = level - close
    if displacement <= 0:
        return 0.0
    return min(
        100.0,
        (displacement / atr) * 100.0
    )
# ============================================================
# CLÔTURE -2
# ============================================================
def close_minus_2_confirmation(
    df,
    level,
    direction
):
    """
    Confirmation basée sur la clôture de la bougie -2.
    BUY:
        la bougie -2 doit clôturer au-dessus du niveau.
    SELL:
        la bougie -2 doit clôturer sous le niveau.
    La dernière bougie ne doit pas invalider cette confirmation.
    """
    if df is None or len(df) < 5:
        return False
    try:
        candle_minus_2 = df.iloc[-2]
        last_candle = df.iloc[-1]
        close_minus_2 = safe_float(
            candle_minus_2["close"]
        )
        last_close = safe_float(
            last_candle["close"]
        )
        if direction == "BUY":
            return (
                close_minus_2 > level
                and
                last_close >= level
            )
        if direction == "SELL":
            return (
                close_minus_2 < level
                and
                last_close <= level
            )
    except Exception:
        return False
    return False
# ============================================================
# SWINGS
# ============================================================
def detect_swings(
    df,
    left=2,
    right=2
):
    highs = []
    lows = []
    if df is None:
        return highs, lows
    if len(df) < left + right + 5:
        return highs, lows
    high_values = df["high"].values
    low_values = df["low"].values
    index_values = df.index
    for i in range(
        left,
        len(df) - right
    ):
        current_high = high_values[i]
        current_low = low_values[i]
        left_highs = high_values[
            i - left:i
        ]
        right_highs = high_values[
            i + 1:i + right + 1
        ]
        left_lows = low_values[
            i - left:i
        ]
        right_lows = low_values[
            i + 1:i + right + 1
        ]
        if current_high >= max(left_highs) and \
                current_high >= max(right_highs):
            highs.append({
                "index": i,
                "time": str(index_values[i]),
                "price": float(current_high)
            })
        if current_low <= min(left_lows) and \
                current_low <= min(right_lows):
            lows.append({
                "index": i,
                "time": str(index_values[i]),
                "price": float(current_low)
            })
    return highs, lows
# ============================================================
# M15 POLARITY
# ============================================================
def detect_m15_polarity(
    symbol,
    df
):
    if df is None or len(df) < 100:
        return None
    df = add_indicators(df)
    swings_high, swings_low = detect_swings(
        df,
        left=3,
        right=3
    )
    if not swings_high and not swings_low:
        return None
    last_close = safe_float(
        df.iloc[-1]["close"]
    )
    candidates = []
    for swing in swings_high:
        price = swing["price"]
        if last_close > price:
            candidates.append({
                "direction": "BUY",
                "polarity_type": "RESISTANCE_TO_SUPPORT",
                "trigger_price": price,
                "macro_target": max(
                    price,
                    last_close
                ),
                "structure_time": swing["time"],
                "strength": last_close - price
            })
    for swing in swings_low:
        price = swing["price"]
        if last_close < price:
            candidates.append({
                "direction": "SELL",
                "polarity_type": "SUPPORT_TO_RESISTANCE",
                "trigger_price": price,
                "macro_target": min(
                    price,
                    last_close
                ),
                "structure_time": swing["time"],
                "strength": price - last_close
            })
    if not candidates:
        return None
    candidates.sort(
        key=lambda x: x["strength"],
        reverse=True
    )
    opportunity = candidates[0]
    atr = safe_float(
        df.iloc[-1]["atr"]
    )
    if np.isfinite(atr) and atr > 0:
        if opportunity["direction"] == "BUY":
            macro_target = max(
                opportunity["macro_target"],
                last_close + atr
            )
        else:
            macro_target = min(
                opportunity["macro_target"],
                last_close - atr
            )
        opportunity["macro_target"] = macro_target
    opportunity.update({
        "symbol": symbol,
        "timeframe": "M15",
        "status": "WAITING_M5_RETEST",
        "created_at": iso_now()
    })
    return opportunity
# ============================================================
# M5 POLARITY RETEST / LIQUIDITY SWEEP
# ============================================================
def detect_m5_retest(
    opportunity,
    df
):
    if df is None or len(df) < 50:
        return None
    df = add_indicators(df)
    direction = opportunity["direction"]
    level = safe_float(
        opportunity["trigger_price"]
    )
    last = df.iloc[-1]
    atr = safe_float(
        last["atr"]
    )
    if not np.isfinite(atr) or atr <= 0:
        return None
    tolerance = atr * POLARITY_TOLERANCE_ATR
    high = safe_float(last["high"])
    low = safe_float(last["low"])
    close = safe_float(last["close"])
    if not atr_noise_filter(df):
        return None
    if direction == "BUY":
        swept = low <= level + tolerance
        closed_back_inside = close >= level
        if not (
            swept
            and
            closed_back_inside
        ):
            return None
        if not close_minus_2_confirmation(
            df,
            level,
            direction
        ):
            return None
        manipulation_wick = low
    else:
        swept = high >= level - tolerance
        closed_back_inside = close <= level
        if not (
            swept
            and
            closed_back_inside
        ):
            return None
        if not close_minus_2_confirmation(
            df,
            level,
            direction
        ):
            return None
        manipulation_wick = high
    result = dict(opportunity)
    result.update({
        "status": "WAITING_M1_CHOCH",
        "m5_retest_time": str(df.index[-1]),
        "m5_liquidity_extreme": manipulation_wick,
        "m5_atr": atr
    })
    return result
# ============================================================
# M1 CHOCH
# ============================================================
def detect_m1_choch(
    opportunity,
    df
):
    if df is None or len(df) < 80:
        return None
    df = add_indicators(df)
    direction = opportunity["direction"]
    swings_high, swings_low = detect_swings(
        df,
        left=2,
        right=2
    )
    if direction == "BUY":
        if not swings_high:
            return None
        reference = swings_high[-1]["price"]
        confirmation = (
            safe_float(df.iloc[-1]["close"])
            >
            reference
        )
        if not confirmation:
            return None
        if not close_minus_2_confirmation(
            df,
            reference,
            "BUY"
        ):
            return None
        strength = atr_displacement_strength(
            df,
            reference,
            "BUY"
        )
    else:
        if not swings_low:
            return None
        reference = swings_low[-1]["price"]
        confirmation = (
            safe_float(df.iloc[-1]["close"])
            <
            reference
        )
        if not confirmation:
            return None
        if not close_minus_2_confirmation(
            df,
            reference,
            "SELL"
        ):
            return None
        strength = atr_displacement_strength(
            df,
            reference,
            "SELL"
        )
    # ========================================================
    # FILTRE CHOCH >= 60 %
    # ========================================================
    if strength < CHOCH_MIN_STRENGTH:
        logger.info(
            "[CHOCH REJECT] %s | force %.2f%% < %.2f%%",
            opportunity["symbol"],
            strength,
            CHOCH_MIN_STRENGTH
        )
        return None
    # ========================================================
    # FILTRE ATR ANTI-BRUIT
    # ========================================================
    if not atr_noise_filter(df):
        logger.info(
            "[ATR REJECT] %s | CHoCH trop faible",
            opportunity["symbol"]
        )
        return None
    result = dict(opportunity)
    result.update({
        "status": "CHOCH_CONFIRMED",
        "choch_time": str(df.index[-1]),
        "choch_level": reference,
        "choch_strength": round(
            strength,
            2
        ),
        "m1_atr": safe_float(
            df.iloc[-1]["atr"]
        )
    })
    return result
# ============================================================
# M1 BOS
# ============================================================
def detect_m1_bos(
    opportunity,
    df
):
    if df is None or len(df) < 30:
        return None
    direction = opportunity["direction"]
    level = safe_float(
        opportunity["choch_level"]
    )
    last_close = safe_float(
        df.iloc[-1]["close"]
    )
    if direction == "BUY":
        if last_close <= level:
            return None
    else:
        if last_close >= level:
            return None
    if not close_minus_2_confirmation(
        df,
        level,
        direction
    ):
        return None
    if not atr_noise_filter(
        add_indicators(df)
    ):
        return None
    result = dict(opportunity)
    result.update({
        "status": "BOS_CONFIRMED",
        "bos_time": str(df.index[-1]),
        "bos_level": level
    })
    return result
# ============================================================
# FVG
# ============================================================
def find_recent_fvg(
    df,
    direction,
    lookback=20
):
    if df is None or len(df) < 5:
        return None
    start = max(
        2,
        len(df) - lookback
    )
    for i in range(
        len(df) - 2,
        start - 1,
        -1
    ):
        first = df.iloc[i - 2]
        middle = df.iloc[i - 1]
        third = df.iloc[i]
        if direction == "BUY":
            if safe_float(third["low"]) > \
                    safe_float(first["high"]):
                return {
                    "type": "FVG",
                    "direction": "BUY",
                    "low": safe_float(first["high"]),
                    "high": safe_float(third["low"]),
                    "entry": (
                        safe_float(first["high"])
                        +
                        safe_float(third["low"])
                    ) / 2
                }
        else:
            if safe_float(third["high"]) < \
                    safe_float(first["low"]):
                return {
                    "type": "FVG",
                    "direction": "SELL",
                    "low": safe_float(third["high"]),
                    "high": safe_float(first["low"]),
                    "entry": (
                        safe_float(third["high"])
                        +
                        safe_float(first["low"])
                    ) / 2
                }
    return None
# ============================================================
# ORDER BLOCK
# ============================================================
def find_recent_ob(
    df,
    direction,
    lookback=20
):
    if df is None or len(df) < 5:
        return None
    start = max(
        1,
        len(df) - lookback
    )
    for i in range(
        len(df) - 2,
        start - 1,
        -1
    ):
        candle = df.iloc[i]
        open_price = safe_float(
            candle["open"]
        )
        close_price = safe_float(
            candle["close"]
        )
        high = safe_float(
            candle["high"]
        )
        low = safe_float(
            candle["low"]
        )
        if direction == "BUY":
            if close_price < open_price:
                return {
                    "type": "OB",
                    "direction": "BUY",
                    "low": low,
                    "high": high,
                    "entry": open_price
                }
        else:
            if close_price > open_price:
                return {
                    "type": "OB",
                    "direction": "SELL",
                    "low": low,
                    "high": high,
                    "entry": open_price
                }
    return None
# ============================================================
# ENTRY
# ============================================================
def select_entry(
    opportunity,
    df
):
    direction = opportunity["direction"]
    fvg = find_recent_fvg(
        df,
        direction
    )
    if fvg:
        entry = fvg["entry"]
        return entry, fvg
    ob = find_recent_ob(
        df,
        direction
    )
    if ob:
        return ob["entry"], ob
    return None, None
# ============================================================
# SL M1 MANIPULATION WICK
# ============================================================
def calculate_m1_sl(
    opportunity,
    df
):
    if df is None or len(df) < 5:
        return None
    direction = opportunity["direction"]
    recent = df.tail(10)
    if direction == "BUY":
        return float(
            recent["low"].min()
        )
    return float(
        recent["high"].max()
    )
# ============================================================
# TP CALCULATION
# ============================================================
def calculate_trade_levels(
    opportunity,
    entry,
    sl
):
    direction = opportunity["direction"]
    if direction == "BUY":
        risk = entry - sl
        if risk <= 0:
            return None
        tp1 = entry + risk
        macro_target = safe_float(
            opportunity["macro_target"]
        )
        tp3 = (
            macro_target
            *
            (1.0 - TP3_SAFETY_MARGIN)
        )
        if tp3 <= tp1:
            return None
        tp2 = (
            tp1 + tp3
        ) / 2
    else:
        risk = sl - entry
        if risk <= 0:
            return None
        tp1 = entry - risk
        macro_target = safe_float(
            opportunity["macro_target"]
        )
        tp3 = (
            macro_target
            *
            (1.0 + TP3_SAFETY_MARGIN)
        )
        if tp3 >= tp1:
            return None
        tp2 = (
            tp1 + tp3
        ) / 2
    rr = abs(
        tp3 - entry
    ) / risk
    if rr < MIN_RR:
        return None
    return {
        "entry": entry,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "risk": risk,
        "rr": rr
    }
# ============================================================
# TELEGRAM
# ============================================================
def telegram_send(
    chat_id,
    message
):
    if not TELEGRAM_BOT_TOKEN:
        return False
    if not chat_id:
        return False
    url = (
        "https://api.telegram.org/bot"
        f"{TELEGRAM_BOT_TOKEN}/sendMessage"
    )
    try:
        response = session.post(
            url,
            json={
                "chat_id": chat_id,
                "text": message
            },
            timeout=10
        )
        if response.ok:
            payload = response.json()
            if payload.get("ok"):
                return True
        logger.warning(
            "Telegram erreur: %s",
            response.text[:500]
        )
    except Exception as exc:
        logger.warning(
            "Telegram exception: %s",
            exc
        )
    return False
def telegram_public(message):
    return telegram_send(
        TELEGRAM_CHAT_ID,
        message
    )
def telegram_admin(message):
    return telegram_send(
        TELEGRAM_ADMIN_ID,
        message
    )
# ============================================================
# SIGNAL MESSAGE
# ============================================================
def signal_message(trade):
    return (
        f"{trade['symbol']} "
        f"{trade['direction']}\n"
        f"Entry: {trade['entry']}\n"
        f"SL: {trade['sl']}\n"
        f"TP1: {trade['tp1']}\n"
        f"TP2: {trade['tp2']}\n"
        f"TP3: {trade['tp3']}"
    )
# ============================================================
# PENDING OPPORTUNITIES
# ============================================================
def save_pending(
    opportunity
):
    opportunity = dict(opportunity)
    opportunity["updated_at"] = iso_now()
    upsert_json_list(
        PENDING_FILE,
        "id",
        opportunity["id"],
        opportunity
    )
def remove_pending(
    opportunity_id
):
    with json_lock:
        data = read_json(
            PENDING_FILE,
            []
        )
        data = [
            item for item in data
            if item.get("id") != opportunity_id
        ]
        atomic_write_json(
            PENDING_FILE,
            data
        )
def get_pending():
    return read_json(
        PENDING_FILE,
        []
    )
# ============================================================
# ACTIVE TRADES
# ============================================================
def save_active_trade(
    trade
):
    upsert_json_list(
        ACTIVE_FILE,
        "id",
        trade["id"],
        trade
    )
def get_active_trades():
    return read_json(
        ACTIVE_FILE,
        []
    )
def remove_active_trade(
    trade_id
):
    with json_lock:
        data = read_json(
            ACTIVE_FILE,
            []
        )
        data = [
            item for item in data
            if item.get("id") != trade_id
        ]
        atomic_write_json(
            ACTIVE_FILE,
            data
        )
# ============================================================
# TRADE CREATION
# ============================================================
def create_trade(
    opportunity,
    df
):
    entry, setup = select_entry(
        opportunity,
        df
    )
    if entry is None:
        return None
    sl = calculate_m1_sl(
        opportunity,
        df
    )
    if sl is None:
        return None
    levels = calculate_trade_levels(
        opportunity,
        entry,
        sl
    )
    if levels is None:
        logger.info(
            "[RR REJECT] %s",
            opportunity["symbol"]
        )
        return None
    trade_id = (
        f"{opportunity['symbol']}_"
        f"{opportunity['direction']}_"
        f"{int(time.time() * 1000)}"
    )
    trade = {
        "id": trade_id,
        "symbol": opportunity["symbol"],
        "direction": opportunity["direction"],
        "entry": levels["entry"],
        "sl": levels["sl"],
        "tp1": levels["tp1"],
        "tp2": levels["tp2"],
        "tp3": levels["tp3"],
        "risk": levels["risk"],
        "rr": levels["rr"],
        "setup_type": setup["type"],
        "polarity_type": opportunity.get(
            "polarity_type"
        ),
        "trigger_price": opportunity.get(
            "trigger_price"
        ),
        "choch_strength": opportunity.get(
            "choch_strength"
        ),
        "source": opportunity.get(
            "data_source",
            "unknown"
        ),
        "status": "ACTIVE",
        "tp1_hit": False,
        "tp2_hit": False,
        "tp3_hit": False,
        "created_at": iso_now(),
        "updated_at": iso_now()
    }
    save_active_trade(
        trade
    )
    public_sent = telegram_public(
        signal_message(trade)
    )
    if public_sent:
        trade["telegram_sent"] = True
        save_active_trade(trade)
    return trade
# ============================================================
# TRADE MONITOR
# ============================================================
def price_hit_trade(
    trade,
    price
):
    direction = trade["direction"]
    if direction == "BUY":
        if not trade["tp1_hit"] and price >= trade["tp1"]:
            return "TP1"
        if not trade["tp2_hit"] and price >= trade["tp2"]:
            return "TP2"
        if price >= trade["tp3"]:
            return "TP3"
        if price <= trade["sl"]:
            return "SL"
    else:
        if not trade["tp1_hit"] and price <= trade["tp1"]:
            return "TP1"
        if not trade["tp2_hit"] and price <= trade["tp2"]:
            return "TP2"
        if price <= trade["tp3"]:
            return "TP3"
        if price >= trade["sl"]:
            return "SL"
    return None
def process_trade_event(
    trade,
    event,
    price
):
    trade = dict(trade)
    if event == "TP1":
        trade["tp1_hit"] = True
        trade["sl"] = trade["entry"]
        telegram_public(
            f"{trade['symbol']} {trade['direction']}\n"
            f"TP1 atteint\n"
            f"SL déplacé à l'entrée"
        )
        save_active_trade(trade)
        return
    if event == "TP2":
        trade["tp2_hit"] = True
        telegram_public(
            f"{trade['symbol']} {trade['direction']}\n"
            f"TP2 atteint"
        )
        save_active_trade(trade)
        return
    if event == "TP3":
        trade["tp3_hit"] = True
        trade["status"] = "CLOSED"
        trade["exit_reason"] = "TP3"
        trade["exit_price"] = price
        trade["closed_at"] = iso_now()
        append_json_list(
            HISTORY_FILE,
            trade
        )
        remove_active_trade(
            trade["id"]
        )
        telegram_public(
            f"{trade['symbol']} {trade['direction']}\n"
            f"TP3 atteint\n"
            f"Trade clôturé"
        )
        return
    if event == "SL":
        trade["status"] = "CLOSED"
        trade["exit_reason"] = "SL"
        trade["exit_price"] = price
        trade["closed_at"] = iso_now()
        append_json_list(
            HISTORY_FILE,
            trade
        )
        remove_active_trade(
            trade["id"]
        )
        telegram_public(
            f"{trade['symbol']} {trade['direction']}\n"
            f"SL atteint\n"
            f"Trade clôturé"
        )
def monitor_active_trades():
    while True:
        try:
            trades = get_active_trades()
            for trade in trades:
                symbol = trade.get("symbol")
                df = fetch_market_data_safe(
                    symbol,
                    "1m",
                    50
                )
                if df is None or df.empty:
                    continue
                price = safe_float(
                    df.iloc[-1]["close"]
                )
                if not np.isfinite(price):
                    continue
                event = price_hit_trade(
                    trade,
                    price
                )
                if event:
                    process_trade_event(
                        trade,
                        event,
                        price
                    )
        except Exception as exc:
            logger.exception(
                "Erreur monitor: %s",
                exc
            )
        time.sleep(5)
# ============================================================
# ANALYSE D'UN ACTIF
# ============================================================
def analyze_symbol(
    symbol
):
    try:
        m15 = fetch_market_data_safe(
            symbol,
            "15m",
            500
        )
        m5 = fetch_market_data_safe(
            symbol,
            "5m",
            500
        )
        m1 = fetch_market_data_safe(
            symbol,
            "1m",
            500
        )
        if m15 is None or m5 is None or m1 is None:
            return
        polarity = detect_m15_polarity(
            symbol,
            m15
        )
        if polarity is None:
            return
        polarity["data_source"] = "cascade"
        polarity["id"] = (
            f"{symbol}_"
            f"{polarity['direction']}_"
            f"{polarity['trigger_price']}"
        )
        existing = [
            x for x in get_pending()
            if x.get("id") == polarity["id"]
        ]
        if existing:
            opportunity = existing[0]
        else:
            opportunity = polarity
            save_pending(opportunity)
        retest = detect_m5_retest(
            opportunity,
            m5
        )
        if retest is None:
            return
        save_pending(retest)
        choch = detect_m1_choch(
            retest,
            m1
        )
        if choch is None:
            return
        save_pending(choch)
        bos = detect_m1_bos(
            choch,
            m1
        )
        if bos is None:
            return
        save_pending(bos)
        trade = create_trade(
            bos,
            m1
        )
        if trade:
            remove_pending(
                bos["id"]
            )
            logger.info(
                "[TRADE CREATED] %s %s | Entry %.5f | "
                "SL %.5f | TP1 %.5f | TP2 %.5f | TP3 %.5f | "
                "CHOCH %.2f%%",
                trade["symbol"],
                trade["direction"],
                trade["entry"],
                trade["sl"],
                trade["tp1"],
                trade["tp2"],
                trade["tp3"],
                trade.get("choch_strength", 0)
            )
    except Exception as exc:
        logger.exception(
            "Erreur analyse %s: %s",
            symbol,
            exc
        )
# ============================================================
# MARKET SCANNER
# ============================================================
def scan_market():
    while True:
        started = time.time()
        futures = []
        for symbol in SYMBOLS:
            futures.append(
                executor.submit(
                    analyze_symbol,
                    symbol
                )
            )
        for future in futures:
            try:
                future.result()
            except Exception as exc:
                logger.exception(
                    "Worker error: %s",
                    exc
                )
        elapsed = time.time() - started
        sleep_time = max(
            1,
            SCAN_INTERVAL - elapsed
        )
        time.sleep(
            sleep_time
        )
# ============================================================
# WEEKLY REPORT
# ============================================================
def generate_weekly_report():
    history = read_json(
        HISTORY_FILE,
        []
    )
    total = len(history)
    wins = len([
        x for x in history
        if x.get("exit_reason") in (
            "TP1",
            "TP2",
            "TP3"
        )
    ])
    losses = len([
        x for x in history
        if x.get("exit_reason") == "SL"
    ])
    win_rate = (
        (wins / total) * 100
        if total
        else 0
    )
    return {
        "generated_at": iso_now(),
        "total_trades": total,
        "wins": wins,
        "losses": losses,
        "win_rate": round(
            win_rate,
            2
        )
    }
def weekly_report_loop():
    while True:
        try:
            now = utc_now()
            if now.weekday() == 4 and now.hour == 22:
                report = generate_weekly_report()
                append_json_list(
                    REPORT_FILE,
                    report
                )
                telegram_public(
                    "RAPPORT HEBDOMADAIRE\n"
                    f"Trades: {report['total_trades']}\n"
                    f"Gagnants: {report['wins']}\n"
                    f"Perdants: {report['losses']}\n"
                    f"Win rate: {report['win_rate']}%"
                )
                time.sleep(3600)
        except Exception as exc:
            logger.exception(
                "Erreur rapport hebdomadaire: %s",
                exc
            )
        time.sleep(30)
# ============================================================
# FLASK
# ============================================================
@app.route("/")
def home():
    return jsonify({
        "name": "NOVA",
        "status": "running",
        "strategy": (
            "Price Action + SMC + "
            "Structure Polarity"
        ),
        "filters": {
            "atr_noise": True,
            "close_minus_2": True,
            "choch_min_strength": CHOCH_MIN_STRENGTH
        },
        "symbols": SYMBOLS
    })
@app.route("/health")
def health():
    return jsonify({
        "status": "ok",
        "timestamp": iso_now()
    })
@app.route("/status")
def status():
    return jsonify({
        "status": "running",
        "symbols": SYMBOLS,
        "pending": len(
            get_pending()
        ),
        "active": len(
            get_active_trades()
        ),
        "filters": {
            "ATR_MIN_RATIO": ATR_MIN_RATIO,
            "CLOSE_MINUS_2": True,
            "CHOCH_MIN_STRENGTH": CHOCH_MIN_STRENGTH
        }
    })
@app.route("/pending")
def pending():
    return jsonify(
        get_pending()
    )
@app.route("/active")
def active():
    return jsonify(
        get_active_trades()
    )
@app.route("/history")
def history():
    return jsonify(
        read_json(
            HISTORY_FILE,
            []
        )
    )
# ============================================================
# STARTUP
# ============================================================
_started = False
_start_lock = threading.Lock()
def start_background_workers():
    global _started
    with _start_lock:
        if _started:
            return
        _started = True
        threading.Thread(
            target=scan_market,
            daemon=True,
            name="market-scanner"
        ).start()
        threading.Thread(
            target=monitor_active_trades,
            daemon=True,
            name="trade-monitor"
        ).start()
        threading.Thread(
            target=weekly_report_loop,
            daemon=True,
            name="weekly-report"
        ).start()
        logger.info(
            "NOVA workers démarrés"
        )
start_background_workers()
if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=PORT,
        debug=False,
        threaded=True
    )