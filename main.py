# ============================================================
# NOVA TRADE ENGINE
# Strategy: Trend Continuation by Structure Polarity
# Assets: BTCUSD / XAUUSD / EURUSD / GBPUSD
#
# Architecture:
#   Flask
#   ThreadPoolExecutor
#   Thread-safe JSON persistence
#   Multi-source market data cascade
#   M15 -> M5 -> M1
#   BOS / CHoCH / Liquidity Sweep / FVG / OB
#   TP1 / TP2 / TP3
#   Break-Even
#   Telegram
#
# No real broker execution is performed by this file.
# Pending/active trades are tracked locally.
# ============================================================
import os
import json
import time
import math
import logging
import threading
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple
import numpy as np
import pandas as pd
import requests
from flask import Flask, jsonify
# ============================================================
# CONFIGURATION
# ============================================================
APP_NAME = "NOVA POLARITY ENGINE"
SYMBOLS = [
    "BTCUSD",
    "XAUUSD",
    "EURUSD",
    "GBPUSD",
]
TIMEFRAMES = [
    "M15",
    "M5",
    "M1",
]
DATA_LIMIT = 500
REQUEST_TIMEOUT = 5
ANALYSIS_INTERVAL_SECONDS = 60
TRADE_MONITOR_INTERVAL_SECONDS = 2
DATA_RETRY_DELAY = 0.5
POLARITY_TOLERANCE = 0.0003
MIN_RR = 3.0
TP3_SAFETY_MARGIN = 0.0002
SWING_LEFT = 3
SWING_RIGHT = 3
MICRO_SWING_LEFT = 2
MICRO_SWING_RIGHT = 2
MIN_FVG_SIZE = 0.0
JSON_LOCK = threading.RLock()
ENGINE_LOCK = threading.RLock()
SHUTDOWN_EVENT = threading.Event()
# ============================================================
# FILES
# ============================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PENDING_FILE = os.path.join(
    BASE_DIR,
    "pending_opportunities.json",
)
ACTIVE_FILE = os.path.join(
    BASE_DIR,
    "active_trades.json",
)
HISTORY_FILE = os.path.join(
    BASE_DIR,
    "trade_history.json",
)
STATE_FILE = os.path.join(
    BASE_DIR,
    "engine_state.json",
)
# ============================================================
# ENVIRONMENT
# ============================================================
PORT = int(
    os.getenv(
        "PORT",
        "8080",
    )
)
BIQUOTE_BASE_URL = os.getenv(
    "BIQUOTE_BASE_URL",
    "https://biquote.io/api",
).strip()
BIQUOTE_API_KEY = os.getenv(
    "BIQUOTE_API_KEY",
    "",
).strip()
TIINGO_TOKEN = os.getenv(
    "TIINGO_TOKEN",
    os.getenv(
        "TIINGO_API_KEY",
        "",
    ),
).strip()
TELEGRAM_BOT_TOKEN = os.getenv(
    "TELEGRAM_BOT_TOKEN",
    os.getenv(
        "TELEGRAM_TOKEN",
        "",
    ),
).strip()
TELEGRAM_CHAT_ID = os.getenv(
    "TELEGRAM_CHAT_ID",
    "",
).strip()
TELEGRAM_ADMIN_ID = os.getenv(
    "TELEGRAM_ADMIN_ID",
    os.getenv(
        "TELEGRAM_OWNER_ID",
        "",
    ),
).strip()
# ============================================================
# LOGGING
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format=(
        "%(asctime)s | "
        "%(levelname)s | "
        "%(threadName)s | "
        "%(message)s"
    ),
)
logger = logging.getLogger(APP_NAME)
# ============================================================
# FLASK
# ============================================================
app = Flask(__name__)
# ============================================================
# THREAD POOL
# ============================================================
MAX_WORKERS = max(
    8,
    len(SYMBOLS) * 3,
)
EXECUTOR = ThreadPoolExecutor(
    max_workers=MAX_WORKERS,
    thread_name_prefix="NOVA",
)
# ============================================================
# HTTP SESSION
# ============================================================
HTTP = requests.Session()
HTTP.headers.update(
    {
        "User-Agent": (
            "NOVA-Trade-Engine/1.0 "
            "(Python requests)"
        )
    }
)
# ============================================================
# TIME HELPERS
# ============================================================
def utc_now() -> datetime:
    return datetime.now(timezone.utc)
def iso_now() -> str:
    return utc_now().isoformat()
def safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        result = float(value)
        if not math.isfinite(result):
            return None
        return result
    except (
        TypeError,
        ValueError,
    ):
        return None
def timeframe_to_seconds(
    timeframe: str,
) -> int:
    mapping = {
        "M1": 60,
        "M5": 300,
        "M15": 900,
        "H1": 3600,
    }
    return mapping.get(
        timeframe.upper(),
        60,
    )
def timeframe_to_yahoo(
    timeframe: str,
) -> str:
    mapping = {
        "M1": "1m",
        "M5": "5m",
        "M15": "15m",
        "H1": "1h",
    }
    return mapping.get(
        timeframe.upper(),
        "5m",
    )
# ============================================================
# JSON UTILITIES
# ============================================================
def ensure_json_file(
    path: str,
    default: Any,
) -> None:
    with JSON_LOCK:
        if os.path.exists(path):
            return
        _atomic_write_json(
            path,
            default,
        )
def _atomic_write_json(
    path: str,
    data: Any,
) -> None:
    temp_path = (
        f"{path}."
        f"{threading.get_ident()}."
        f"tmp"
    )
    with open(
        temp_path,
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            data,
            file,
            ensure_ascii=False,
            indent=2,
            default=str,
        )
        file.flush()
        os.fsync(
            file.fileno()
        )
    os.replace(
        temp_path,
        path,
    )
def read_json(
    path: str,
    default: Any,
) -> Any:
    with JSON_LOCK:
        try:
            if not os.path.exists(path):
                _atomic_write_json(
                    path,
                    default,
                )
                return default
            with open(
                path,
                "r",
                encoding="utf-8",
            ) as file:
                return json.load(file)
        except (
            OSError,
            json.JSONDecodeError,
            TypeError,
            ValueError,
        ) as exc:
            logger.error(
                "JSON read error %s: %s",
                path,
                exc,
            )
            return default
def write_json(
    path: str,
    data: Any,
) -> None:
    with JSON_LOCK:
        _atomic_write_json(
            path,
            data,
        )
def update_json(
    path: str,
    updater,
    default: Any,
) -> Any:
    with JSON_LOCK:
        current = read_json(
            path,
            default,
        )
        updated = updater(
            current
        )
        _atomic_write_json(
            path,
            updated,
        )
        return updated
# ============================================================
# INITIAL STATE
# ============================================================
ensure_json_file(
    PENDING_FILE,
    {},
)
ensure_json_file(
    ACTIVE_FILE,
    {},
)
ensure_json_file(
    HISTORY_FILE,
    [],
)
ensure_json_file(
    STATE_FILE,
    {},
)
# ============================================================
# STATE
# ============================================================
def set_engine_state(
    key: str,
    value: Any,
) -> None:
    def updater(state):
        if not isinstance(
            state,
            dict,
        ):
            state = {}
        state[key] = value
        return state
    update_json(
        STATE_FILE,
        updater,
        {},
    )
def get_engine_state() -> Dict[str, Any]:
    state = read_json(
        STATE_FILE,
        {},
    )
    if not isinstance(
        state,
        dict,
    ):
        return {}
    return state
# ============================================================
# SYMBOL MAPPING
# ============================================================
YAHOO_SYMBOLS = {
    "BTCUSD": "BTC-USD",
    "XAUUSD": "GC=F",
    "EURUSD": "EURUSD=X",
    "GBPUSD": "GBPUSD=X",
}
KRAKEN_SYMBOLS = {
    "BTCUSD": "BTCUSD",
}
TIINGO_SYMBOLS = {
    "XAUUSD": "XAUUSD",
    "EURUSD": "eurusd",
    "GBPUSD": "gbpusd",
}
# ============================================================
# DATAFRAME NORMALIZATION
# ============================================================
REQUIRED_COLUMNS = [
    "open",
    "high",
    "low",
    "close",
    "volume",
]
def normalize_dataframe(
    df: pd.DataFrame,
    limit: int = 500,
) -> pd.DataFrame:
    if df is None:
        raise ValueError(
            "DataFrame is None"
        )
    if df.empty:
        raise ValueError(
            "DataFrame is empty"
        )
    result = df.copy()
    result.columns = [
        str(column).lower()
        for column in result.columns
    ]
    aliases = {
        "o": "open",
        "h": "high",
        "l": "low",
        "c": "close",
        "v": "volume",
        "timestamp": "timestamp",
        "time": "timestamp",
        "datetime": "timestamp",
        "date": "timestamp",
    }
    rename = {}
    for column in result.columns:
        if column in aliases:
            rename[column] = aliases[column]
    result = result.rename(
        columns=rename
    )
    if not all(
        column in result.columns
        for column in REQUIRED_COLUMNS
    ):
        raise ValueError(
            "Missing OHLCV columns"
        )
    if not isinstance(
        result.index,
        pd.DatetimeIndex,
    ):
        if "timestamp" in result.columns:
            result.index = pd.to_datetime(
                result["timestamp"],
                utc=True,
                errors="coerce",
            )
        else:
            result.index = pd.to_datetime(
                result.index,
                utc=True,
                errors="coerce",
            )
    else:
        result.index = pd.to_datetime(
            result.index,
            utc=True,
            errors="coerce",
        )
    result = result[
        ~result.index.isna()
    ]
    result = result[
        REQUIRED_COLUMNS
    ]
    for column in REQUIRED_COLUMNS:
        result[column] = pd.to_numeric(
            result[column],
            errors="coerce",
        )
    result = result.replace(
        [
            np.inf,
            -np.inf,
        ],
        np.nan,
    )
    result = result.dropna(
        subset=[
            "open",
            "high",
            "low",
            "close",
        ]
    )
    result["volume"] = (
        result["volume"]
        .fillna(0.0)
    )
    result = result.sort_index()
    result = result[
        ~result.index.duplicated(
            keep="last"
        )
    ]
    if limit > 0:
        result = result.tail(
            limit
        )
    if len(result) < 20:
        raise ValueError(
            "Insufficient market data"
        )
    return result
# ============================================================
# GENERIC API RESPONSE PARSER
# ============================================================
def response_to_dataframe(
    payload: Any,
) -> pd.DataFrame:
    if isinstance(
        payload,
        dict,
    ):
        for key in (
            "data",
            "result",
            "results",
            "candles",
            "bars",
            "ohlcv",
            "prices",
            "items",
        ):
            if key in payload:
                nested = payload[key]
                if isinstance(
                    nested,
                    dict,
                ):
                    try:
                        return response_to_dataframe(
                            nested
                        )
                    except ValueError:
                        continue
                if isinstance(
                    nested,
                    list,
                ):
                    payload = nested
                    break
        if isinstance(
            payload,
            dict,
        ):
            if all(
                key in payload
                for key in (
                    "open",
                    "high",
                    "low",
                    "close",
                )
            ):
                payload = [
                    payload
                ]
            else:
                for key in (
                    "chart",
                    "query",
                    "quotes",
                ):
                    if key in payload:
                        try:
                            return response_to_dataframe(
                                payload[key]
                            )
                        except ValueError:
                            continue
    if isinstance(
        payload,
        list,
    ):
        if not payload:
            raise ValueError(
                "Empty API payload"
            )
        first = payload[0]
        if isinstance(
            first,
            dict,
        ):
            rows = []
            for item in payload:
                if not isinstance(
                    item,
                    dict,
                ):
                    continue
                timestamp = (
                    item.get(
                        "timestamp"
                    )
                    or item.get(
                        "time"
                    )
                    or item.get(
                        "datetime"
                    )
                    or item.get(
                        "date"
                    )
                )
                rows.append(
                    {
                        "timestamp": timestamp,
                        "open": (
                            item.get("open")
                            or item.get("o")
                        ),
                        "high": (
                            item.get("high")
                            or item.get("h")
                        ),
                        "low": (
                            item.get("low")
                            or item.get("l")
                        ),
                        "close": (
                            item.get("close")
                            or item.get("c")
                        ),
                        "volume": (
                            item.get("volume")
                            if item.get(
                                "volume"
                            ) is not None
                            else item.get("v", 0)
                        ),
                    }
                )
            return pd.DataFrame(
                rows
            )
        if isinstance(
            first,
            (list, tuple),
        ):
            rows = []
            for item in payload:
                if len(item) < 5:
                    continue
                rows.append(
                    {
                        "timestamp": item[0],
                        "open": item[1],
                        "high": item[2],
                        "low": item[3],
                        "close": item[4],
                        "volume": (
                            item[5]
                            if len(item) > 5
                            else 0
                        ),
                    }
                )
            return pd.DataFrame(
                rows
            )
    raise ValueError(
        "Unsupported API payload"
    )
# ============================================================
# BIQUOTE
# ============================================================
def fetch_biquote(
    symbol: str,
    timeframe: str,
    limit: int = 500,
) -> pd.DataFrame:
    if not BIQUOTE_BASE_URL:
        raise RuntimeError(
            "BIQUOTE_BASE_URL not configured"
        )
    interval = timeframe.lower()
    endpoints = [
        (
            f"{BIQUOTE_BASE_URL.rstrip('/')}"
            f"/ohlcv"
        ),
        (
            f"{BIQUOTE_BASE_URL.rstrip('/')}"
            f"/candles"
        ),
        (
            f"{BIQUOTE_BASE_URL.rstrip('/')}"
            f"/market/ohlcv"
        ),
    ]
    headers = {}
    if BIQUOTE_API_KEY:
        headers[
            "Authorization"
        ] = (
            f"Bearer {BIQUOTE_API_KEY}"
        )
        headers[
            "X-API-Key"
        ] = BIQUOTE_API_KEY
    params_variants = [
        {
            "symbol": symbol,
            "timeframe": interval,
            "limit": max(
                500,
                limit,
            ),
        },
        {
            "symbol": symbol,
            "interval": interval,
            "limit": max(
                500,
                limit,
            ),
        },
    ]
    last_error = None
    for endpoint in endpoints:
        for params in params_variants:
            try:
                response = HTTP.get(
                    endpoint,
                    params=params,
                    headers=headers,
                    timeout=REQUEST_TIMEOUT,
                )
                response.raise_for_status()
                payload = response.json()
                df = response_to_dataframe(
                    payload
                )
                df = normalize_dataframe(
                    df,
                    limit=limit,
                )
                if len(df) < min(
                    500,
                    limit,
                ):
                    raise ValueError(
                        "BiQuote returned fewer "
                        "than requested candles"
                    )
                logger.info(
                    "[BiQuote OK] %s %s | %d candles",
                    symbol,
                    timeframe,
                    len(df),
                )
                return df
            except Exception as exc:
                last_error = exc
    raise RuntimeError(
        f"BiQuote failed: {last_error}"
    )
# ============================================================
# KRAKEN
# ============================================================
def fetch_kraken(
    symbol: str,
    timeframe: str,
    limit: int = 500,
) -> pd.DataFrame:
    if symbol not in KRAKEN_SYMBOLS:
        raise RuntimeError(
            f"Kraken does not support {symbol}"
        )
    if symbol != "BTCUSD":
        raise RuntimeError(
            "Kraken fallback is crypto-only"
        )
    interval_map = {
        "M1": 1,
        "M5": 5,
        "M15": 15,
    }
    interval = interval_map.get(
        timeframe.upper(),
        5,
    )
    url = (
        "https://api.kraken.com/0/public/OHLC"
    )
    params = {
        "pair": "XBTUSD",
        "interval": interval,
    }
    response = HTTP.get(
        url,
        params=params,
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("error"):
        raise RuntimeError(
            str(payload["error"])
        )
    result = payload.get(
        "result",
        {},
    )
    candles = None
    for key, value in result.items():
        if key != "last" and isinstance(
            value,
            list,
        ):
            candles = value
            break
    if not candles:
        raise ValueError(
            "Kraken returned no candles"
        )
    rows = []
    for candle in candles:
        if len(candle) < 7:
            continue
        rows.append(
            {
                "timestamp": candle[0],
                "open": candle[1],
                "high": candle[2],
                "low": candle[3],
                "close": candle[4],
                "volume": candle[6],
            }
        )
    df = pd.DataFrame(
        rows
    )
    df = normalize_dataframe(
        df,
        limit=limit,
    )
    logger.info(
        "[Kraken OK] %s %s | %d candles",
        symbol,
        timeframe,
        len(df),
    )
    return df
# ============================================================
# TIINGO
# ============================================================
def fetch_tiingo(
    symbol: str,
    timeframe: str,
    limit: int = 500,
) -> pd.DataFrame:
    if not TIINGO_TOKEN:
        raise RuntimeError(
            "TIINGO_TOKEN not configured"
        )
    if symbol not in TIINGO_SYMBOLS:
        raise RuntimeError(
            f"Tiingo mapping unavailable for {symbol}"
        )
    ticker = TIINGO_SYMBOLS[
        symbol
    ]
    url = (
        "https://api.tiingo.com"
        f"/tiingo/fx/{ticker}/prices"
    )
    seconds = timeframe_to_seconds(
        timeframe
    )
    start = (
        utc_now()
        - timedelta(
            seconds=(
                seconds
                * max(
                    1000,
                    limit * 3,
                )
            )
        )
    )
    params = {
        "startDate": start.strftime(
            "%Y-%m-%d"
        ),
        "resampleFreq": {
            "M1": "1min",
            "M5": "5min",
            "M15": "15min",
        }.get(
            timeframe.upper(),
            "5min",
        ),
        "token": TIINGO_TOKEN,
    }
    response = HTTP.get(
        url,
        params=params,
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    payload = response.json()
    df = response_to_dataframe(
        payload
    )
    df = normalize_dataframe(
        df,
        limit=limit,
    )
    if len(df) < 20:
        raise ValueError(
            "Tiingo returned insufficient data"
        )
    logger.info(
        "[Tiingo OK] %s %s | %d candles",
        symbol,
        timeframe,
        len(df),
    )
    return df
# ============================================================
# YAHOO FINANCE HTTP
# ============================================================
def fetch_yahoo(
    symbol: str,
    timeframe: str,
    limit: int = 500,
) -> pd.DataFrame:
    yahoo_symbol = YAHOO_SYMBOLS.get(
        symbol
    )
    if not yahoo_symbol:
        raise RuntimeError(
            f"No Yahoo symbol for {symbol}"
        )
    interval = timeframe_to_yahoo(
        timeframe
    )
    seconds = timeframe_to_seconds(
        timeframe
    )
    required_period = (
        seconds
        * max(
            1000,
            limit * 2,
        )
    )
    period2 = int(
        time.time()
    )
    period1 = (
        period2
        - required_period
    )
    url = (
        "https://query1.finance.yahoo.com"
        f"/v8/finance/chart/{yahoo_symbol}"
    )
    params = {
        "period1": period1,
        "period2": period2,
        "interval": interval,
        "events": "history",
        "includeAdjustedClose": "true",
    }
    response = HTTP.get(
        url,
        params=params,
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    payload = response.json()
    chart = payload.get(
        "chart",
        {},
    )
    results = chart.get(
        "result"
    )
    if not results:
        raise ValueError(
            "Yahoo returned no chart"
        )
    chart_data = results[0]
    timestamps = chart_data.get(
        "timestamp"
    )
    indicators = chart_data.get(
        "indicators",
        {},
    )
    quote_list = indicators.get(
        "quote",
        [],
    )
    if not timestamps or not quote_list:
        raise ValueError(
            "Yahoo returned no OHLC data"
        )
    quote = quote_list[0]
    df = pd.DataFrame(
        {
            "timestamp": timestamps,
            "open": quote.get(
                "open",
                [],
            ),
            "high": quote.get(
                "high",
                [],
            ),
            "low": quote.get(
                "low",
                [],
            ),
            "close": quote.get(
                "close",
                [],
            ),
            "volume": quote.get(
                "volume",
                [],
            ),
        }
    )
    df = normalize_dataframe(
        df,
        limit=limit,
    )
    if len(df) < 20:
        raise ValueError(
            "Yahoo returned insufficient data"
        )
    logger.info(
        "[Yahoo OK] %s %s | Price %.8f | Volume %.2f | %d candles",
        symbol,
        timeframe,
        float(df["close"].iloc[-1]),
        float(df["volume"].iloc[-1]),
        len(df),
    )
    return df
# ============================================================
# MASTER DATA CASCADE
# ============================================================
def fetch_market_data_safe(
    symbol: str,
    timeframe: str,
    limit: int = 500,
) -> pd.DataFrame:
    symbol = symbol.upper()
    timeframe = timeframe.upper()
    errors = []
    # --------------------------------------------------------
    # SOURCE 1: BIQUOTE
    # --------------------------------------------------------
    try:
        df = fetch_biquote(
            symbol,
            timeframe,
            limit,
        )
        set_engine_state(
            f"source_{symbol}_{timeframe}",
            {
                "source": "BiQuote",
                "updated_at": iso_now(),
            },
        )
        return df
    except Exception as exc:
        errors.append(
            f"BiQuote: {exc}"
        )
        logger.warning(
            "[BiQuote FAILED] %s %s | %s",
            symbol,
            timeframe,
            exc,
        )
    # --------------------------------------------------------
    # SOURCE 2: KRAKEN FOR CRYPTO / TIINGO FOR FOREX-GOLD
    # --------------------------------------------------------
    try:
        if symbol == "BTCUSD":
            df = fetch_kraken(
                symbol,
                timeframe,
                limit,
            )
            source = "Kraken"
        else:
            df = fetch_tiingo(
                symbol,
                timeframe,
                limit,
            )
            source = "Tiingo"
        set_engine_state(
            f"source_{symbol}_{timeframe}",
            {
                "source": source,
                "updated_at": iso_now(),
            },
        )
        return df
    except Exception as exc:
        errors.append(
            f"Fallback 1: {exc}"
        )
        logger.warning(
            "[FALLBACK 1 FAILED] %s %s | %s",
            symbol,
            timeframe,
            exc,
        )
    # --------------------------------------------------------
    # SOURCE 3: YAHOO FINANCE HTTP
    # --------------------------------------------------------
    try:
        df = fetch_yahoo(
            symbol,
            timeframe,
            limit,
        )
        set_engine_state(
            f"source_{symbol}_{timeframe}",
            {
                "source": "Yahoo Finance",
                "updated_at": iso_now(),
            },
        )
        return df
    except Exception as exc:
        errors.append(
            f"Yahoo: {exc}"
        )
        logger.error(
            "[ALL DATA SOURCES FAILED] %s %s | %s",
            symbol,
            timeframe,
            " | ".join(errors),
        )
        raise RuntimeError(
            f"No market data for "
            f"{symbol} {timeframe}: "
            f"{' | '.join(errors)}"
        )
# ============================================================
# MARKET STRUCTURE
# ============================================================
def detect_swing_highs(
    df: pd.DataFrame,
    left: int = SWING_LEFT,
    right: int = SWING_RIGHT,
) -> pd.Series:
    high = df["high"]
    result = pd.Series(
        False,
        index=df.index,
    )
    for i in range(
        left,
        len(df) - right,
    ):
        current = float(
            high.iloc[i]
        )
        left_values = high.iloc[
            i - left:i
        ]
        right_values = high.iloc[
            i + 1:i + 1 + right
        ]
        if (
            current >= left_values.max()
            and current >= right_values.max()
        ):
            result.iloc[i] = True
    return result
def detect_swing_lows(
    df: pd.DataFrame,
    left: int = SWING_LEFT,
    right: int = SWING_RIGHT,
) -> pd.Series:
    low = df["low"]
    result = pd.Series(
        False,
        index=df.index,
    )
    for i in range(
        left,
        len(df) - right,
    ):
        current = float(
            low.iloc[i]
        )
        left_values = low.iloc[
            i - left:i
        ]
        right_values = low.iloc[
            i + 1:i + 1 + right
        ]
        if (
            current <= left_values.min()
            and current <= right_values.min()
        ):
            result.iloc[i] = True
    return result
def latest_major_structure(
    df: pd.DataFrame,
) -> Dict[str, Any]:
    highs = detect_swing_highs(
        df
    )
    lows = detect_swing_lows(
        df
    )
    high_indexes = list(
        np.where(
            highs.values
        )[0]
    )
    low_indexes = list(
        np.where(
            lows.values
        )[0]
    )
    major_high = None
    major_low = None
    if high_indexes:
        idx = high_indexes[-1]
        major_high = {
            "index": int(idx),
            "timestamp": df.index[idx].isoformat(),
            "price": float(
                df["high"].iloc[idx]
            ),
        }
    if low_indexes:
        idx = low_indexes[-1]
        major_low = {
            "index": int(idx),
            "timestamp": df.index[idx].isoformat(),
            "price": float(
                df["low"].iloc[idx]
            ),
        }
    return {
        "high": major_high,
        "low": major_low,
    }
# ============================================================
# M15 BOS / POLARITY
# ============================================================
def detect_m15_polarity(
    symbol: str,
    df: pd.DataFrame,
) -> List[Dict[str, Any]]:
    df = normalize_dataframe(
        df,
        limit=500,
    )
    highs = detect_swing_highs(
        df
    )
    lows = detect_swing_lows(
        df
    )
    opportunities = []
    known_highs = []
    known_lows = []
    for i in range(
        len(df)
    ):
        candle = df.iloc[i]
        close = float(
            candle["close"]
        )
        high = float(
            candle["high"]
        )
        low = float(
            candle["low"]
        )
        timestamp = (
            df.index[i]
            .isoformat()
        )
        # ----------------------------------------------------
        # RESISTANCE -> SUPPORT
        # Bullish BOS
        # ----------------------------------------------------
        if known_highs:
            candidate = known_highs[-1]
            if (
                close
                > candidate["price"]
                * (
                    1.0
                    + POLARITY_TOLERANCE
                )
            ):
                subsequent_highs = [
                    item
                    for item in known_highs
                    if item["index"] < i
                ]
                if subsequent_highs:
                    broken = (
                        subsequent_highs[-1]
                    )
                    target = high
                    opportunity = {
                        "id": (
                            f"{symbol}_BULL_"
                            f"{int(time.time() * 1000)}"
                        ),
                        "symbol": symbol,
                        "direction": "BUY",
                        "status": "WAITING_M5_RETEST",
                        "created_at": iso_now(),
                        "m15_bos_time": timestamp,
                        "trigger_price": float(
                            broken["price"]
                        ),
                        "macro_target": float(
                            target
                        ),
                        "macro_structure": "HIGH",
                        "polarity_type": (
                            "RESISTANCE_TO_SUPPORT"
                        ),
                        "bos_index": i,
                        "bos_price": close,
                        "source": "M15",
                        "m5_liquidity_extreme": None,
                        "m1_choch": None,
                        "m1_bos": None,
                        "entry": None,
                        "sl": None,
                        "tp1": None,
                        "tp2": None,
                        "tp3": None,
                        "rr": None,
                    }
                    opportunities.append(
                        opportunity
                    )
                    known_highs = [
                        item
                        for item in known_highs
                        if item["index"] != broken["index"]
                    ]
        # ----------------------------------------------------
        # SUPPORT -> RESISTANCE
        # Bearish BOS
        # ----------------------------------------------------
        if known_lows:
            candidate = known_lows[-1]
            if (
                close
                < candidate["price"]
                * (
                    1.0
                    - POLARITY_TOLERANCE
                )
            ):
                subsequent_lows = [
                    item
                    for item in known_lows
                    if item["index"] < i
                ]
                if subsequent_lows:
                    broken = (
                        subsequent_lows[-1]
                    )
                    target = low
                    opportunity = {
                        "id": (
                            f"{symbol}_BEAR_"
                            f"{int(time.time() * 1000)}"
                        ),
                        "symbol": symbol,
                        "direction": "SELL",
                        "status": "WAITING_M5_RETEST",
                        "created_at": iso_now(),
                        "m15_bos_time": timestamp,
                        "trigger_price": float(
                            broken["price"]
                        ),
                        "macro_target": float(
                            target
                        ),
                        "macro_structure": "LOW",
                        "polarity_type": (
                            "SUPPORT_TO_RESISTANCE"
                        ),
                        "bos_index": i,
                        "bos_price": close,
                        "source": "M15",
                        "m5_liquidity_extreme": None,
                        "m1_choch": None,
                        "m1_bos": None,
                        "entry": None,
                        "sl": None,
                        "tp1": None,
                        "tp2": None,
                        "tp3": None,
                        "rr": None,
                    }
                    opportunities.append(
                        opportunity
                    )
                    known_lows = [
                        item
                        for item in known_lows
                        if item["index"] != broken["index"]
                    ]
        if highs.iloc[i]:
            known_highs.append(
                {
                    "index": i,
                    "timestamp": timestamp,
                    "price": high,
                }
            )
        if lows.iloc[i]:
            known_lows.append(
                {
                    "index": i,
                    "timestamp": timestamp,
                    "price": low,
                }
            )
    # --------------------------------------------------------
    # Deduplicate
    # --------------------------------------------------------
    unique = {}
    for opportunity in opportunities:
        key = (
            opportunity["direction"],
            round(
                opportunity[
                    "trigger_price"
                ],
                10,
            ),
            opportunity[
                "m15_bos_time"
            ],
        )
        unique[key] = opportunity
    return list(
        unique.values()
    )[-10:]
# ============================================================
# PENDING OPPORTUNITIES
# ============================================================
def load_pending() -> Dict[str, Any]:
    data = read_json(
        PENDING_FILE,
        {},
    )
    if not isinstance(
        data,
        dict,
    ):
        return {}
    return data
def save_pending(
    pending: Dict[str, Any],
) -> None:
    write_json(
        PENDING_FILE,
        pending,
    )
def register_m15_opportunities(
    symbol: str,
    opportunities: List[Dict[str, Any]],
) -> None:
    if not opportunities:
        return
    with JSON_LOCK:
        pending = load_pending()
        for opportunity in opportunities:
            key = opportunity["id"]
            already_exists = any(
                item.get("trigger_price")
                == opportunity["trigger_price"]
                and item.get("symbol")
                == symbol
                and item.get("direction")
                == opportunity["direction"]
                and item.get("status")
                in {
                    "WAITING_M5_RETEST",
                    "WAITING_M1_CHOCH",
                    "WAITING_M1_BOS",
                }
                for item in pending.values()
            )
            if already_exists:
                continue
            pending[key] = opportunity
            logger.info(
                "[M15 POLARITY] %s | %s | "
                "Trigger %.8f | Macro %.8f",
                symbol,
                opportunity["direction"],
                opportunity["trigger_price"],
                opportunity["macro_target"],
            )
            send_telegram(
                format_generation_message(
                    opportunity
                )
            )
        save_pending(
            pending
        )
# ============================================================
# M5 LIQUIDITY RETEST
# ============================================================
def detect_m5_liquidity_retest(
    opportunity: Dict[str, Any],
    df: pd.DataFrame,
) -> Optional[Dict[str, Any]]:
    if df.empty:
        return None
    trigger = float(
        opportunity["trigger_price"]
    )
    direction = opportunity[
        "direction"
    ]
    recent = df.tail(
        min(
            30,
            len(df),
        )
    )
    for timestamp, candle in recent.iterrows():
        high = float(
            candle["high"]
        )
        low = float(
            candle["low"]
        )
        close = float(
            candle["close"]
        )
        if direction == "BUY":
            swept = (
                low
                < trigger
            )
            reentered = (
                close
                > trigger
            )
            if swept and reentered:
                return {
                    "timestamp": timestamp.isoformat(),
                    "trigger_price": trigger,
                    "liquidity_extreme": low,
                    "close": close,
                    "direction": "BUY",
                }
        else:
            swept = (
                high
                > trigger
            )
            reentered = (
                close
                < trigger
            )
            if swept and reentered:
                return {
                    "timestamp": timestamp.isoformat(),
                    "trigger_price": trigger,
                    "liquidity_extreme": high,
                    "close": close,
                    "direction": "SELL",
                }
    return None
def update_m5_retests(
    symbol: str,
    df: pd.DataFrame,
) -> None:
    with JSON_LOCK:
        pending = load_pending()
        changed = False
        for key, opportunity in list(
            pending.items()
        ):
            if opportunity.get(
                "symbol"
            ) != symbol:
                continue
            if opportunity.get(
                "status"
            ) != "WAITING_M5_RETEST":
                continue
            retest = detect_m5_liquidity_retest(
                opportunity,
                df,
            )
            if not retest:
                continue
            opportunity[
                "status"
            ] = "WAITING_M1_CHOCH"
            opportunity[
                "m5_retest"
            ] = retest
            opportunity[
                "m5_liquidity_extreme"
            ] = retest[
                "liquidity_extreme"
            ]
            opportunity[
                "m5_retest_time"
            ] = retest[
                "timestamp"
            ]
            pending[key] = opportunity
            changed = True
            logger.info(
                "[M5 RETEST] %s | %s | "
                "Liquidity %.8f",
                symbol,
                opportunity["direction"],
                retest[
                    "liquidity_extreme"
                ],
            )
            send_telegram(
                format_m5_retest_message(
                    opportunity
                )
            )
        if changed:
            save_pending(
                pending
            )
# ============================================================
# M1 CHOCH
# ============================================================
def micro_structure(
    df: pd.DataFrame,
) -> Dict[str, Any]:
    highs = detect_swing_highs(
        df,
        MICRO_SWING_LEFT,
        MICRO_SWING_RIGHT,
    )
    lows = detect_swing_lows(
        df,
        MICRO_SWING_LEFT,
        MICRO_SWING_RIGHT,
    )
    swing_highs = []
    swing_lows = []
    for i in np.where(
        highs.values
    )[0]:
        swing_highs.append(
            {
                "index": int(i),
                "price": float(
                    df["high"].iloc[i]
                ),
                "timestamp": (
                    df.index[i].isoformat()
                ),
            }
        )
    for i in np.where(
        lows.values
    )[0]:
        swing_lows.append(
            {
                "index": int(i),
                "price": float(
                    df["low"].iloc[i]
                ),
                "timestamp": (
                    df.index[i].isoformat()
                ),
            }
        )
    return {
        "highs": swing_highs,
        "lows": swing_lows,
    }
def detect_m1_choch(
    opportunity: Dict[str, Any],
    df: pd.DataFrame,
) -> Optional[Dict[str, Any]]:
    structure = micro_structure(
        df
    )
    highs = structure[
        "highs"
    ]
    lows = structure[
        "lows"
    ]
    if len(df) < 10:
        return None
    direction = opportunity[
        "direction"
    ]
    recent_df = df.tail(
        min(
            100,
            len(df),
        )
    )
    if direction == "BUY":
        if not lows:
            return None
        last_low = lows[-1]
        after_index = last_low[
            "index"
        ]
        if after_index >= len(df) - 1:
            return None
        for i in range(
            max(
                after_index + 1,
                1,
            ),
            len(df),
        ):
            close = float(
                df["close"].iloc[i]
            )
            if (
                close
                > last_low["price"]
            ):
                continue
            for high in highs:
                if high["index"] <= i:
                    if (
                        close
                        > high["price"]
                    ):
                        return {
                            "type": "BULLISH_CHOCH",
                            "index": i,
                            "timestamp": (
                                df.index[i].isoformat()
                            ),
                            "price": close,
                            "broken_level": high[
                                "price"
                            ],
                        }
    else:
        if not highs:
            return None
        last_high = highs[-1]
        after_index = last_high[
            "index"
        ]
        if after_index >= len(df) - 1:
            return None
        for i in range(
            max(
                after_index + 1,
                1,
            ),
            len(df),
        ):
            close = float(
                df["close"].iloc[i]
            )
            for low in lows:
                if low["index"] <= i:
                    if (
                        close
                        < low["price"]
                    ):
                        return {
                            "type": "BEARISH_CHOCH",
                            "index": i,
                            "timestamp": (
                                df.index[i].isoformat()
                            ),
                            "price": close,
                            "broken_level": low[
                                "price"
                            ],
                        }
    return None
# ============================================================
# M1 BOS
# ============================================================
def detect_m1_bos(
    opportunity: Dict[str, Any],
    df: pd.DataFrame,
    choch: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    direction = opportunity[
        "direction"
    ]
    start = int(
        choch["index"]
    )
    if start >= len(df) - 1:
        return None
    highs = detect_swing_highs(
        df,
        MICRO_SWING_LEFT,
        MICRO_SWING_RIGHT,
    )
    lows = detect_swing_lows(
        df,
        MICRO_SWING_LEFT,
        MICRO_SWING_RIGHT,
    )
    if direction == "BUY":
        levels = []
        for i in np.where(
            highs.values
        )[0]:
            if i < start:
                levels.append(
                    float(
                        df["high"].iloc[i]
                    )
                )
        if not levels:
            return None
        level = max(
            levels[-3:]
        )
        for i in range(
            start + 1,
            len(df),
        ):
            close = float(
                df["close"].iloc[i]
            )
            if close > level:
                return {
                    "type": "BULLISH_BOS",
                    "index": i,
                    "timestamp": (
                        df.index[i].isoformat()
                    ),
                    "price": close,
                    "broken_level": level,
                }
    else:
        levels = []
        for i in np.where(
            lows.values
        )[0]:
            if i < start:
                levels.append(
                    float(
                        df["low"].iloc[i]
                    )
                )
        if not levels:
            return None
        level = min(
            levels[-3:]
        )
        for i in range(
            start + 1,
            len(df),
        ):
            close = float(
                df["close"].iloc[i]
            )
            if close < level:
                return {
                    "type": "BEARISH_BOS",
                    "index": i,
                    "timestamp": (
                        df.index[i].isoformat()
                    ),
                    "price": close,
                    "broken_level": level,
                }
    return None
# ============================================================
# FVG DETECTION
# ============================================================
def detect_fvg(
    df: pd.DataFrame,
    direction: str,
) -> Optional[Dict[str, Any]]:
    if len(df) < 5:
        return None
    start = max(
        2,
        len(df) - 50,
    )
    for i in range(
        len(df) - 1,
        start - 1,
        -1,
    ):
        if i - 2 < 0:
            continue
        left = df.iloc[
            i - 2
        ]
        middle = df.iloc[
            i - 1
        ]
        right = df.iloc[
            i
        ]
        if direction == "BUY":
            gap_low = float(
                left["high"]
            )
            gap_high = float(
                right["low"]
            )
            if (
                gap_high
                > gap_low
                + MIN_FVG_SIZE
            ):
                return {
                    "type": "BULLISH_FVG",
                    "low": gap_low,
                    "high": gap_high,
                    "entry": (
                        gap_low
                        + (
                            gap_high
                            - gap_low
                        )
                        / 2.0
                    ),
                    "timestamp": (
                        df.index[i].isoformat()
                    ),
                    "index": i,
                }
        else:
            gap_low = float(
                right["high"]
            )
            gap_high = float(
                left["low"]
            )
            if (
                gap_high
                > gap_low
                + MIN_FVG_SIZE
            ):
                return {
                    "type": "BEARISH_FVG",
                    "low": gap_low,
                    "high": gap_high,
                    "entry": (
                        gap_low
                        + (
                            gap_high
                            - gap_low
                        )
                        / 2.0
                    ),
                    "timestamp": (
                        df.index[i].isoformat()
                    ),
                    "index": i,
                }
    return None
# ============================================================
# ORDER BLOCK DETECTION
# ============================================================
def detect_order_block(
    df: pd.DataFrame,
    direction: str,
) -> Optional[Dict[str, Any]]:
    if len(df) < 10:
        return None
    start = max(
        1,
        len(df) - 40,
    )
    for i in range(
        len(df) - 2,
        start - 1,
        -1,
    ):
        candle = df.iloc[i]
        candle_open = float(
            candle["open"]
        )
        candle_close = float(
            candle["close"]
        )
        candle_high = float(
            candle["high"]
        )
        candle_low = float(
            candle["low"]
        )
        if direction == "BUY":
            bearish = (
                candle_close
                < candle_open
            )
            if not bearish:
                continue
            future_high = float(
                df["high"].iloc[
                    i + 1:
                :].max()
            )
            if future_high > candle_high:
                return {
                    "type": "BULLISH_OB",
                    "open": candle_open,
                    "high": candle_high,
                    "low": candle_low,
                    "entry": candle_open,
                    "timestamp": (
                        df.index[i].isoformat()
                    ),
                    "index": i,
                }
        else:
            bullish = (
                candle_close
                > candle_open
            )
            if not bullish:
                continue
            future_low = float(
                df["low"].iloc[
                    i + 1:
                :].min()
            )
            if future_low < candle_low:
                return {
                    "type": "BEARISH_OB",
                    "open": candle_open,
                    "high": candle_high,
                    "low": candle_low,
                    "entry": candle_open,
                    "timestamp": (
                        df.index[i].isoformat()
                    ),
                    "index": i,
                }
    return None
# ============================================================
# ENTRY ENGINE
# ============================================================
def determine_m1_entry(
    df: pd.DataFrame,
    direction: str,
) -> Dict[str, Any]:
    fvg = detect_fvg(
        df,
        direction,
    )
    if fvg:
        return {
            "type": "FVG",
            "price": float(
                fvg["entry"]
            ),
            "zone": fvg,
        }
    ob = detect_order_block(
        df,
        direction,
    )
    if ob:
        return {
            "type": "OB",
            "price": float(
                ob["entry"]
            ),
            "zone": ob,
        }
    return {
        "type": "MARKET_STRUCTURE",
        "price": float(
            df["close"].iloc[-1]
        ),
        "zone": None,
    }
# ============================================================
# SL / TP ENGINE
# ============================================================
def calculate_trade_levels(
    opportunity: Dict[str, Any],
    entry: float,
    manipulation_extreme: float,
) -> Optional[Dict[str, float]]:
    direction = opportunity[
        "direction"
    ]
    macro_target = float(
        opportunity[
            "macro_target"
        ]
    )
    if direction == "BUY":
        sl = float(
            manipulation_extreme
        )
        risk = (
            entry
            - sl
        )
        if risk <= 0:
            return None
        tp1 = (
            entry
            + risk
        )
        tp3 = (
            macro_target
            * (
                1.0
                - TP3_SAFETY_MARGIN
            )
        )
        if tp3 <= tp1:
            return None
        reward = (
            tp3
            - entry
        )
    else:
        sl = float(
            manipulation_extreme
        )
        risk = (
            sl
            - entry
        )
        if risk <= 0:
            return None
        tp1 = (
            entry
            - risk
        )
        tp3 = (
            macro_target
            * (
                1.0
                + TP3_SAFETY_MARGIN
            )
        )
        if tp3 >= tp1:
            return None
        reward = (
            entry
            - tp3
        )
    rr = (
        reward / risk
        if risk > 0
        else 0
    )
    if rr < MIN_RR:
        logger.info(
            "[RR FILTER] %s | %s | RR %.2f < %.2f",
            opportunity["symbol"],
            direction,
            rr,
            MIN_RR,
        )
        return None
    tp2 = (
        tp1
        + tp3
    ) / 2.0
    return {
        "entry": float(entry),
        "sl": float(sl),
        "tp1": float(tp1),
        "tp2": float(tp2),
        "tp3": float(tp3),
        "rr": float(rr),
        "risk": float(risk),
    }
# ============================================================
# M1 OPPORTUNITY VALIDATION
# ============================================================
def validate_m1_opportunity(
    symbol: str,
    df: pd.DataFrame,
) -> None:
    with JSON_LOCK:
        pending = load_pending()
        changed = False
        for key, opportunity in list(
            pending.items()
        ):
            if opportunity.get(
                "symbol"
            ) != symbol:
                continue
            if opportunity.get(
                "status"
            ) != "WAITING_M1_CHOCH":
                continue
            choch = detect_m1_choch(
                opportunity,
                df,
            )
            if not choch:
                continue
            opportunity[
                "m1_choch"
            ] = choch
            opportunity[
                "status"
            ] = "WAITING_M1_BOS"
            pending[key] = opportunity
            changed = True
            logger.info(
                "[M1 CHoCH] %s | %s | %.8f",
                symbol,
                opportunity["direction"],
                choch["price"],
            )
            send_telegram(
                format_choch_message(
                    opportunity
                )
            )
        if changed:
            save_pending(
                pending
            )
    validate_m1_bos_and_create_orders(
        symbol,
        df,
    )
def validate_m1_bos_and_create_orders(
    symbol: str,
    df: pd.DataFrame,
) -> None:
    with JSON_LOCK:
        pending = load_pending()
        changed = False
        for key, opportunity in list(
            pending.items()
        ):
            if opportunity.get(
                "symbol"
            ) != symbol:
                continue
            if opportunity.get(
                "status"
            ) != "WAITING_M1_BOS":
                continue
            choch = opportunity.get(
                "m1_choch"
            )
            if not choch:
                continue
            bos = detect_m1_bos(
                opportunity,
                df,
                choch,
            )
            if not bos:
                continue
            entry_data = determine_m1_entry(
                df,
                opportunity[
                    "direction"
                ],
            )
            entry = float(
                entry_data["price"]
            )
            manipulation_extreme = safe_float(
                opportunity.get(
                    "m5_liquidity_extreme"
                )
            )
            if manipulation_extreme is None:
                logger.warning(
                    "[M1] No manipulation extreme "
                    "for %s",
                    symbol,
                )
                opportunity[
                    "status"
                ] = "CANCELLED"
                changed = True
                continue
            levels = calculate_trade_levels(
                opportunity,
                entry,
                manipulation_extreme,
            )
            if levels is None:
                opportunity[
                    "status"
                ] = "CANCELLED_RR"
                opportunity[
                    "cancel_reason"
                ] = (
                    "TP3 RR below 1:3 "
                    "or invalid levels"
                )
                pending[key] = opportunity
                changed = True
                send_telegram(
                    format_rr_rejection_message(
                        opportunity
                    )
                )
                continue
            opportunity[
                "m1_bos"
            ] = bos
            opportunity[
                "entry_type"
            ] = entry_data[
                "type"
            ]
            opportunity[
                "entry_zone"
            ] = entry_data[
                "zone"
            ]
            opportunity[
                "entry"
            ] = levels[
                "entry"
            ]
            opportunity[
                "sl"
            ] = levels[
                "sl"
            ]
            opportunity[
                "tp1"
            ] = levels[
                "tp1"
            ]
            opportunity[
                "tp2"
            ] = levels[
                "tp2"
            ]
            opportunity[
                "tp3"
            ] = levels[
                "tp3"
            ]
            opportunity[
                "rr"
            ] = levels[
                "rr"
            ]
            opportunity[
                "risk"
            ] = levels[
                "risk"
            ]
            opportunity[
                "status"
            ] = "PENDING_LIMIT"
            opportunity[
                "limit_created_at"
            ] = iso_now()
            pending[key] = opportunity
            changed = True
            logger.info(
                "[LIMIT ORDER] %s | %s | "
                "Entry %.8f | SL %.8f | "
                "TP1 %.8f | TP2 %.8f | TP3 %.8f | RR %.2f",
                symbol,
                opportunity["direction"],
                opportunity["entry"],
                opportunity["sl"],
                opportunity["tp1"],
                opportunity["tp2"],
                opportunity["tp3"],
                opportunity["rr"],
            )
            send_telegram(
                format_limit_signal_message(
                    opportunity
                )
            )
        if changed:
            save_pending(
                pending
            )
# ============================================================
# PENDING LIMIT ORDER MONITOR
# ============================================================
def price_touches_entry(
    direction: str,
    candle: pd.Series,
    entry: float,
) -> bool:
    high = float(
        candle["high"]
    )
    low = float(
        candle["low"]
    )
    if direction == "BUY":
        return (
            low <= entry <= high
        )
    return (
        low <= entry <= high
    )
def promote_pending_orders(
    symbol: str,
    df: pd.DataFrame,
) -> None:
    if df.empty:
        return
    last_candle = df.iloc[-1]
    with JSON_LOCK:
        pending = load_pending()
        active = read_json(
            ACTIVE_FILE,
            {},
        )
        if not isinstance(
            active,
            dict,
        ):
            active = {}
        changed_pending = False
        changed_active = False
        for key, opportunity in list(
            pending.items()
        ):
            if opportunity.get(
                "symbol"
            ) != symbol:
                continue
            if opportunity.get(
                "status"
            ) != "PENDING_LIMIT":
                continue
            entry = safe_float(
                opportunity.get(
                    "entry"
                )
            )
            if entry is None:
                continue
            if not price_touches_entry(
                opportunity[
                    "direction"
                ],
                last_candle,
                entry,
            ):
                continue
            trade_id = (
                opportunity["id"]
            )
            trade = {
                "id": trade_id,
                "symbol": symbol,
                "direction": opportunity[
                    "direction"
                ],
                "status": "ACTIVE",
                "created_at": iso_now(),
                "entry_time": iso_now(),
                "entry": entry,
                "initial_entry": entry,
                "sl": float(
                    opportunity["sl"]
                ),
                "initial_sl": float(
                    opportunity["sl"]
                ),
                "tp1": float(
                    opportunity["tp1"]
                ),
                "tp2": float(
                    opportunity["tp2"]
                ),
                "tp3": float(
                    opportunity["tp3"]
                ),
                "rr": float(
                    opportunity["rr"]
                ),
                "tp1_hit": False,
                "tp2_hit": False,
                "tp3_hit": False,
                "break_even": False,
                "remaining_position": 1.0,
                "tp1_position_closed": 0.50,
                "tp2_position_closed": 0.25,
                "tp3_position_closed": 0.25,
                "macro_target": float(
                    opportunity[
                        "macro_target"
                    ]
                ),
                "polarity_type": opportunity[
                    "polarity_type"
                ],
                "m5_liquidity_extreme": float(
                    opportunity[
                        "m5_liquidity_extreme"
                    ]
                ),
                "entry_type": opportunity.get(
                    "entry_type"
                ),
                "source": opportunity.get(
                    "data_source"
                ),
            }
            active[
                trade_id
            ] = trade
            opportunity[
                "status"
            ] = "ACTIVE"
            opportunity[
                "activated_at"
            ] = iso_now()
            pending[key] = opportunity
            changed_pending = True
            changed_active = True
            logger.info(
                "[ORDER FILLED] %s | %s | Entry %.8f",
                symbol,
                trade["direction"],
                entry,
            )
            send_telegram(
                format_order_filled_message(
                    trade
                )
            )
        if changed_pending:
            save_pending(
                pending
            )
        if changed_active:
            write_json(
                ACTIVE_FILE,
                active,
            )
# ============================================================
# ACTIVE TRADE MONITOR
# ============================================================
def get_latest_price(
    symbol: str,
) -> Optional[float]:
    try:
        df = fetch_market_data_safe(
            symbol,
            "M1",
            limit=10,
        )
        if df.empty:
            return None
        return float(
            df["close"].iloc[-1]
        )
    except Exception as exc:
        logger.warning(
            "[PRICE MONITOR] %s | %s",
            symbol,
            exc,
        )
        return None
def close_trade(
    trade_id: str,
    trade: Dict[str, Any],
    exit_price: float,
    reason: str,
    quantity: float,
) -> None:
    quantity = max(
        0.0,
        min(
            1.0,
            quantity,
        ),
    )
    direction = trade[
        "direction"
    ]
    entry = float(
        trade["entry"]
    )
    if direction == "BUY":
        pnl = (
            exit_price
            - entry
        ) * quantity
    else:
        pnl = (
            entry
            - exit_price
        ) * quantity
    history_entry = dict(
        trade
    )
    history_entry[
        "exit_time"
    ] = iso_now()
    history_entry[
        "exit_price"
    ] = exit_price
    history_entry[
        "exit_reason"
    ] = reason
    history_entry[
        "closed_quantity"
    ] = quantity
    history_entry[
        "pnl_price_units"
    ] = pnl
    def history_updater(
        history
    ):
        if not isinstance(
            history,
            list,
        ):
            history = []
        history.append(
            history_entry
        )
        return history
    update_json(
        HISTORY_FILE,
        history_updater,
        [],
    )
def monitor_active_trade(
    trade_id: str,
    trade: Dict[str, Any],
    price: float,
) -> Tuple[
    Dict[str, Any],
    Optional[str],
]:
    direction = trade[
        "direction"
    ]
    event = None
    # --------------------------------------------------------
    # STOP LOSS
    # --------------------------------------------------------
    sl = float(
        trade["sl"]
    )
    if direction == "BUY":
        sl_hit = (
            price <= sl
        )
    else:
        sl_hit = (
            price >= sl
        )
    if sl_hit:
        remaining = float(
            trade[
                "remaining_position"
            ]
        )
        if remaining > 0:
            close_trade(
                trade_id,
                trade,
                price,
                "SL",
                remaining,
            )
        trade[
            "remaining_position"
        ] = 0.0
        trade[
            "status"
        ] = "CLOSED_SL"
        event = "SL"
        return (
            trade,
            event,
        )
    # --------------------------------------------------------
    # TP1
    # --------------------------------------------------------
    if not trade.get(
        "tp1_hit",
        False,
    ):
        tp1 = float(
            trade["tp1"]
        )
        if (
            price >= tp1
            if direction == "BUY"
            else price <= tp1
        ):
            partial = min(
                0.50,
                float(
                    trade[
                        "remaining_position"
                    ]
                ),
            )
            if partial > 0:
                close_trade(
                    trade_id,
                    trade,
                    tp1,
                    "TP1",
                    partial,
                )
                trade[
                    "remaining_position"
                ] -= partial
            trade[
                "tp1_hit"
            ] = True
            trade[
                "break_even"
            ] = True
            trade[
                "sl"
            ] = float(
                trade["entry"]
            )
            event = "TP1"
    # --------------------------------------------------------
    # TP2
    # --------------------------------------------------------
    if (
        trade.get(
            "tp1_hit",
            False,
        )
        and not trade.get(
            "tp2_hit",
            False,
        )
    ):
        tp2 = float(
            trade["tp2"]
        )
        if (
            price >= tp2
            if direction == "BUY"
            else price <= tp2
        ):
            partial = min(
                0.25,
                float(
                    trade[
                        "remaining_position"
                    ]
                ),
            )
            if partial > 0:
                close_trade(
                    trade_id,
                    trade,
                    tp2,
                    "TP2",
                    partial,
                )
                trade[
                    "remaining_position"
                ] -= partial
            trade[
                "tp2_hit"
            ] = True
            event = "TP2"
    # --------------------------------------------------------
    # TP3
    # --------------------------------------------------------
    if (
        trade.get(
            "tp2_hit",
            False,
        )
        and not trade.get(
            "tp3_hit",
            False,
        )
    ):
        tp3 = float(
            trade["tp3"]
        )
        if (
            price >= tp3
            if direction == "BUY"
            else price <= tp3
        ):
            remaining = float(
                trade[
                    "remaining_position"
                ]
            )
            if remaining > 0:
                close_trade(
                    trade_id,
                    trade,
                    tp3,
                    "TP3",
                    remaining,
                )
            trade[
                "remaining_position"
            ] = 0.0
            trade[
                "tp3_hit"
            ] = True
            trade[
                "status"
            ] = "CLOSED_TP3"
            event = "TP3"
    return (
        trade,
        event,
    )
def active_trade_monitor_loop() -> None:
    logger.info(
        "Active trade monitor started"
    )
    while not SHUTDOWN_EVENT.is_set():
        try:
            active = read_json(
                ACTIVE_FILE,
                {},
            )
            if not isinstance(
                active,
                dict,
            ):
                active = {}
            if not active:
                SHUTDOWN_EVENT.wait(
                    TRADE_MONITOR_INTERVAL_SECONDS
                )
                continue
            symbols = sorted(
                {
                    trade.get(
                        "symbol"
                    )
                    for trade in active.values()
                    if trade.get(
                        "status"
                    ) == "ACTIVE"
                }
            )
            prices = {}
            futures = {
                EXECUTOR.submit(
                    get_latest_price,
                    symbol,
                ): symbol
                for symbol in symbols
            }
            for future in as_completed(
                futures
            ):
                symbol = futures[
                    future
                ]
                try:
                    price = future.result()
                    if price is not None:
                        prices[
                            symbol
                        ] = price
                except Exception as exc:
                    logger.warning(
                        "Price future failed "
                        "%s: %s",
                        symbol,
                        exc,
                    )
            with JSON_LOCK:
                active = read_json(
                    ACTIVE_FILE,
                    {},
                )
                changed = False
                for trade_id, trade in list(
                    active.items()
                ):
                    if trade.get(
                        "status"
                    ) != "ACTIVE":
                        continue
                    symbol = trade.get(
                        "symbol"
                    )
                    price = prices.get(
                        symbol
                    )
                    if price is None:
                        continue
                    updated_trade, event = (
                        monitor_active_trade(
                            trade_id,
                            trade,
                            price,
                        )
                    )
                    active[
                        trade_id
                    ] = updated_trade
                    if event:
                        changed = True
                        if event == "TP1":
                            send_telegram(
                                format_tp1_message(
                                    updated_trade,
                                    price,
                                )
                            )
                        elif event == "TP2":
                            send_telegram(
                                format_tp2_message(
                                    updated_trade,
                                    price,
                                )
                            )
                        elif event == "TP3":
                            send_telegram(
                                format_tp3_message(
                                    updated_trade,
                                    price,
                                )
                            )
                        elif event == "SL":
                            send_telegram(
                                format_sl_message(
                                    updated_trade,
                                    price,
                                )
                            )
                    elif (
                        updated_trade.get(
                            "status"
                        )
                        != "ACTIVE"
                    ):
                        changed = True
                if changed:
                    write_json(
                        ACTIVE_FILE,
                        active,
                    )
        except Exception as exc:
            logger.exception(
                "Active monitor error: %s",
                exc,
            )
        SHUTDOWN_EVENT.wait(
            TRADE_MONITOR_INTERVAL_SECONDS
        )
# ============================================================
# TELEGRAM
# ============================================================
def telegram_url(
    method: str,
) -> str:
    return (
        "https://api.telegram.org"
        f"/bot{TELEGRAM_BOT_TOKEN}"
        f"/{method}"
    )
def send_telegram(
    message: str,
) -> bool:
    if not TELEGRAM_BOT_TOKEN:
        logger.warning(
            "Telegram token not configured"
        )
        return False
    if not TELEGRAM_CHAT_ID:
        logger.warning(
            "Telegram chat ID not configured"
        )
        return False
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "disable_web_page_preview": True,
    }
    try:
        response = HTTP.post(
            telegram_url(
                "sendMessage"
            ),
            json=payload,
            timeout=10,
        )
        response.raise_for_status()
        body = response.json()
        if not body.get(
            "ok",
            False,
        ):
            logger.error(
                "Telegram rejected message: %s",
                body,
            )
            return False
        return True
    except Exception as exc:
        logger.error(
            "Telegram error: %s",
            exc,
        )
        return False
def format_generation_message(
    opportunity: Dict[str, Any],
) -> str:
    return (
        "🟢 NOVA — POLARITY SIGNAL\n\n"
        f"Actif : {opportunity['symbol']}\n"
        f"Direction : {opportunity['direction']}\n"
        f"Polarité : "
        f"{opportunity['polarity_type']}\n"
        f"Trigger : "
        f"{format_price(opportunity['trigger_price'])}\n"
        f"Macro target : "
        f"{format_price(opportunity['macro_target'])}\n"
        f"Étape : M15 BOS → attente retest M5"
    )
def format_m5_retest_message(
    opportunity: Dict[str, Any],
) -> str:
    return (
        "🔵 NOVA — M5 RETEST VALIDÉ\n\n"
        f"Actif : {opportunity['symbol']}\n"
        f"Direction : {opportunity['direction']}\n"
        f"Polarité : "
        f"{format_price(opportunity['trigger_price'])}\n"
        f"Liquidité : "
        f"{format_price(opportunity['m5_liquidity_extreme'])}\n"
        f"Étape : WAITING_M1_CHOCH"
    )
def format_choch_message(
    opportunity: Dict[str, Any],
) -> str:
    choch = opportunity[
        "m1_choch"
    ]
    return (
        "🟣 NOVA — M1 CHoCH\n\n"
        f"Actif : {opportunity['symbol']}\n"
        f"Direction : {opportunity['direction']}\n"
        f"Prix CHoCH : "
        f"{format_price(choch['price'])}\n"
        f"Étape : attente BOS M1"
    )
def format_limit_signal_message(
    opportunity: Dict[str, Any],
) -> str:
    return (
        "🚨 NOVA — LIMIT ORDER\n\n"
        f"Actif : {opportunity['symbol']}\n"
        f"Direction : {opportunity['direction']}\n"
        f"Entrée : "
        f"{format_price(opportunity['entry'])}\n"
        f"SL : "
        f"{format_price(opportunity['sl'])}\n"
        f"TP1 : "
        f"{format_price(opportunity['tp1'])}\n"
        f"TP2 : "
        f"{format_price(opportunity['tp2'])}\n"
        f"TP3 : "
        f"{format_price(opportunity['tp3'])}\n"
        f"RR : 1:{opportunity['rr']:.2f}\n"
        f"Source : "
        f"{opportunity.get('data_source', 'AUTO')}"
    )
def format_order_filled_message(
    trade: Dict[str, Any],
) -> str:
    return (
        "✅ NOVA — ORDER FILLED\n\n"
        f"Actif : {trade['symbol']}\n"
        f"Direction : {trade['direction']}\n"
        f"Entrée : "
        f"{format_price(trade['entry'])}\n"
        f"SL : "
        f"{format_price(trade['sl'])}\n"
        f"TP1 : "
        f"{format_price(trade['tp1'])}\n"
        f"TP2 : "
        f"{format_price(trade['tp2'])}\n"
        f"TP3 : "
        f"{format_price(trade['tp3'])}"
    )
def format_tp1_message(
    trade: Dict[str, Any],
    price: float,
) -> str:
    return (
        "💰 NOVA — TP1 TOUCHÉ\n\n"
        f"Actif : {trade['symbol']}\n"
        f"Direction : {trade['direction']}\n"
        f"Prix : {format_price(price)}\n"
        "TP1 : 1R atteint\n"
        "50% sécurisé\n"
        "SL déplacé à Break-Even"
    )
def format_tp2_message(
    trade: Dict[str, Any],
    price: float,
) -> str:
    return (
        "💵 NOVA — TP2 TOUCHÉ\n\n"
        f"Actif : {trade['symbol']}\n"
        f"Direction : {trade['direction']}\n"
        f"Prix : {format_price(price)}\n"
        "25% supplémentaire encaissé\n"
        "Position restante : 25%"
    )
def format_tp3_message(
    trade: Dict[str, Any],
    price: float,
) -> str:
    return (
        "🏆 NOVA — TP3 TOUCHÉ\n\n"
        f"Actif : {trade['symbol']}\n"
        f"Direction : {trade['direction']}\n"
        f"Prix : {format_price(price)}\n"
        "Objectif macro atteint\n"
        "Position clôturée"
    )
def format_sl_message(
    trade: Dict[str, Any],
    price: float,
) -> str:
    return (
        "🛑 NOVA — STOP LOSS\n\n"
        f"Actif : {trade['symbol']}\n"
        f"Direction : {trade['direction']}\n"
        f"Prix : {format_price(price)}\n"
        f"SL : {format_price(trade['sl'])}\n"
        "Position clôturée"
    )
def format_rr_rejection_message(
    opportunity: Dict[str, Any],
) -> str:
    return (
        "⚪ NOVA — OPPORTUNITÉ ANNULÉE\n\n"
        f"Actif : {opportunity['symbol']}\n"
        f"Direction : {opportunity['direction']}\n"
        "Motif : RR TP3 inférieur à 1:3"
    )
def format_price(
    value: Any,
) -> str:
    number = safe_float(
        value
    )
    if number is None:
        return "N/A"
    if abs(number) >= 1000:
        return f"{number:,.2f}"
    if abs(number) >= 10:
        return f"{number:.4f}"
    return f"{number:.6f}"
# ============================================================
# ANALYSIS ENGINE
# ============================================================
def analyze_symbol(
    symbol: str,
) -> None:
    logger.info(
        "========== ANALYSE %s ==========",
        symbol,
    )
    try:
        # ----------------------------------------------------
        # M15
        # ----------------------------------------------------
        m15 = fetch_market_data_safe(
            symbol,
            "M15",
            500,
        )
        m15_opportunities = (
            detect_m15_polarity(
                symbol,
                m15,
            )
        )
        source_state = get_engine_state().get(
            f"source_{symbol}_M15",
            {},
        )
        active_source = source_state.get(
            "source",
            "UNKNOWN",
        )
        for opportunity in (
            m15_opportunities
        ):
            opportunity[
                "data_source"
            ] = active_source
        register_m15_opportunities(
            symbol,
            m15_opportunities,
        )
        # ----------------------------------------------------
        # M5
        # ----------------------------------------------------
        m5 = fetch_market_data_safe(
            symbol,
            "M5",
            500,
        )
        update_m5_retests(
            symbol,
            m5,
        )
        # ----------------------------------------------------
        # M1
        # ----------------------------------------------------
        m1 = fetch_market_data_safe(
            symbol,
            "M1",
            500,
        )
        validate_m1_opportunity(
            symbol,
            m1,
        )
        promote_pending_orders(
            symbol,
            m1,
        )
        set_engine_state(
            f"analysis_{symbol}",
            {
                "last_run": iso_now(),
                "status": "OK",
                "m15_candles": len(m15),
                "m5_candles": len(m5),
                "m1_candles": len(m1),
                "m15_source": get_engine_state().get(
                    f"source_{symbol}_M15",
                    {},
                ).get(
                    "source",
                    "UNKNOWN",
                ),
                "m5_source": get_engine_state().get(
                    f"source_{symbol}_M5",
                    {},
                ).get(
                    "source",
                    "UNKNOWN",
                ),
                "m1_source": get_engine_state().get(
                    f"source_{symbol}_M1",
                    {},
                ).get(
                    "source",
                    "UNKNOWN",
                ),
            },
        )
        logger.info(
            "========== FIN ANALYSE %s ==========",
            symbol,
        )
    except Exception as exc:
        logger.exception(
            "[ANALYSIS ERROR] %s | %s",
            symbol,
            exc,
        )
        set_engine_state(
            f"analysis_{symbol}",
            {
                "last_run": iso_now(),
                "status": "ERROR",
                "error": str(exc),
            },
        )
# ============================================================
# PARALLEL ANALYSIS LOOP
# ============================================================
def analysis_loop() -> None:
    logger.info(
        "Parallel analysis engine started"
    )
    while not SHUTDOWN_EVENT.is_set():
        started = time.monotonic()
        futures = []
        for symbol in SYMBOLS:
            future = EXECUTOR.submit(
                analyze_symbol,
                symbol,
            )
            futures.append(
                future
            )
        for future in futures:
            try:
                future.result()
            except Exception as exc:
                logger.exception(
                    "Asset analysis future failed: %s",
                    exc,
                )
        elapsed = (
            time.monotonic()
            - started
        )
        delay = max(
            1,
            ANALYSIS_INTERVAL_SECONDS
            - int(elapsed),
        )
        logger.info(
            "Analysis cycle completed "
            "in %.2fs | next cycle in %ss",
            elapsed,
            delay,
        )
        SHUTDOWN_EVENT.wait(
            delay
        )
# ============================================================
# CLEANUP OF STALE OPPORTUNITIES
# ============================================================
def cleanup_opportunities() -> None:
    max_age = timedelta(
        hours=24
    )
    now = utc_now()
    with JSON_LOCK:
        pending = load_pending()
        changed = False
        for key, opportunity in list(
            pending.items()
        ):
            created = opportunity.get(
                "created_at"
            )
            if not created:
                continue
            try:
                created_dt = (
                    datetime.fromisoformat(
                        created
                    )
                )
                if (
                    created_dt.tzinfo
                    is None
                ):
                    created_dt = (
                        created_dt.replace(
                            tzinfo=timezone.utc
                        )
                    )
                if (
                    now
                    - created_dt
                    > max_age
                ):
                    if opportunity.get(
                        "status"
                    ) not in {
                        "ACTIVE",
                        "CLOSED",
                    }:
                        opportunity[
                            "status"
                        ] = "EXPIRED"
                        pending[
                            key
                        ] = opportunity
                        changed = True
            except (
                ValueError,
                TypeError,
            ):
                continue
        if changed:
            save_pending(
                pending
            )
# ============================================================
# HOUSEKEEPING LOOP
# ============================================================
def housekeeping_loop() -> None:
    while not SHUTDOWN_EVENT.is_set():
        try:
            cleanup_opportunities()
        except Exception as exc:
            logger.exception(
                "Housekeeping error: %s",
                exc,
            )
        SHUTDOWN_EVENT.wait(
            300
        )
# ============================================================
# FLASK API
# ============================================================
@app.route(
    "/",
    methods=[
        "GET"
    ],
)
def home():
    return jsonify(
        {
            "application": APP_NAME,
            "status": "online",
            "timestamp": iso_now(),
            "symbols": SYMBOLS,
            "strategy": (
                "Trend Continuation "
                "by Structure Polarity"
            ),
            "timeframes": TIMEFRAMES,
        }
    )
@app.route(
    "/health",
    methods=[
        "GET"
    ],
)
def health():
    state = get_engine_state()
    return jsonify(
        {
            "status": "healthy",
            "timestamp": iso_now(),
            "engine": APP_NAME,
            "symbols": SYMBOLS,
            "shutdown": SHUTDOWN_EVENT.is_set(),
            "state": state,
        }
    )
@app.route(
    "/status",
    methods=[
        "GET"
    ],
)
def status():
    pending = load_pending()
    active = read_json(
        ACTIVE_FILE,
        {},
    )
    history = read_json(
        HISTORY_FILE,
        [],
    )
    active_count = len(
        [
            trade
            for trade in active.values()
            if trade.get(
                "status"
            ) == "ACTIVE"
        ]
    )
    pending_count = len(
        [
            opportunity
            for opportunity in pending.values()
            if opportunity.get(
                "status"
            ) in {
                "WAITING_M5_RETEST",
                "WAITING_M1_CHOCH",
                "WAITING_M1_BOS",
                "PENDING_LIMIT",
            }
        ]
    )
    return jsonify(
        {
            "status": "online",
            "timestamp": iso_now(),
            "pending_opportunities": pending_count,
            "active_trades": active_count,
            "history_count": len(
                history
                if isinstance(
                    history,
                    list,
                )
                else []
            ),
            "symbols": SYMBOLS,
        }
    )
@app.route(
    "/pending",
    methods=[
        "GET"
    ],
)
def pending_endpoint():
    return jsonify(
        load_pending()
    )
@app.route(
    "/active",
    methods=[
        "GET"
    ],
)
def active_endpoint():
    active = read_json(
        ACTIVE_FILE,
        {},
    )
    return jsonify(
        active
    )
@app.route(
    "/history",
    methods=[
        "GET"
    ],
)
def history_endpoint():
    history = read_json(
        HISTORY_FILE,
        [],
    )
    return jsonify(
        history
    )
# ============================================================
# TELEGRAM CONNECTIVITY TEST
# ============================================================
def telegram_get_me() -> bool:
    if not TELEGRAM_BOT_TOKEN:
        return False
    try:
        response = HTTP.get(
            telegram_url(
                "getMe"
            ),
            timeout=10,
        )
        response.raise_for_status()
        data = response.json()
        if data.get(
            "ok",
            False,
        ):
            bot = data.get(
                "result",
                {},
            )
            logger.info(
                "Telegram connected: @%s",
                bot.get(
                    "username",
                    "unknown",
                ),
            )
            return True
        return False
    except Exception as exc:
        logger.error(
            "Telegram getMe failed: %s",
            exc,
        )
        return False
# ============================================================
# STARTUP
# ============================================================
def initialize_engine() -> None:
    logger.info(
        "=========================================="
    )
    logger.info(
        "%s",
        APP_NAME,
    )
    logger.info(
        "Strategy: Structure Polarity"
    )
    logger.info(
        "Symbols: %s",
        ", ".join(
            SYMBOLS
        ),
    )
    logger.info(
        "Timeframes: M15 / M5 / M1"
    )
    logger.info(
        "BiQuote: %s",
        (
            "configured"
            if BIQUOTE_BASE_URL
            else "missing"
        ),
    )
    logger.info(
        "Tiingo: %s",
        (
            "configured"
            if TIINGO_TOKEN
            else "not configured"
        ),
    )
    logger.info(
        "Telegram: %s",
        (
            "configured"
            if TELEGRAM_BOT_TOKEN
            and TELEGRAM_CHAT_ID
            else "not configured"
        ),
    )
    telegram_get_me()
    set_engine_state(
        "startup",
        {
            "timestamp": iso_now(),
            "status": "RUNNING",
            "application": APP_NAME,
        },
    )
    logger.info(
        "=========================================="
    )
# ============================================================
# BACKGROUND THREAD STARTER
# ============================================================
BACKGROUND_THREADS = []
def start_background_threads() -> None:
    analysis_thread = threading.Thread(
        target=analysis_loop,
        name="NOVA-Analysis",
        daemon=True,
    )
    trade_thread = threading.Thread(
        target=active_trade_monitor_loop,
        name="NOVA-TradeMonitor",
        daemon=True,
    )
    housekeeping_thread = threading.Thread(
        target=housekeeping_loop,
        name="NOVA-Housekeeping",
        daemon=True,
    )
    BACKGROUND_THREADS.extend(
        [
            analysis_thread,
            trade_thread,
            housekeeping_thread,
        ]
    )
    for thread in BACKGROUND_THREADS:
        thread.start()
        logger.info(
            "Background thread started: %s",
            thread.name,
        )
# ============================================================
# SHUTDOWN
# ============================================================
def shutdown_engine() -> None:
    if SHUTDOWN_EVENT.is_set():
        return
    logger.info(
        "NOVA shutdown requested"
    )
    SHUTDOWN_EVENT.set()
    set_engine_state(
        "shutdown",
        {
            "timestamp": iso_now(),
            "status": "STOPPED",
        },
    )
    try:
        EXECUTOR.shutdown(
            wait=False,
            cancel_futures=True,
        )
    except TypeError:
        EXECUTOR.shutdown(
            wait=False
        )
# ============================================================
# MAIN
# ============================================================
def main() -> None:
    initialize_engine()
    start_background_threads()
    try:
        app.run(
            host="0.0.0.0",
            port=PORT,
            debug=False,
            threaded=True,
            use_reloader=False,
        )
    except KeyboardInterrupt:
        logger.info(
            "Keyboard interrupt received"
        )
    finally:
        shutdown_engine()
if __name__ == "__main__":
    main()