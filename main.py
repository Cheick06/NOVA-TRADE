"""
NOVA POLARITY ENGINE
====================

Moteur de trading multi-actifs basé sur :
    - Continuité de tendance
    - Polarité des structures
    - Price Action
    - BOS / CHoCH
    - Liquidité
    - FVG / OB
    - Multi-timeframe M15 -> M5 -> M1
    - Filtre ATR
    - Filtre de force de bougie
    - Gestion TP1 / TP2 / TP3
    - Break-Even après TP1
    - Persistance JSON thread-safe
    - Telegram
    - Flask
    - ThreadPoolExecutor

Actifs :
    BTCUSD
    XAUUSD
    EURUSD
    GBPUSD

IMPORTANT :
Ce moteur ne transmet aucun ordre à un broker.
Il génère et simule les ordres limites et leur suivi à partir des
données de marché publiques.

Variables d'environnement principales :

    PORT=5000

    BIQUOTE_BASE_URL=https://biquote.io/api
    BIQUOTE_API_KEY=

    TIINGO_API_TOKEN=

    TELEGRAM_BOT_TOKEN=
    TELEGRAM_CHAT_ID=

    DATA_TIMEOUT=5

Les fichiers JSON sont créés automatiquement dans le répertoire courant.
"""

from __future__ import annotations

import os
import json
import time
import math
import logging
import threading
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests

from flask import Flask, jsonify

# ============================================================================
# CONFIGURATION
# ============================================================================

APP_NAME = "NOVA POLARITY ENGINE"

PORT = int(os.getenv("PORT", "5000"))

DATA_TIMEOUT = float(os.getenv("DATA_TIMEOUT", "5"))
ANALYSIS_INTERVAL = float(os.getenv("ANALYSIS_INTERVAL", "20"))
TRADE_MONITOR_INTERVAL = float(os.getenv("TRADE_MONITOR_INTERVAL", "2"))

MIN_CANDLES = 500

SYMBOLS = [
    "BTCUSD",
    "XAUUSD",
    "EURUSD",
    "GBPUSD",
]

TIMEFRAMES = {
    "M15": "15m",
    "M5": "5m",
    "M1": "1m",
}

# ---------------------------------------------------------------------------
# Polarité / structure
# ---------------------------------------------------------------------------

ATR_PERIOD = 14
BOS_ATR_MULTIPLIER = 0.5

STRUCTURE_LOOKBACK = 5

# CHoCH institutionnel
CHOCH_BODY_RATIO_MIN = 0.60

# Marge de sécurité autour du TP3
TP3_SAFETY_MARGIN = 0.0002

# RR minimum
MIN_GLOBAL_RR = 3.0

# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

PENDING_FILE = "pending_opportunities.json"
ACTIVE_FILE = "active_trades.json"
HISTORY_FILE = "trade_history.json"
PROCESSED_FILE = "processed_signals.json"

JSON_LOCK = threading.RLock()

# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------

BIQUOTE_BASE_URL = os.getenv(
    "BIQUOTE_BASE_URL",
    "https://biquote.io/api"
).rstrip("/")

BIQUOTE_API_KEY = os.getenv("BIQUOTE_API_KEY", "").strip()

TIINGO_API_TOKEN = os.getenv(
    "TIINGO_API_TOKEN",
    ""
).strip()

TELEGRAM_BOT_TOKEN = os.getenv(
    "TELEGRAM_BOT_TOKEN",
    ""
).strip()

TELEGRAM_CHAT_ID = os.getenv(
    "TELEGRAM_CHAT_ID",
    ""
).strip()

# ============================================================================
# LOGGING
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(threadName)s | %(message)s",
)

logger = logging.getLogger(APP_NAME)

# ============================================================================
# FLASK
# ============================================================================

app = Flask(__name__)

# ============================================================================
# THREADS
# ============================================================================

executor = ThreadPoolExecutor(
    max_workers=max(8, len(SYMBOLS) + 4),
    thread_name_prefix="nova-worker",
)

shutdown_event = threading.Event()

# Verrou empêchant plusieurs analyses simultanées du même symbole.
SYMBOL_LOCKS: Dict[str, threading.RLock] = {
    symbol: threading.RLock()
    for symbol in SYMBOLS
}

# Verrou spécifique au suivi des trades.
TRADE_LOCK = threading.RLock()

# ============================================================================
# OUTILS TEMPS
# ============================================================================


def utc_now() -> datetime:
    """Retourne l'heure UTC timezone-aware."""
    return datetime.now(timezone.utc)


def utc_iso() -> str:
    """Retourne l'heure UTC au format ISO."""
    return utc_now().isoformat()


def normalize_symbol(symbol: str) -> str:
    """Normalise le symbole."""
    return symbol.upper().replace("/", "").replace("-", "").strip()


# ============================================================================
# PERSISTANCE JSON THREAD-SAFE
# ============================================================================


def _atomic_write_json(path: str, data: Any) -> None:
    """
    Écriture atomique :
        fichier temporaire -> replace

    Cela évite de laisser un JSON partiellement écrit si le processus
    est interrompu pendant l'écriture.
    """

    temp_path = f"{path}.tmp"

    with open(
        temp_path,
        "w",
        encoding="utf-8",
    ) as handle:
        json.dump(
            data,
            handle,
            indent=2,
            ensure_ascii=False,
        )

    os.replace(temp_path, path)


def load_json(path: str, default: Any) -> Any:
    """Lecture JSON thread-safe."""

    with JSON_LOCK:

        try:

            if not os.path.exists(path):
                return default

            with open(
                path,
                "r",
                encoding="utf-8",
            ) as handle:
                return json.load(handle)

        except Exception as exc:

            logger.exception(
                "Erreur lecture JSON %s : %s",
                path,
                exc,
            )

            return default


def save_json(path: str, data: Any) -> bool:
    """Sauvegarde JSON thread-safe."""

    with JSON_LOCK:

        try:

            _atomic_write_json(path, data)

            return True

        except Exception as exc:

            logger.exception(
                "Erreur écriture JSON %s : %s",
                path,
                exc,
            )

            return False


def update_json_list(
    path: str,
    item: Dict[str, Any],
) -> bool:
    """Ajoute un élément à une liste JSON."""

    with JSON_LOCK:

        data = load_json(path, [])

        if not isinstance(data, list):
            data = []

        data.append(item)

        return save_json(path, data)


# ============================================================================
# HTTP
# ============================================================================


HTTP_SESSION = requests.Session()

HTTP_SESSION.headers.update(
    {
        "User-Agent": (
            "NOVA-Polarity-Engine/1.0 "
            "(Market Data Client)"
        )
    }
)


def http_get(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: float = DATA_TIMEOUT,
) -> Optional[requests.Response]:
    """
    GET robuste.

    Aucun appel ne doit bloquer le moteur indéfiniment.
    """

    try:

        response = HTTP_SESSION.get(
            url,
            params=params,
            headers=headers,
            timeout=timeout,
        )

        response.raise_for_status()

        return response

    except Exception as exc:

        logger.warning(
            "HTTP GET échoué | %s | %s",
            url,
            exc,
        )

        return None


# ============================================================================
# NORMALISATION DATAFRAME
# ============================================================================


REQUIRED_COLUMNS = [
    "open",
    "high",
    "low",
    "close",
    "volume",
]


def normalize_dataframe(
    df: pd.DataFrame,
    limit: int = MIN_CANDLES,
) -> pd.DataFrame:
    """
    Normalise toutes les sources vers :

        ['open', 'high', 'low', 'close', 'volume']

    Timestamp en index.
    """

    if df is None or df.empty:
        return pd.DataFrame(
            columns=REQUIRED_COLUMNS
        )


    result = df.copy()

    # ----------------------------------------------------------------------
    # Flatten MultiIndex
    # ----------------------------------------------------------------------

    if isinstance(result.columns, pd.MultiIndex):

        result.columns = [
            str(column[0]).lower()
            if isinstance(column, tuple)
            else str(column).lower()
            for column in result.columns
        ]

    else:

        result.columns = [
            str(column).lower().strip()
            for column in result.columns
        ]

    # ----------------------------------------------------------------------
    # Recherche timestamp
    # ----------------------------------------------------------------------

    timestamp_candidates = [
        "timestamp",
        "datetime",
        "date",
        "time",
        "t",
    ]

    timestamp_column = None

    for candidate in timestamp_candidates:

        if candidate in result.columns:

            timestamp_column = candidate

            break

    if timestamp_column is not None:

        result[timestamp_column] = pd.to_datetime(
            result[timestamp_column],
            errors="coerce",
            utc=True,
        )

        result = result.set_index(
            timestamp_column
        )

    else:

        result.index = pd.to_datetime(
            result.index,
            errors="coerce",
            utc=True,
        )

    # ----------------------------------------------------------------------
    # OHLCV
    # ----------------------------------------------------------------------

    aliases = {
        "o": "open",
        "h": "high",
        "l": "low",
        "c": "close",
        "v": "volume",
    }

    result = result.rename(
        columns=aliases
    )

    # ----------------------------------------------------------------------
    # Colonnes manquantes
    # ----------------------------------------------------------------------

    for column in REQUIRED_COLUMNS:

        if column not in result.columns:

            result[column] = np.nan

    result = result[
        REQUIRED_COLUMNS
    ]

    # ----------------------------------------------------------------------
    # Numérique
    # ----------------------------------------------------------------------

    for column in REQUIRED_COLUMNS:

        result[column] = pd.to_numeric(
            result[column],
            errors="coerce",
        )

    result = result.dropna(
        subset=[
            "open",
            "high",
            "low",
            "close",
        ]
    )

    # ----------------------------------------------------------------------
    # Index
    # ----------------------------------------------------------------------

    result = result[
        ~result.index.isna()
    ]

    result.index = pd.to_datetime(
        result.index,
        utc=True,
    )

    result = result[
        ~result.index.duplicated(
            keep="last"
        )
    ]

    result = result.sort_index()

    if limit > 0:
        result = result.tail(limit)

    return result


# ============================================================================
# SOURCE 1 — BIQUOTE
# ============================================================================


def biquote_symbol(symbol: str) -> str:
    """
    Format symbole pour BiQuote.

    La fonction reste volontairement simple car les installations
    BiQuote peuvent exposer plusieurs formats d'endpoint.
    """

    return normalize_symbol(symbol)


def biquote_timeframe(timeframe: str) -> str:
    """Conversion timeframe interne -> API."""

    return TIMEFRAMES.get(
        timeframe.upper(),
        timeframe,
    )


def fetch_biquote(
    symbol: str,
    timeframe: str,
    limit: int = MIN_CANDLES,
) -> pd.DataFrame:
    """
    Source principale.

    Plusieurs formats de réponses JSON sont supportés :
        data
        candles
        result
        rows
        liste directe
    """

    endpoint_candidates = [
        f"{BIQUOTE_BASE_URL}/ohlcv",
        f"{BIQUOTE_BASE_URL}/candles",
        f"{BIQUOTE_BASE_URL}/market/ohlcv",
    ]

    params = {
        "symbol": biquote_symbol(symbol),
        "timeframe": biquote_timeframe(timeframe),
        "limit": max(limit, MIN_CANDLES),
    }

    headers: Dict[str, str] = {}

    if BIQUOTE_API_KEY:

        headers["Authorization"] = (
            f"Bearer {BIQUOTE_API_KEY}"
        )

        headers["X-API-Key"] = BIQUOTE_API_KEY

    for endpoint in endpoint_candidates:

        try:

            response = http_get(
                endpoint,
                params=params,
                headers=headers,
                timeout=DATA_TIMEOUT,
            )

            if response is None:
                continue

            payload = response.json()

            data = payload

            if isinstance(payload, dict):

                for key in (
                    "data",
                    "candles",
                    "result",
                    "rows",
                    "ohlcv",
                ):

                    if key in payload:

                        data = payload[key]

                        break

            if isinstance(data, dict):

                for key in (
                    "data",
                    "candles",
                    "rows",
                    "result",
                ):

                    if key in data:

                        data = data[key]

                        break

            if not isinstance(data, list):
                continue

            # ----------------------------------------------------------------
            # Liste de dictionnaires
            # ----------------------------------------------------------------

            if data and isinstance(data[0], dict):

                df = pd.DataFrame(data)

            # ----------------------------------------------------------------
            # Liste OHLCV
            # ----------------------------------------------------------------

            else:

                rows = []

                for row in data:

                    if not isinstance(row, (list, tuple)):
                        continue

                    if len(row) >= 6:

                        rows.append(
                            {
                                "timestamp": row[0],
                                "open": row[1],
                                "high": row[2],
                                "low": row[3],
                                "close": row[4],
                                "volume": row[5],
                            }
                        )

                df = pd.DataFrame(rows)

            df = normalize_dataframe(
                df,
                limit=limit,
            )

            if len(df) >= MIN_CANDLES:

                logger.info(
                    "[BIQUOTE OK] %s %s | %s bougies",
                    symbol,
                    timeframe,
                    len(df),
                )

                return df

            logger.warning(
                "[BIQUOTE INSUFFISANT] %s %s | %s bougies",
                symbol,
                timeframe,
                len(df),
            )

        except Exception as exc:

            logger.warning(
                "[BIQUOTE ERROR] %s %s | %s",
                symbol,
                timeframe,
                exc,
            )

    return pd.DataFrame(
        columns=REQUIRED_COLUMNS
    )


# ============================================================================
# SOURCE 2A — KRAKEN
# ============================================================================


def kraken_pair(symbol: str) -> Optional[str]:
    """Mapping crypto NOVA -> Kraken."""

    symbol = normalize_symbol(symbol)

    mapping = {
        "BTCUSD": "XBTUSD",
    }

    return mapping.get(symbol)


def kraken_interval(timeframe: str) -> Optional[int]:
    """Kraken OHLC interval en minutes."""

    mapping = {
        "M1": 1,
        "M5": 5,
        "M15": 15,
    }

    return mapping.get(
        timeframe.upper()
    )


def fetch_kraken(
    symbol: str,
    timeframe: str,
    limit: int = MIN_CANDLES,
) -> pd.DataFrame:
    """Secours crypto."""

    pair = kraken_pair(symbol)

    interval = kraken_interval(
        timeframe
    )

    if not pair or not interval:

        return pd.DataFrame(
            columns=REQUIRED_COLUMNS
        )

    # Kraken possède une profondeur limitée selon l'endpoint.
    # On demande le maximum utile disponible.
    url = (
        "https://api.kraken.com/0/public/OHLC"
    )

    try:

        response = http_get(
            url,
            params={
                "pair": pair,
                "interval": interval,
            },
            timeout=DATA_TIMEOUT,
        )

        if response is None:
            return pd.DataFrame(
                columns=REQUIRED_COLUMNS
            )

        payload = response.json()

        if payload.get("error"):
            raise RuntimeError(
                str(payload["error"])
            )

        result = payload.get(
            "result",
            {}
        )

        rows = None

        for key, value in result.items():

            if key != "last":
                rows = value
                break

        if not rows:
            return pd.DataFrame(
                columns=REQUIRED_COLUMNS
            )

        records = []

        for row in rows:

            if len(row) < 7:
                continue

            records.append(
                {
                    "timestamp": pd.to_datetime(
                        float(row[0]),
                        unit="s",
                        utc=True,
                    ),
                    "open": row[1],
                    "high": row[2],
                    "low": row[3],
                    "close": row[4],
                    "volume": row[6],
                }
            )

        df = normalize_dataframe(
            pd.DataFrame(records),
            limit=limit,
        )

        if not df.empty:

            logger.info(
                "[KRAKEN OK] %s %s | %s bougies",
                symbol,
                timeframe,
                len(df),
            )

        return df

    except Exception as exc:

        logger.warning(
            "[KRAKEN ERROR] %s %s | %s",
            symbol,
            timeframe,
            exc,
        )

        return pd.DataFrame(
            columns=REQUIRED_COLUMNS
        )


# ============================================================================
# SOURCE 2B — TIINGO
# ============================================================================


def tiingo_symbol(symbol: str) -> str:
    """Mapping Forex/Or."""

    mapping = {
        "EURUSD": "eurusd",
        "GBPUSD": "gbpusd",
        "XAUUSD": "xauusd",
    }

    return mapping.get(
        normalize_symbol(symbol),
        normalize_symbol(symbol).lower(),
    )


def tiingo_resample(timeframe: str) -> str:
    """Tiingo resample frequency."""

    mapping = {
        "M1": "1min",
        "M5": "5min",
        "M15": "15min",
    }

    return mapping.get(
        timeframe.upper(),
        "15min",
    )


def fetch_tiingo(
    symbol: str,
    timeframe: str,
    limit: int = MIN_CANDLES,
) -> pd.DataFrame:
    """Secours Forex/Or."""

    if not TIINGO_API_TOKEN:

        logger.warning(
            "[TIINGO] TIINGO_API_TOKEN absent"
        )

        return pd.DataFrame(
            columns=REQUIRED_COLUMNS
        )

    ticker = tiingo_symbol(symbol)

    url = (
        f"https://api.tiingo.com/tiingo/fx/"
        f"{ticker}/prices"
    )

    end_date = utc_now()

    # Marge temporelle suffisante pour récupérer au moins 500 bougies.
    minutes = {
        "M1": 1,
        "M5": 5,
        "M15": 15,
    }.get(
        timeframe.upper(),
        15,
    )

    start_date = (
        end_date
        - pd.Timedelta(
            minutes=minutes * (limit + 100)
        )
    )

    params = {
        "startDate": start_date.strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        ),
        "endDate": end_date.strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        ),
        "resampleFreq": tiingo_resample(
            timeframe
        ),
        "columns": (
            "date,open,high,low,close,"
            "volume"
        ),
    }

    headers = {
        "Authorization": (
            f"Token {TIINGO_API_TOKEN}"
        ),
        "Content-Type": "application/json",
    }

    try:

        response = http_get(
            url,
            params=params,
            headers=headers,
            timeout=DATA_TIMEOUT,
        )

        if response is None:
            return pd.DataFrame(
                columns=REQUIRED_COLUMNS
            )

        payload = response.json()

        if not isinstance(payload, list):
            return pd.DataFrame(
                columns=REQUIRED_COLUMNS
            )

        df = normalize_dataframe(
            pd.DataFrame(payload),
            limit=limit,
        )

        if not df.empty:

            logger.info(
                "[TIINGO OK] %s %s | %s bougies",
                symbol,
                timeframe,
                len(df),
            )

        return df

    except Exception as exc:

        logger.warning(
            "[TIINGO ERROR] %s %s | %s",
            symbol,
            timeframe,
            exc,
        )

        return pd.DataFrame(
            columns=REQUIRED_COLUMNS
        )


# ============================================================================
# SOURCE 3 — YAHOO FINANCE HTTP DIRECT
# ============================================================================


def yahoo_symbol(symbol: str) -> str:
    """
    Mapping Yahoo.

    BTC :
        BTC-USD

    Or :
        GC=F

    Forex :
        EURUSD=X
        GBPUSD=X
    """

    mapping = {
        "BTCUSD": "BTC-USD",
        "XAUUSD": "GC=F",
        "EURUSD": "EURUSD=X",
        "GBPUSD": "GBPUSD=X",
    }

    return mapping.get(
        normalize_symbol(symbol),
        normalize_symbol(symbol),
    )


def yahoo_interval(timeframe: str) -> str:
    mapping = {
        "M1": "1m",
        "M5": "5m",
        "M15": "15m",
    }

    return mapping.get(
        timeframe.upper(),
        "15m",
    )


def yahoo_period_range(
    timeframe: str,
    limit: int,
) -> Tuple[int, int]:
    """
    Yahoo impose des limites sur certains intervalles.
    On demande une période raisonnable avec une marge.
    """

    interval_minutes = {
        "M1": 1,
        "M5": 5,
        "M15": 15,
    }.get(
        timeframe.upper(),
        15,
    )

    seconds = (
        interval_minutes
        * 60
        * (limit + 100)
    )

    end = int(
        utc_now().timestamp()
    )

    start = end - seconds

    return start, end


def fetch_yahoo(
    symbol: str,
    timeframe: str,
    limit: int = MIN_CANDLES,
) -> pd.DataFrame:
    """Yahoo Finance HTTP direct."""

    ticker = yahoo_symbol(symbol)

    start, end = yahoo_period_range(
        timeframe,
        limit,
    )

    url = (
        "https://query1.finance.yahoo.com/"
        f"v8/finance/chart/{ticker}"
    )

    params = {
        "period1": start,
        "period2": end,
        "interval": yahoo_interval(
            timeframe
        ),
        "events": "history",
        "includeAdjustedClose": "true",
    }

    try:

        response = http_get(
            url,
            params=params,
            timeout=DATA_TIMEOUT,
        )

        if response is None:
            return pd.DataFrame(
                columns=REQUIRED_COLUMNS
            )

        payload = response.json()

        chart = payload.get(
            "chart",
            {}
        )

        results = chart.get(
            "result"
        )

        if not results:
            return pd.DataFrame(
                columns=REQUIRED_COLUMNS
            )

        result = results[0]

        timestamps = result.get(
            "timestamp",
            []
        )

        indicators = result.get(
            "indicators",
            {}
        )

        quote = (
            indicators
            .get("quote", [{}])[0]
        )

        records = []

        opens = quote.get("open", [])
        highs = quote.get("high", [])
        lows = quote.get("low", [])
        closes = quote.get("close", [])
        volumes = quote.get("volume", [])

        size = min(
            len(timestamps),
            len(opens),
            len(highs),
            len(lows),
            len(closes),
        )

        for i in range(size):

            records.append(
                {
                    "timestamp": pd.to_datetime(
                        timestamps[i],
                        unit="s",
                        utc=True,
                    ),
                    "open": opens[i],
                    "high": highs[i],
                    "low": lows[i],
                    "close": closes[i],
                    "volume": (
                        volumes[i]
                        if i < len(volumes)
                        else 0
                    ),
                }
            )

        df = normalize_dataframe(
            pd.DataFrame(records),
            limit=limit,
        )

        if not df.empty:

            logger.info(
                "[YAHOO OK] %s %s | %s bougies | Prix %.6f | Volume %.2f",
                symbol,
                timeframe,
                len(df),
                float(df["close"].iloc[-1]),
                float(df["volume"].iloc[-1]),
            )

        return df

    except Exception as exc:

        logger.warning(
            "[YAHOO ERROR] %s %s | %s",
            symbol,
            timeframe,
            exc,
        )

        return pd.DataFrame(
            columns=REQUIRED_COLUMNS
        )


# ============================================================================
# CASCADE DE DONNÉES
# ============================================================================


def fetch_market_data_safe(
    symbol: str,
    timeframe: str,
    limit: int = MIN_CANDLES,
) -> pd.DataFrame:
    """
    Cascade :

        1. BiQuote
        2. Kraken si BTC
           Tiingo si Forex/Or
        3. Yahoo Finance HTTP

    Retourne toujours un DataFrame normalisé.
    """

    symbol = normalize_symbol(symbol)
    timeframe = timeframe.upper()

    # ----------------------------------------------------------------------
    # SOURCE 1
    # ----------------------------------------------------------------------

    try:

        df = fetch_biquote(
            symbol,
            timeframe,
            limit=max(limit, MIN_CANDLES),
        )

        if len(df) >= MIN_CANDLES:

            df.attrs["source"] = "BIQUOTE"

            return df

    except Exception as exc:

        logger.exception(
            "BiQuote exception | %s %s | %s",
            symbol,
            timeframe,
            exc,
        )

    # ----------------------------------------------------------------------
    # SOURCE 2
    # ----------------------------------------------------------------------

    try:

        if symbol == "BTCUSD":

            df = fetch_kraken(
                symbol,
                timeframe,
                limit=max(limit, MIN_CANDLES),
            )

            if len(df) >= MIN_CANDLES:

                df.attrs["source"] = "KRAKEN"

                return df

        else:

            df = fetch_tiingo(
                symbol,
                timeframe,
                limit=max(limit, MIN_CANDLES),
            )

            if len(df) >= MIN_CANDLES:

                df.attrs["source"] = "TIINGO"

                return df

    except Exception as exc:

        logger.exception(
            "Source secours 1 exception | %s %s | %s",
            symbol,
            timeframe,
            exc,
        )

    # ----------------------------------------------------------------------
    # SOURCE 3
    # ----------------------------------------------------------------------

    try:

        df = fetch_yahoo(
            symbol,
            timeframe,
            limit=max(limit, MIN_CANDLES),
        )

        if len(df) >= MIN_CANDLES:

            df.attrs["source"] = "YAHOO"

            return df

        # Yahoo peut retourner moins de bougies à cause de ses
        # limitations historiques. Le moteur préfère néanmoins
        # conserver les données si elles sont exploitables.
        if len(df) >= 100:

            df.attrs["source"] = "YAHOO_PARTIAL"

            return df

    except Exception as exc:

        logger.exception(
            "Yahoo exception | %s %s | %s",
            symbol,
            timeframe,
            exc,
        )

    logger.error(
        "[DATA FAILURE] %s %s : aucune source exploitable",
        symbol,
        timeframe,
    )

    empty = pd.DataFrame(
        columns=REQUIRED_COLUMNS
    )

    empty.attrs["source"] = "NONE"

    return empty


# ============================================================================
# INDICATEURS
# ============================================================================


def calculate_atr(
    df: pd.DataFrame,
    period: int = ATR_PERIOD,
) -> pd.Series:
    """
    ATR vectorisé.

    True Range :

        max(
            high-low,
            abs(high-close précédent),
            abs(low-close précédent)
        )
    """

    if df is None or df.empty:

        return pd.Series(
            dtype=float
        )

    high = df["high"]
    low = df["low"]
    close = df["close"]

    previous_close = close.shift(1)

    tr = pd.concat(
        [
            high - low,
            (high - previous_close).abs(),
            (low - previous_close).abs(),
        ],
        axis=1,
    ).max(axis=1)

    return tr.rolling(
        period,
        min_periods=period,
    ).mean()


def candle_body_ratio(
    row: pd.Series,
) -> float:
    """Ratio corps / amplitude."""

    high = float(row["high"])
    low = float(row["low"])
    open_price = float(row["open"])
    close_price = float(row["close"])

    total_range = high - low

    if total_range <= 0:
        return 0.0

    return abs(
        close_price - open_price
    ) / total_range


def add_body_ratio(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """Ajoute le ratio corps vectorisé."""

    result = df.copy()

    total_range = (
        result["high"]
        - result["low"]
    ).replace(0, np.nan)

    result["body_ratio"] = (
        (
            result["close"]
            - result["open"]
        ).abs()
        / total_range
    ).fillna(0.0)

    return result


# ============================================================================
# STRUCTURE M15
# ============================================================================


def detect_pivot_highs(
    df: pd.DataFrame,
    window: int = STRUCTURE_LOOKBACK,
) -> pd.Series:
    """Détection vectorisée des sommets majeurs."""

    high = df["high"]

    rolling_max = high.rolling(
        window=window * 2 + 1,
        center=True,
        min_periods=window * 2 + 1,
    ).max()

    return high.eq(
        rolling_max
    )


def detect_pivot_lows(
    df: pd.DataFrame,
    window: int = STRUCTURE_LOOKBACK,
) -> pd.Series:
    """Détection vectorisée des creux majeurs."""

    low = df["low"]

    rolling_min = low.rolling(
        window=window * 2 + 1,
        center=True,
        min_periods=window * 2 + 1,
    ).min()

    return low.eq(
        rolling_min
    )


def get_major_structures(
    df: pd.DataFrame,
) -> Tuple[
    pd.DataFrame,
    pd.DataFrame,
]:
    """
    Retourne les pivots majeurs confirmés.

    Les pivots situés sur la dernière bougie ouverte sont exclus.
    """

    if len(df) < 50:

        return (
            pd.DataFrame(),
            pd.DataFrame(),
        )

    work = df.copy()

    work["pivot_high"] = detect_pivot_highs(
        work
    )

    work["pivot_low"] = detect_pivot_lows(
        work
    )

    # Seule la partie clôturée est exploitable.
    confirmed = work.iloc[:-1].copy()

    highs = confirmed[
        confirmed["pivot_high"]
    ][
        ["high"]
    ].copy()

    lows = confirmed[
        confirmed["pivot_low"]
    ][
        ["low"]
    ].copy()

    return highs, lows


# ============================================================================
# BOS M15
# ============================================================================


def detect_m15_bos(
    df: pd.DataFrame,
) -> Optional[Dict[str, Any]]:
    """
    Détecte un BOS M15 sur la dernière bougie entièrement clôturée.

    Long :
        close > ancien sommet + 0.5 * ATR

    Short :
        close < ancien creux - 0.5 * ATR

    Aucun tick live n'est utilisé.
    """

    if df is None or len(df) < 100:

        return None

    work = df.copy()

    work["atr"] = calculate_atr(
        work,
        ATR_PERIOD,
    )

    # Dernière bougie complètement clôturée.
    candle = work.iloc[-2]

    atr = candle["atr"]

    if pd.isna(atr) or atr <= 0:

        return None

    highs, lows = get_major_structures(
        work.iloc[:-1]
    )

    if highs.empty and lows.empty:

        return None

    # ----------------------------------------------------------------------
    # Ancien sommet le plus récent AVANT la bougie de breakout
    # ----------------------------------------------------------------------

    candidate_highs = highs[
        highs.index < candle.name
    ]

    candidate_lows = lows[
        lows.index < candle.name
    ]

    # ----------------------------------------------------------------------
    # BOS haussier
    # ----------------------------------------------------------------------

    if not candidate_highs.empty:

        previous_high = float(
            candidate_highs["high"].iloc[-1]
        )

        breakout_distance = (
            float(candle["close"])
            - previous_high
        )

        if breakout_distance >= (
            BOS_ATR_MULTIPLIER * float(atr)
        ):

            return {
                "direction": "BUY",
                "bos_time": str(
                    candle.name
                ),
                "trigger_price": previous_high,
                "macro_target": float(
                    candle["high"]
                ),
                "breakout_close": float(
                    candle["close"]
                ),
                "atr": float(atr),
                "structure_type": (
                    "RESISTANCE_TO_SUPPORT"
                ),
            }

    # ----------------------------------------------------------------------
    # BOS baissier
    # ----------------------------------------------------------------------

    if not candidate_lows.empty:

        previous_low = float(
            candidate_lows["low"].iloc[-1]
        )

        breakout_distance = (
            previous_low
            - float(candle["close"])
        )

        if breakout_distance >= (
            BOS_ATR_MULTIPLIER * float(atr)
        ):

            return {
                "direction": "SELL",
                "bos_time": str(
                    candle.name
                ),
                "trigger_price": previous_low,
                "macro_target": float(
                    candle["low"]
                ),
                "breakout_close": float(
                    candle["close"]
                ),
                "atr": float(atr),
                "structure_type": (
                    "SUPPORT_TO_RESISTANCE"
                ),
            }

    return None


# ============================================================================
# POLARITY M5
# ============================================================================


def detect_m5_liquidity_retest(
    df: pd.DataFrame,
    opportunity: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """
    Valide le retest M5.

    BUY :
        low < niveau
        close > niveau

    SELL :
        high > niveau
        close < niveau

    La dernière bougie complètement clôturée est utilisée.
    """

    if df is None or len(df) < 50:

        return None

    candle = df.iloc[-2]

    level = float(
        opportunity["trigger_price"]
    )

    direction = opportunity[
        "direction"
    ]

    if direction == "BUY":

        liquidity_taken = (
            float(candle["low"])
            < level
        )

        reentry = (
            float(candle["close"])
            > level
        )

        if liquidity_taken and reentry:

            return {
                "status": "WAITING_M1_CHOCH",
                "direction": "BUY",
                "retest_time": str(
                    candle.name
                ),
                "manipulation_low": float(
                    candle["low"]
                ),
                "manipulation_high": float(
                    candle["high"]
                ),
                "trigger_price": level,
            }

    elif direction == "SELL":

        liquidity_taken = (
            float(candle["high"])
            > level
        )

        reentry = (
            float(candle["close"])
            < level
        )

        if liquidity_taken and reentry:

            return {
                "status": "WAITING_M1_CHOCH",
                "direction": "SELL",
                "retest_time": str(
                    candle.name
                ),
                "manipulation_low": float(
                    candle["low"]
                ),
                "manipulation_high": float(
                    candle["high"]
                ),
                "trigger_price": level,
            }

    return None


# ============================================================================
# CHoCH M1
# ============================================================================


def get_recent_micro_structure(
    df: pd.DataFrame,
    window: int = 3,
) -> Tuple[
    Optional[float],
    Optional[float],
]:
    """Structure micro M1."""

    if len(df) < window * 2 + 10:

        return None, None

    work = df.iloc[:-2].copy()

    pivot_high = detect_pivot_highs(
        work,
        window,
    )

    pivot_low = detect_pivot_lows(
        work,
        window,
    )

    highs = work.loc[
        pivot_high,
        "high",
    ]

    lows = work.loc[
        pivot_low,
        "low",
    ]

    latest_high = (
        float(highs.iloc[-1])
        if not highs.empty
        else None
    )

    latest_low = (
        float(lows.iloc[-1])
        if not lows.empty
        else None
    )

    return latest_high, latest_low


def detect_m1_choch(
    df: pd.DataFrame,
    direction: str,
) -> Optional[Dict[str, Any]]:
    """
    CHoCH M1 strict.

    Utilise UNIQUEMENT la dernière bougie entièrement clôturée.

    Condition supplémentaire :
        body / range >= 0.60
    """

    if df is None or len(df) < 100:

        return None

    work = add_body_ratio(
        df
    )

    candle = work.iloc[-2]

    body_ratio = float(
        candle["body_ratio"]
    )

    if body_ratio < CHOCH_BODY_RATIO_MIN:

        return None

    previous = work.iloc[:-2]

    micro_high, micro_low = (
        get_recent_micro_structure(
            previous,
            window=3,
        )
    )

    if direction == "BUY":

        if micro_high is None:
            return None

        close = float(
            candle["close"]
        )

        open_price = float(
            candle["open"]
        )

        bullish_body = (
            close > open_price
        )

        if (
            close > micro_high
            and bullish_body
        ):

            return {
                "type": "CHOCH",
                "direction": "BUY",
                "time": str(
                    candle.name
                ),
                "level": micro_high,
                "close": close,
                "body_ratio": body_ratio,
            }

    elif direction == "SELL":

        if micro_low is None:
            return None

        close = float(
            candle["close"]
        )

        open_price = float(
            candle["open"]
        )

        bearish_body = (
            close < open_price
        )

        if (
            close < micro_low
            and bearish_body
        ):

            return {
                "type": "CHOCH",
                "direction": "SELL",
                "time": str(
                    candle.name
                ),
                "level": micro_low,
                "close": close,
                "body_ratio": body_ratio,
            }

    return None


# ============================================================================
# BOS M1
# ============================================================================


def detect_m1_bos(
    df: pd.DataFrame,
    direction: str,
    choch: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """
    Confirmation BOS M1 après CHoCH.

    Toujours basé sur une bougie clôturée.
    """

    if df is None or len(df) < 20:

        return None

    work = add_body_ratio(
        df
    )

    candle = work.iloc[-2]

    body_ratio = float(
        candle["body_ratio"]
    )

    if body_ratio < CHOCH_BODY_RATIO_MIN:

        return None

    choch_time = pd.to_datetime(
        choch["time"],
        utc=True,
    )

    after_choch = work[
        work.index > choch_time
    ].iloc[:-1]

    if after_choch.empty:

        return None

    if direction == "BUY":

        reference = float(
            after_choch["high"].max()
        )

        if float(candle["close"]) > reference:

            return {
                "type": "BOS",
                "direction": "BUY",
                "time": str(
                    candle.name
                ),
                "close": float(
                    candle["close"]
                ),
            }

    elif direction == "SELL":

        reference = float(
            after_choch["low"].min()
        )

        if float(candle["close"]) < reference:

            return {
                "type": "BOS",
                "direction": "SELL",
                "time": str(
                    candle.name
                ),
                "close": float(
                    candle["close"]
                ),
            }

    return None


# ============================================================================
# FVG M1
# ============================================================================


def detect_recent_fvg(
    df: pd.DataFrame,
    direction: str,
) -> Optional[Dict[str, Any]]:
    """
    FVG classique 3 bougies.

    Bullish :
        low[i] > high[i-2]

    Bearish :
        high[i] < low[i-2]
    """

    if df is None or len(df) < 10:

        return None

    work = df.iloc[:-1].copy()

    if len(work) < 3:

        return None

    high = work["high"]
    low = work["low"]

    if direction == "BUY":

        condition = (
            low
            > high.shift(2)
        )

        candidates = work[
            condition
        ]

        if not candidates.empty:

            idx = candidates.index[-1]

            position = work.index.get_loc(
                idx
            )

            if position >= 2:

                lower = float(
                    high.iloc[position - 2]
                )

                upper = float(
                    low.iloc[position]
                )

                midpoint = (
                    lower + upper
                ) / 2.0

                return {
                    "type": "FVG",
                    "direction": "BUY",
                    "lower": lower,
                    "upper": upper,
                    "entry": midpoint,
                    "time": str(idx),
                }

    elif direction == "SELL":

        condition = (
            high
            < low.shift(2)
        )

        candidates = work[
            condition
        ]

        if not candidates.empty:

            idx = candidates.index[-1]

            position = work.index.get_loc(
                idx
            )

            if position >= 2:

                upper = float(
                    low.iloc[position - 2]
                )

                lower = float(
                    high.iloc[position]
                )

                midpoint = (
                    lower + upper
                ) / 2.0

                return {
                    "type": "FVG",
                    "direction": "SELL",
                    "lower": lower,
                    "upper": upper,
                    "entry": midpoint,
                    "time": str(idx),
                }

    return None


# ============================================================================
# ORDER BLOCK M1
# ============================================================================


def detect_recent_order_block(
    df: pd.DataFrame,
    direction: str,
) -> Optional[Dict[str, Any]]:
    """
    Détection pragmatique du dernier OB M1.

    BUY :
        dernière bougie baissière avant impulsion haussière.

    SELL :
        dernière bougie haussière avant impulsion baissière.
    """

    if df is None or len(df) < 10:

        return None

    work = df.iloc[:-1].copy()

    # ----------------------------------------------------------------------
    # BUY
    # ----------------------------------------------------------------------

    if direction == "BUY":

        bearish = (
            work["close"]
            < work["open"]
        )

        candidates = work[
            bearish
        ]

        if candidates.empty:
            return None

        candle = candidates.iloc[-1]

        return {
            "type": "OB",
            "direction": "BUY",
            "entry": float(
                candle["open"]
            ),
            "low": float(
                candle["low"]
            ),
            "high": float(
                candle["high"]
            ),
            "time": str(
                candle.name
            ),
        }

    # ----------------------------------------------------------------------
    # SELL
    # ----------------------------------------------------------------------

    if direction == "SELL":

        bullish = (
            work["close"]
            > work["open"]
        )

        candidates = work[
            bullish
        ]

        if candidates.empty:
            return None

        candle = candidates.iloc[-1]

        return {
            "type": "OB",
            "direction": "SELL",
            "entry": float(
                candle["open"]
            ),
            "low": float(
                candle["low"]
            ),
            "high": float(
                candle["high"]
            ),
            "time": str(
                candle.name
            ),
        }

    return None


# ============================================================================
# ENTRY M1
# ============================================================================


def build_m1_entry(
    df: pd.DataFrame,
    direction: str,
) -> Optional[Dict[str, Any]]:
    """
    Priorité :

        FVG récent

    sinon :

        OB récent
    """

    fvg = detect_recent_fvg(
        df,
        direction,
    )

    if fvg:

        return {
            "entry_type": "FVG",
            "entry_price": float(
                fvg["entry"]
            ),
            "zone": fvg,
        }

    ob = detect_recent_order_block(
        df,
        direction,
    )

    if ob:

        return {
            "entry_type": "OB",
            "entry_price": float(
                ob["entry"]
            ),
            "zone": ob,
        }

    return None


# ============================================================================
# TP / SL
# ============================================================================


def calculate_trade_levels(
    direction: str,
    entry: float,
    manipulation_extreme: float,
    macro_target: float,
) -> Optional[Dict[str, Any]]:
    """
    Calcule :

        SL
        TP1  = RR 1:1
        TP3  = macro target +/- marge
        TP2  = milieu TP1 / TP3

    Le RR TP3 doit être >= 3.
    """

    direction = direction.upper()

    entry = float(entry)
    manipulation_extreme = float(
        manipulation_extreme
    )
    macro_target = float(
        macro_target
    )

    if entry <= 0:
        return None

    # ----------------------------------------------------------------------
    # BUY
    # ----------------------------------------------------------------------

    if direction == "BUY":

        sl = manipulation_extreme

        risk = entry - sl

        if risk <= 0:
            return None

        tp1 = entry + risk

        tp3 = (
            macro_target
            * (1.0 - TP3_SAFETY_MARGIN)
        )

        if tp3 <= entry:
            return None

        reward = tp3 - entry

    # ----------------------------------------------------------------------
    # SELL
    # ----------------------------------------------------------------------

    elif direction == "SELL":

        sl = manipulation_extreme

        risk = sl - entry

        if risk <= 0:
            return None

        tp1 = entry - risk

        tp3 = (
            macro_target
            * (1.0 + TP3_SAFETY_MARGIN)
        )

        if tp3 >= entry:
            return None

        reward = entry - tp3

    else:

        return None

    rr = reward / risk

    # ----------------------------------------------------------------------
    # Filtre RR strict
    # ----------------------------------------------------------------------

    if rr < MIN_GLOBAL_RR:

        logger.info(
            "[RR FILTER] Opportunity rejetée | "
            "%s | RR %.2f < %.2f",
            direction,
            rr,
            MIN_GLOBAL_RR,
        )

        return None

    tp2 = (
        tp1 + tp3
    ) / 2.0

    return {
        "entry": entry,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "risk": risk,
        "reward_tp3": reward,
        "rr_tp3": rr,
    }


# ============================================================================
# IDENTIFIANT OPPORTUNITÉ
# ============================================================================


def opportunity_id(
    symbol: str,
    direction: str,
    trigger_price: float,
    bos_time: str,
) -> str:

    return (
        f"{symbol}|"
        f"{direction}|"
        f"{trigger_price:.10f}|"
        f"{bos_time}"
    )


# ============================================================================
# OPPORTUNITÉS
# ============================================================================


def get_pending_opportunities() -> List[Dict[str, Any]]:
    data = load_json(
        PENDING_FILE,
        [],
    )

    return (
        data
        if isinstance(data, list)
        else []
    )


def save_pending_opportunities(
    opportunities: List[Dict[str, Any]],
) -> bool:

    return save_json(
        PENDING_FILE,
        opportunities,
    )


def upsert_pending_opportunity(
    opportunity: Dict[str, Any],
) -> bool:

    with JSON_LOCK:

        opportunities = (
            get_pending_opportunities()
        )

        existing_ids = {
            item.get("id")
            for item in opportunities
        }

        if opportunity["id"] in existing_ids:

            return False

        opportunities.append(
            opportunity
        )

        return save_pending_opportunities(
            opportunities
        )


def update_opportunity(
    opportunity_id_value: str,
    updates: Dict[str, Any],
) -> Optional[Dict[str, Any]]:

    with JSON_LOCK:

        opportunities = (
            get_pending_opportunities()
        )

        updated = None

        for opportunity in opportunities:

            if opportunity.get("id") == (
                opportunity_id_value
            ):

                opportunity.update(
                    updates
                )

                updated = opportunity

                break

        save_pending_opportunities(
            opportunities
        )

        return updated


def remove_opportunity(
    opportunity_id_value: str,
) -> bool:

    with JSON_LOCK:

        opportunities = (
            get_pending_opportunities()
        )

        new_items = [
            item
            for item in opportunities
            if item.get("id")
            != opportunity_id_value
        ]

        changed = (
            len(new_items)
            != len(opportunities)
        )

        if changed:

            save_pending_opportunities(
                new_items
            )

        return changed


# ============================================================================
# TELEGRAM
# ============================================================================


def telegram_send(
    message: str,
) -> bool:
    """Envoi Telegram officiel."""

    if not TELEGRAM_BOT_TOKEN:

        logger.warning(
            "[TELEGRAM] TELEGRAM_BOT_TOKEN absent"
        )

        return False

    if not TELEGRAM_CHAT_ID:

        logger.warning(
            "[TELEGRAM] TELEGRAM_CHAT_ID absent"
        )

        return False

    url = (
        "https://api.telegram.org/"
        f"bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    )

    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "disable_web_page_preview": True,
    }

    try:

        response = HTTP_SESSION.post(
            url,
            json=payload,
            timeout=10,
        )

        if response.ok:

            return True

        logger.warning(
            "[TELEGRAM ERROR] %s",
            response.text[:500],
        )

        return False

    except Exception as exc:

        logger.warning(
            "[TELEGRAM EXCEPTION] %s",
            exc,
        )

        return False


def telegram_signal_generated(
    opportunity: Dict[str, Any],
) -> bool:

    message = (
        "🚨 NOVA POLARITY SIGNAL\n\n"
        f"Asset : {opportunity['symbol']}\n"
        f"Direction : {opportunity['direction']}\n"
        f"Source : {opportunity.get('data_source', 'UNKNOWN')}\n"
        f"Entry : {opportunity['entry']:.8f}\n"
        f"SL : {opportunity['sl']:.8f}\n"
        f"TP1 : {opportunity['tp1']:.8f}\n"
        f"TP2 : {opportunity['tp2']:.8f}\n"
        f"TP3 : {opportunity['tp3']:.8f}\n"
        f"RR : 1:{opportunity['rr_tp3']:.2f}\n\n"
        "Statut : ORDRE LIMITE EN ATTENTE"
    )

    return telegram_send(
        message
    )


def telegram_tp1(
    trade: Dict[str, Any],
) -> bool:

    message = (
        "🟢 TP1 TOUCHÉ\n\n"
        f"{trade['symbol']} "
        f"{trade['direction']}\n"
        f"TP1 : {trade['tp1']:.8f}\n"
        f"SL déplacé au Break-Even\n"
        f"Entrée : {trade['entry']:.8f}\n"
        "Profit partiel sécurisé."
    )

    return telegram_send(
        message
    )


def telegram_tp2(
    trade: Dict[str, Any],
) -> bool:

    message = (
        "🟢 TP2 TOUCHÉ\n\n"
        f"{trade['symbol']} "
        f"{trade['direction']}\n"
        f"TP2 : {trade['tp2']:.8f}\n"
        "Position partielle restante."
    )

    return telegram_send(
        message
    )


def telegram_tp3(
    trade: Dict[str, Any],
) -> bool:

    message = (
        "🏆 TP3 TOUCHÉ\n\n"
        f"{trade['symbol']} "
        f"{trade['direction']}\n"
        f"TP3 : {trade['tp3']:.8f}\n"
        f"RR final : 1:{trade['rr_tp3']:.2f}\n"
        "Trade clôturé avec objectif macro atteint."
    )

    return telegram_send(
        message
    )


def telegram_sl(
    trade: Dict[str, Any],
) -> bool:

    message = (
        "🔴 STOP LOSS TOUCHÉ\n\n"
        f"{trade['symbol']} "
        f"{trade['direction']}\n"
        f"SL : {trade['sl']:.8f}\n"
        f"Entry : {trade['entry']:.8f}\n"
        f"Statut : {trade.get('status', 'CLOSED')}"
    )

    return telegram_send(
        message
    )


# ============================================================================
# TRADE PERSISTENCE
# ============================================================================


def get_active_trades() -> List[Dict[str, Any]]:
    data = load_json(
        ACTIVE_FILE,
        [],
    )

    return (
        data
        if isinstance(data, list)
        else []
    )


def save_active_trades(
    trades: List[Dict[str, Any]],
) -> bool:

    return save_json(
        ACTIVE_FILE,
        trades,
    )


def add_active_trade(
    trade: Dict[str, Any],
) -> bool:

    with TRADE_LOCK:

        trades = get_active_trades()

        existing = [
            item
            for item in trades
            if item.get("id") == trade.get("id")
        ]

        if existing:

            return False

        trades.append(
            trade
        )

        return save_active_trades(
            trades
        )


def get_trade_history() -> List[Dict[str, Any]]:
    data = load_json(
        HISTORY_FILE,
        [],
    )

    return (
        data
        if isinstance(data, list)
        else []
    )


def close_trade(
    trade: Dict[str, Any],
    reason: str,
    exit_price: float,
) -> None:

    with TRADE_LOCK:

        trade["status"] = "CLOSED"
        trade["exit_reason"] = reason
        trade["exit_price"] = float(
            exit_price
        )
        trade["closed_at"] = utc_iso()

        active = get_active_trades()

        active = [
            item
            for item in active
            if item.get("id") != trade.get("id")
        ]

        save_active_trades(
            active
        )

        history = get_trade_history()

        history.append(
            trade
        )

        save_json(
            HISTORY_FILE,
            history,
        )


# ============================================================================
# ORDRE LIMITE
# ============================================================================


def try_activate_pending_orders(
    symbol: str,
    current_price: float,
) -> None:
    """
    Transforme un ordre limite en trade actif lorsque le prix atteint
    réellement l'entrée.

    BUY :
        prix <= entry

    SELL :
        prix >= entry
    """

    with JSON_LOCK:

        opportunities = (
            get_pending_opportunities()
        )

        for opportunity in opportunities:

            if opportunity.get(
                "symbol"
            ) != symbol:

                continue

            if opportunity.get(
                "status"
            ) != "LIMIT_ORDER_PENDING":

                continue

            direction = opportunity[
                "direction"
            ]

            entry = float(
                opportunity["entry"]
            )

            activated = False

            if (
                direction == "BUY"
                and current_price <= entry
            ):

                activated = True

            elif (
                direction == "SELL"
                and current_price >= entry
            ):

                activated = True

            if not activated:
                continue

            trade = {
                **opportunity,
                "status": "ACTIVE",
                "activated_at": utc_iso(),
                "current_sl": float(
                    opportunity["sl"]
                ),
                "tp1_hit": False,
                "tp2_hit": False,
                "tp3_hit": False,
                "partial_tp1": 0.33,
                "partial_tp2": 0.33,
                "remaining": 1.0,
            }

            add_active_trade(
                trade
            )

            opportunity["status"] = (
                "ACTIVATED"
            )

            logger.info(
                "[ORDER ACTIVATED] %s %s @ %.8f",
                symbol,
                direction,
                entry,
            )

        save_pending_opportunities(
            opportunities
        )


# ============================================================================
# TRADE TRACKING
# ============================================================================


def trade_price_touched(
    trade: Dict[str, Any],
    price: float,
    level: float,
) -> bool:

    direction = trade[
        "direction"
    ]

    if direction == "BUY":

        return price >= level

    return price <= level


def stop_touched(
    trade: Dict[str, Any],
    price: float,
) -> bool:

    direction = trade[
        "direction"
    ]

    stop = float(
        trade["current_sl"]
    )

    if direction == "BUY":

        return price <= stop

    return price >= stop


def update_trade_tracking(
    symbol: str,
    price: float,
) -> None:
    """
    Gestion tick par tick logique.

    Priorité :

        SL
        TP1
        TP2
        TP3

    Après TP1 :
        SL -> Entry
    """

    with TRADE_LOCK:

        trades = get_active_trades()

        changed = False

        for trade in trades:

            if trade.get("symbol") != symbol:
                continue

            if trade.get("status") != "ACTIVE":
                continue

            # --------------------------------------------------------------
            # SL
            # --------------------------------------------------------------

            if stop_touched(
                trade,
                price,
            ):

                # Après TP1, toucher le BE est considéré comme BE.
                if trade.get(
                    "tp1_hit"
                ):

                    reason = "BREAK_EVEN"

                else:

                    reason = "STOP_LOSS"

                trade["status"] = "CLOSING"

                close_trade(
                    trade,
                    reason,
                    price,
                )

                if reason == "STOP_LOSS":

                    telegram_sl(
                        trade
                    )

                else:

                    telegram_send(
                        "⚪ BREAK-EVEN TOUCHÉ\n\n"
                        f"{trade['symbol']} "
                        f"{trade['direction']}\n"
                        f"Sortie : {price:.8f}"
                    )

                continue

            # --------------------------------------------------------------
            # TP1
            # --------------------------------------------------------------

            if (
                not trade.get("tp1_hit")
                and trade_price_touched(
                    trade,
                    price,
                    float(trade["tp1"]),
                )
            ):

                trade["tp1_hit"] = True

                trade["current_sl"] = float(
                    trade["entry"]
                )

                trade["remaining"] = (
                    max(
                        0.0,
                        float(
                            trade.get(
                                "remaining",
                                1.0,
                            )
                        )
                        - float(
                            trade.get(
                                "partial_tp1",
                                0.33,
                            )
                        ),
                    )
                )

                changed = True

                telegram_tp1(
                    trade
                )

            # --------------------------------------------------------------
            # TP2
            # --------------------------------------------------------------

            if (
                trade.get("tp1_hit")
                and not trade.get("tp2_hit")
                and trade_price_touched(
                    trade,
                    price,
                    float(trade["tp2"]),
                )
            ):

                trade["tp2_hit"] = True

                trade["remaining"] = (
                    max(
                        0.0,
                        float(
                            trade.get(
                                "remaining",
                                1.0,
                            )
                        )
                        - float(
                            trade.get(
                                "partial_tp2",
                                0.33,
                            )
                        ),
                    )
                )

                changed = True

                telegram_tp2(
                    trade
                )

            # --------------------------------------------------------------
            # TP3
            # --------------------------------------------------------------

            if (
                trade.get("tp2_hit")
                and not trade.get("tp3_hit")
                and trade_price_touched(
                    trade,
                    price,
                    float(trade["tp3"]),
                )
            ):

                trade["tp3_hit"] = True
                trade["remaining"] = 0.0
                trade["status"] = "CLOSING"

                close_trade(
                    trade,
                    "TP3",
                    price,
                )

                telegram_tp3(
                    trade
                )

        if changed:

            # Recharge car close_trade() peut déjà avoir modifié le fichier.
            remaining = [
                trade
                for trade in trades
                if trade.get(
                    "status"
                ) == "ACTIVE"
            ]

            save_active_trades(
                remaining
            )


# ============================================================================
# RÉCUPÉRATION DU PRIX
# ============================================================================


def get_latest_price(
    symbol: str,
) -> Tuple[
    Optional[float],
    Optional[str],
]:
    """
    Utilise M1 pour le prix courant exploitable par le moteur.
    """

    df = fetch_market_data_safe(
        symbol,
        "M1",
        limit=100,
    )

    if df.empty:

        return None, None

    return (
        float(df["close"].iloc[-1]),
        df.attrs.get(
            "source",
            "UNKNOWN",
        ),
    )


# ============================================================================
# PIPELINE PAR SYMBOLE
# ============================================================================


def create_m15_opportunity(
    symbol: str,
    m15: pd.DataFrame,
) -> Optional[Dict[str, Any]]:
    """Création d'une opportunité après BOS M15."""

    bos = detect_m15_bos(
        m15
    )

    if not bos:

        return None

    oid = opportunity_id(
        symbol,
        bos["direction"],
        bos["trigger_price"],
        bos["bos_time"],
    )

    opportunity = {
        "id": oid,
        "symbol": symbol,
        "direction": bos["direction"],
        "status": "WAITING_M5_RETEST",
        "trigger_price": bos[
            "trigger_price"
        ],
        "macro_target": bos[
            "macro_target"
        ],
        "bos_time": bos[
            "bos_time"
        ],
        "breakout_close": bos[
            "breakout_close"
        ],
        "atr": bos["atr"],
        "structure_type": bos[
            "structure_type"
        ],
        "created_at": utc_iso(),
        "data_source_m15": m15.attrs.get(
            "source",
            "UNKNOWN",
        ),
    }

    created = upsert_pending_opportunity(
        opportunity
    )

    if created:

        logger.info(
            "[M15 BOS] %s | %s | trigger %.8f | macro %.8f",
            symbol,
            bos["direction"],
            bos["trigger_price"],
            bos["macro_target"],
        )

    return opportunity


def process_pending_m5(
    symbol: str,
    m5: pd.DataFrame,
) -> None:
    """Progression des opportunités M5."""

    opportunities = (
        get_pending_opportunities()
    )

    for opportunity in opportunities:

        if opportunity.get(
            "symbol"
        ) != symbol:

            continue

        if opportunity.get(
            "status"
        ) != "WAITING_M5_RETEST":

            continue

        retest = detect_m5_liquidity_retest(
            m5,
            opportunity,
        )

        if not retest:
            continue

        updates = {
            **retest,
            "data_source_m5": m5.attrs.get(
                "source",
                "UNKNOWN",
            ),
            "updated_at": utc_iso(),
        }

        update_opportunity(
            opportunity["id"],
            updates,
        )

        logger.info(
            "[M5 RETEST] %s | %s | niveau %.8f",
            symbol,
            opportunity["direction"],
            opportunity["trigger_price"],
        )


def process_pending_m1(
    symbol: str,
    m1: pd.DataFrame,
) -> None:
    """Progression CHoCH -> BOS -> Entry."""

    opportunities = (
        get_pending_opportunities()
    )

    for opportunity in opportunities:

        if opportunity.get(
            "symbol"
        ) != symbol:

            continue

        status = opportunity.get(
            "status"
        )

        if status != "WAITING_M1_CHOCH":
            continue

        direction = opportunity[
            "direction"
        ]

        # ------------------------------------------------------------------
        # CHoCH
        # ------------------------------------------------------------------

        choch = detect_m1_choch(
            m1,
            direction,
        )

        if not choch:

            continue

        update_opportunity(
            opportunity["id"],
            {
                "status": "WAITING_M1_BOS",
                "choch": choch,
                "data_source_m1": m1.attrs.get(
                    "source",
                    "UNKNOWN",
                ),
                "updated_at": utc_iso(),
            },
        )

        logger.info(
            "[M1 CHOCH] %s | %s | level %.8f | body %.2f%%",
            symbol,
            direction,
            choch["level"],
            choch["body_ratio"] * 100,
        )

        continue


    # ----------------------------------------------------------------------
    # BOS M1
    # ----------------------------------------------------------------------

    opportunities = (
        get_pending_opportunities()
    )

    for opportunity in opportunities:

        if opportunity.get(
            "symbol"
        ) != symbol:

            continue

        if opportunity.get(
            "status"
        ) != "WAITING_M1_BOS":

            continue

        direction = opportunity[
            "direction"
        ]

        choch = opportunity.get(
            "choch"
        )

        if not choch:
            continue

        bos = detect_m1_bos(
            m1,
            direction,
            choch,
        )

        if not bos:
            continue

        # --------------------------------------------------------------
        # Entry FVG / OB
        # --------------------------------------------------------------

        entry_data = build_m1_entry(
            m1,
            direction,
        )

        if not entry_data:

            logger.info(
                "[ENTRY REJECTED] %s | Aucun FVG/OB exploitable",
                symbol,
            )

            update_opportunity(
                opportunity["id"],
                {
                    "status": "REJECTED",
                    "rejection_reason": (
                        "NO_FVG_OR_OB"
                    ),
                    "updated_at": utc_iso(),
                },
            )

            continue

        entry = float(
            entry_data["entry_price"]
        )

        # --------------------------------------------------------------
        # SL absolu sur l'extrême M5 de manipulation.
        #
        # Ce niveau est conservé comme référence absolue.
        # --------------------------------------------------------------

        if direction == "BUY":

            manipulation_extreme = float(
                opportunity[
                    "manipulation_low"
                ]
            )

        else:

            manipulation_extreme = float(
                opportunity[
                    "manipulation_high"
                )

        # --------------------------------------------------------------
        # TP
        # --------------------------------------------------------------

        levels = calculate_trade_levels(
            direction=direction,
            entry=entry,
            manipulation_extreme=(
                manipulation_extreme
            ),
            macro_target=float(
                opportunity[
                    "macro_target"
                ]
            ),
        )

        if levels is None:

            update_opportunity(
                opportunity["id"],
                {
                    "status": "REJECTED",
                    "rejection_reason": (
                        "RR_BELOW_1_3_OR_INVALID_LEVELS"
                    ),
                    "updated_at": utc_iso(),
                },
            )

            continue

        # --------------------------------------------------------------
        # Création ordre limite
        # --------------------------------------------------------------

        signal = {
            **opportunity,
            **levels,
            "entry_type": entry_data[
                "entry_type"
            ],
            "entry_zone": entry_data[
                "zone"
            ],
            "choch": choch,
            "m1_bos": bos,
            "status": "LIMIT_ORDER_PENDING",
            "generated_at": utc_iso(),
            "data_source": m1.attrs.get(
                "source",
                "UNKNOWN",
            ),
        }

        update_opportunity(
            opportunity["id"],
            signal,
        )

        logger.info(
            "[SIGNAL] %s | %s | Entry %.8f | SL %.8f | TP3 %.8f | RR %.2f",
            symbol,
            direction,
            levels["entry"],
            levels["sl"],
            levels["tp3"],
            levels["rr_tp3"],
        )

        telegram_signal_generated(
            signal
        )


# ============================================================================
# ANALYSE SYMBOL
# ============================================================================


def analyze_symbol(
    symbol: str,
) -> None:
    """
    Pipeline complet :

        M15
         ↓
        BOS + ATR
         ↓
        pending_opportunities.json
         ↓
        M5 retest + liquidity sweep
         ↓
        M1 CHoCH
         ↓
        M1 BOS
         ↓
        FVG / OB
         ↓
        ordre limite
    """

    lock = SYMBOL_LOCKS[
        symbol
    ]

    if not lock.acquire(
        blocking=False
    ):

        logger.debug(
            "[SKIP] %s déjà en analyse",
            symbol,
        )

        return

    try:

        # ------------------------------------------------------------------
        # M15
        # ------------------------------------------------------------------

        m15 = fetch_market_data_safe(
            symbol,
            "M15",
            MIN_CANDLES,
        )

        if len(m15) < 100:

            logger.warning(
                "[M15] données insuffisantes %s",
                symbol,
            )

            return

        create_m15_opportunity(
            symbol,
            m15,
        )

        # ------------------------------------------------------------------
        # M5
        # ------------------------------------------------------------------

        m5 = fetch_market_data_safe(
            symbol,
            "M5",
            MIN_CANDLES,
        )

        if len(m5) >= 100:

            process_pending_m5(
                symbol,
                m5,
            )

        # ------------------------------------------------------------------
        # M1
        # ------------------------------------------------------------------

        m1 = fetch_market_data_safe(
            symbol,
            "M1",
            MIN_CANDLES,
        )

        if len(m1) >= 100:

            process_pending_m1(
                symbol,
                m1,
            )

    except Exception as exc:

        logger.exception(
            "[ANALYSIS ERROR] %s | %s",
            symbol,
            exc,
        )

    finally:

        lock.release()


# ============================================================================
# THREAD ANALYSE
# ============================================================================


def symbol_analysis_loop(
    symbol: str,
) -> None:

    logger.info(
        "[THREAD START] Analyse %s",
        symbol,
    )

    while not shutdown_event.is_set():

        started = time.monotonic()

        try:

            analyze_symbol(
                symbol
            )

        except Exception as exc:

            logger.exception(
                "[LOOP ERROR] %s | %s",
                symbol,
                exc,
            )

        elapsed = (
            time.monotonic()
            - started
        )

        wait_time = max(
            1.0,
            ANALYSIS_INTERVAL - elapsed,
        )

        shutdown_event.wait(
            wait_time
        )

    logger.info(
        "[THREAD STOP] Analyse %s",
        symbol,
    )


# ============================================================================
# TRADE MONITOR
# ============================================================================


def trade_monitor_loop() -> None:
    """
    Thread permanent de suivi des trades.

    Prix M1 :
        - activation des ordres limites
        - TP
        - SL
        - Break-Even
    """

    logger.info(
        "[TRADE MONITOR] démarrage"
    )

    while not shutdown_event.is_set():

        try:

            for symbol in SYMBOLS:

                if shutdown_event.is_set():
                    break

                price, source = (
                    get_latest_price(
                        symbol
                    )
                )

                if price is None:
                    continue

                try:

                    try_activate_pending_orders(
                        symbol,
                        price,
                    )

                    update_trade_tracking(
                        symbol,
                        price,
                    )

                except Exception as exc:

                    logger.exception(
                        "[TRADE MONITOR ERROR] "
                        "%s | %s",
                        symbol,
                        exc,
                    )

                time.sleep(
                    0.2
                )

        except Exception as exc:

            logger.exception(
                "[MONITOR LOOP ERROR] %s",
                exc,
            )

        shutdown_event.wait(
            TRADE_MONITOR_INTERVAL
        )

    logger.info(
        "[TRADE MONITOR] arrêt"
    )


# ============================================================================
# MAINTENANCE
# ============================================================================


def cleanup_stale_opportunities() -> None:
    """
    Nettoyage des opportunités invalides.

    Une opportunité LIMIT_ORDER_PENDING n'est pas supprimée arbitrairement :
    elle reste disponible jusqu'à activation ou rejet.
    """

    with JSON_LOCK:

        opportunities = (
            get_pending_opportunities()
        )

        active_ids = {
            trade.get("id")
            for trade in get_active_trades()
        }

        cleaned = []

        for opportunity in opportunities:

            if opportunity.get(
                "status"
            ) == "ACTIVATED":

                continue

            if opportunity.get(
                "id"
            ) in active_ids:

                continue

            cleaned.append(
                opportunity
            )

        if len(cleaned) != len(
            opportunities
        ):

            save_pending_opportunities(
                cleaned
            )


# ============================================================================
# FLASK ROUTES
# ============================================================================


@app.route(
    "/",
    methods=["GET"],
)
def index():
    return jsonify(
        {
            "application": APP_NAME,
            "status": "online",
            "utc": utc_iso(),
            "symbols": SYMBOLS,
            "strategy": (
                "TREND_CONTINUATION_POLARITY"
            ),
        }
    )


@app.route(
    "/health",
    methods=["GET"],
)
def health():
    return jsonify(
        {
            "status": "healthy",
            "application": APP_NAME,
            "utc": utc_iso(),
        }
    )


@app.route(
    "/status",
    methods=["GET"],
)
def status():
    with TRADE_LOCK:

        active = get_active_trades()

    pending = (
        get_pending_opportunities()
    )

    return jsonify(
        {
            "application": APP_NAME,
            "status": "running",
            "utc": utc_iso(),
            "symbols": SYMBOLS,
            "active_trades": len(
                active
            ),
            "pending_opportunities": len(
                pending
            ),
            "active_trade_details": active,
        }
    )


@app.route(
    "/trades",
    methods=["GET"],
)
def trades():
    return jsonify(
        {
            "active": get_active_trades(),
            "history": get_trade_history(),
        }
    )


@app.route(
    "/opportunities",
    methods=["GET"],
)
def opportunities():
    return jsonify(
        {
            "pending": (
                get_pending_opportunities()
            )
        }
    )


# ============================================================================
# INITIALISATION
# ============================================================================


_started = False
_start_lock = threading.Lock()


def initialize_json_files() -> None:
    """Crée les fichiers de persistance s'ils n'existent pas."""

    files = [
        PENDING_FILE,
        ACTIVE_FILE,
        HISTORY_FILE,
        PROCESSED_FILE,
    ]

    for path in files:

        if not os.path.exists(path):

            save_json(
                path,
                [],
            )


def start_engine() -> None:
    """
    Démarrage idempotent du moteur.

    Le verrou évite un double lancement lorsque Flask est exécuté
    dans un contexte où le module pourrait être initialisé plusieurs fois.
    """

    global _started

    with _start_lock:

        if _started:

            return

        _started = True

        initialize_json_files()

        # --------------------------------------------------------------
        # Threads analyse
        # --------------------------------------------------------------

        for symbol in SYMBOLS:

            executor.submit(
                symbol_analysis_loop,
                symbol,
            )

        # --------------------------------------------------------------
        # Thread suivi trades
        # --------------------------------------------------------------

        executor.submit(
            trade_monitor_loop
        )

        logger.info(
            "=================================================="
        )

        logger.info(
            "%s démarré",
            APP_NAME,
        )

        logger.info(
            "Actifs : %s",
            ", ".join(SYMBOLS),
        )

        logger.info(
            "Stratégie : Polarité / Continuité de tendance"
        )

        logger.info(
            "Timeframes internes : M15 -> M5 -> M1"
        )

        logger.info(
            "Filtre BOS : %.2f ATR",
            BOS_ATR_MULTIPLIER,
        )

        logger.info(
            "Filtre CHoCH : %.0f%% corps",
            CHOCH_BODY_RATIO_MIN * 100,
        )

        logger.info(
            "RR minimum : 1:%.2f",
            MIN_GLOBAL_RR,
        )

        logger.info(
            "=================================================="
        )


# ============================================================================
# ARRÊT PROPRE
# ============================================================================


def stop_engine() -> None:

    shutdown_event.set()

    executor.shutdown(
        wait=False,
        cancel_futures=True,
    )

    logger.info(
        "[ENGINE] arrêt demandé"
    )


# ============================================================================
# MAIN
# ============================================================================


if __name__ == "__main__":

    start_engine()

    try:

        app.run(
            host="0.0.0.0",
            port=PORT,
            threaded=True,
            use_reloader=False,
        )

    except KeyboardInterrupt:

        logger.info(
            "[ENGINE] interruption clavier"
        )

    finally:

        stop_engine()