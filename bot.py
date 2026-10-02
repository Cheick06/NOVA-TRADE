import os
import time
import json
import logging
import threading
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests
import yfinance as yf


# ============================================================
# CONFIGURATION
# ============================================================

# Nettoyage automatique des espaces, retours ligne et espaces
# invisibles provenant des variables Railway.
TELEGRAM_BOT_TOKEN = os.getenv(
    "TELEGRAM_BOT_TOKEN", ""
).strip().replace("\r", "").replace("\n", "")

TELEGRAM_CHAT_ID = os.getenv(
    "TELEGRAM_CHAT_ID", ""
).strip().replace("\r", "").replace("\n", "")

TELEGRAM_ADMIN_ID = os.getenv(
    "TELEGRAM_ADMIN_ID", ""
).strip().replace("\r", "").replace("\n", "")

# BIQUOTE peut être configuré sans rendre cette variable obligatoire.
BIQUOTE_BASE_URL = os.getenv(
    "BIQUOTE_BASE_URL",
    "https://biquote.io/api"
).strip().rstrip("/")

# Clés facultatives pour les sources de secours.
ALPHA_VANTAGE_API_KEY = os.getenv(
    "ALPHA_VANTAGE_API_KEY",
    ""
).strip()

TWELVE_DATA_API_KEY = os.getenv(
    "TWELVE_DATA_API_KEY",
    ""
).strip()


# ============================================================
# PARAMÈTRES DU BOT
# ============================================================

SCAN_INTERVAL_SECONDS = 60

M15_CANDLES = 150
M5_CANDLES = 150
M1_CANDLES = 180

CHOC_LOOKBACK = 15
SMV_LOOKBACK = 20

SIGNAL_COOLDOWN_MINUTES = 60
PENDING_ORDER_TIMEOUT_MINUTES = 15

BE_TRIGGER_RR = 1.5

TP1_RR = 3.0
TP2_RR = 6.0


# ============================================================
# ACTIFS
# ============================================================

ASSETS = {
    "XAUUSD": {
        "yahoo": "GC=F",
        "decimals": 2,
        "buffer": 1.5,
        "pip": 0.10,
        "alpha_from": "XAU",
        "alpha_to": "USD",
        "twelve": "XAU/USD",
    },
    "BTCUSD": {
        "yahoo": "BTC-USD",
        "decimals": 2,
        "buffer": 50.0,
        "pip": 1.0,
        "alpha_from": "BTC",
        "alpha_to": "USD",
        "twelve": "BTC/USD",
    },
    "GBPUSD": {
        "yahoo": "GBPUSD=X",
        "decimals": 5,
        "buffer": 0.00030,
        "pip": 0.00001,
        "alpha_from": "GBP",
        "alpha_to": "USD",
        "twelve": "GBP/USD",
    },
    "EURUSD": {
        "yahoo": "EURUSD=X",
        "decimals": 5,
        "buffer": 0.00030,
        "pip": 0.00001,
        "alpha_from": "EUR",
        "alpha_to": "USD",
        "twelve": "EUR/USD",
    },
}

SYMBOLS = list(ASSETS.keys())


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger("NOVA-MTF-BOT")


# ============================================================
# HTTP SESSION
# ============================================================

session = requests.Session()

session.headers.update(
    {
        "User-Agent": "NOVA-MTF-TRADING-BOT/1.0",
        "Accept": "application/json",
    }
)


# ============================================================
# TELEGRAM
# ============================================================

TELEGRAM_API_URL = (
    f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
)


# ============================================================
# ÉTAT GLOBAL
# ============================================================

state_lock = threading.RLock()
scan_lock = threading.Lock()

last_prices: Dict[str, Dict[str, Any]] = {}

cooldown_tracker: Dict[str, datetime] = {}

pending_orders: Dict[str, Dict[str, Any]] = {}

closed_setups: Dict[str, Dict[str, Any]] = {}

last_scan_at: Optional[datetime] = None
last_scan_duration = 0.0

total_scans = 0
total_signals = 0

bot_started_at = datetime.now(timezone.utc)

telegram_offset = 0

telegram_running = True

scan_in_progress = False


# ============================================================
# FICHIER DE PERSISTANCE
# ============================================================

STATE_FILE = "trade_state.json"


# ============================================================
# OUTILS
# ============================================================

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None

        number = float(value)

        if not np.isfinite(number):
            return None

        return number

    except (TypeError, ValueError):
        return None


def format_price(
    symbol: str,
    value: Any,
) -> str:

    number = safe_float(value)

    if number is None:
        return "N/A"

    decimals = ASSETS[symbol]["decimals"]

    return f"{number:.{decimals}f}"


def format_volume(value: Any) -> str:

    number = safe_float(value)

    if number is None:
        return "N/A"

    if abs(number) >= 1_000_000:
        return f"{number:,.0f}"

    if abs(number) >= 1_000:
        return f"{number:,.2f}"

    return f"{number:.2f}"


def iso_datetime(value: Optional[datetime]) -> str:

    if value is None:
        return "N/A"

    return value.astimezone(timezone.utc).isoformat()


# ============================================================
# PERSISTANCE
# ============================================================

def serialize_state() -> Dict[str, Any]:

    with state_lock:

        cooldowns = {
            symbol: timestamp.isoformat()
            for symbol, timestamp in cooldown_tracker.items()
        }

        orders = {}

        for symbol, order in pending_orders.items():

            serialized = dict(order)

            for key in [
                "created_at",
                "last_update",
                "expires_at",
            ]:

                if isinstance(
                    serialized.get(key),
                    datetime,
                ):

                    serialized[key] = (
                        serialized[key].isoformat()
                    )

            orders[symbol] = serialized

        return {
            "cooldown_tracker": cooldowns,
            "pending_orders": orders,
            "closed_setups": closed_setups,
        }


def save_state() -> None:

    try:

        data = serialize_state()

        temporary_file = STATE_FILE + ".tmp"

        with open(
            temporary_file,
            "w",
            encoding="utf-8",
        ) as file:

            json.dump(
                data,
                file,
                ensure_ascii=False,
                indent=2,
            )

        os.replace(
            temporary_file,
            STATE_FILE,
        )

    except Exception as exc:

        logger.warning(
            "Impossible de sauvegarder l'état : %s",
            exc,
        )


def load_state() -> None:

    if not os.path.exists(STATE_FILE):
        return

    try:

        with open(
            STATE_FILE,
            "r",
            encoding="utf-8",
        ) as file:

            data = json.load(file)

        with state_lock:

            for symbol, value in data.get(
                "cooldown_tracker",
                {},
            ).items():

                try:

                    cooldown_tracker[symbol] = (
                        datetime.fromisoformat(value)
                    )

                except Exception:
                    pass

            for symbol, order in data.get(
                "pending_orders",
                {},
            ).items():

                restored = dict(order)

                for key in [
                    "created_at",
                    "last_update",
                    "expires_at",
                ]:

                    if restored.get(key):

                        try:

                            restored[key] = (
                                datetime.fromisoformat(
                                    restored[key]
                                )
                            )

                        except Exception:
                            pass

                pending_orders[symbol] = restored

            closed_setups.update(
                data.get(
                    "closed_setups",
                    {},
                )
            )

        logger.info(
            "État précédent restauré depuis %s.",
            STATE_FILE,
        )

    except Exception as exc:

        logger.warning(
            "Impossible de restaurer l'état : %s",
            exc,
        )


# ============================================================
# VALIDATION ENVIRONNEMENT
# ============================================================

def validate_environment() -> None:

    missing = []

    if not TELEGRAM_BOT_TOKEN:
        missing.append("TELEGRAM_BOT_TOKEN")

    if not TELEGRAM_CHAT_ID:
        missing.append("TELEGRAM_CHAT_ID")

    if not TELEGRAM_ADMIN_ID:
        missing.append("TELEGRAM_ADMIN_ID")

    if missing:

        raise RuntimeError(
            "Variables Railway manquantes : "
            + ", ".join(missing)
        )

    try:
        int(TELEGRAM_ADMIN_ID)

    except ValueError:

        raise RuntimeError(
            "TELEGRAM_ADMIN_ID doit être un identifiant numérique."
        )

    logger.info(
        "Variables Telegram validées."
    )


# ============================================================
# NORMALISATION DATAFRAME
# ============================================================

def normalize_dataframe(
    df: pd.DataFrame,
    limit: int,
) -> pd.DataFrame:

    if df is None or df.empty:
        raise ValueError(
            "DataFrame vide."
        )

    df = df.copy()

    # --------------------------------------------------------
    # MultiIndex Yahoo
    # --------------------------------------------------------

    if isinstance(
        df.columns,
        pd.MultiIndex,
    ):

        flattened = []

        for column in df.columns:

            parts = [
                str(part)
                for part in column
                if str(part).lower()
                not in (
                    "",
                    "nan",
                    "none",
                )
            ]

            flattened.append(
                parts[0]
                if parts
                else ""
            )

        df.columns = flattened

    # --------------------------------------------------------
    # Colonnes minuscules
    # --------------------------------------------------------

    df.columns = [
        str(column)
        .strip()
        .lower()
        .replace("_", " ")
        for column in df.columns
    ]

    rename_map = {
        "adj close": "close",
        "adjclose": "close",
        "open price": "open",
        "high price": "high",
        "low price": "low",
        "close price": "close",
        "tick volume": "tickvolume",
    }

    df.rename(
        columns=rename_map,
        inplace=True,
    )

    df = df.loc[
        :,
        ~df.columns.duplicated(),
    ]

    for column in [
        "open",
        "high",
        "low",
        "close",
    ]:

        if column not in df.columns:

            raise ValueError(
                f"Colonne absente : {column}"
            )

    if "volume" not in df.columns:
        df["volume"] = np.nan

    if "tickvolume" not in df.columns:
        df["tickvolume"] = np.nan

    for column in [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "tickvolume",
    ]:

        df[column] = pd.to_numeric(
            df[column],
            errors="coerce",
        )

    # Si le fournisseur donne uniquement le tick volume.
    if (
        df["volume"].isna().all()
        or df["volume"].fillna(0).sum() == 0
    ):

        if (
            not df["tickvolume"].isna().all()
            and df["tickvolume"].fillna(0).sum() > 0
        ):

            df["volume"] = df[
                "tickvolume"
            ]

    # --------------------------------------------------------
    # INDEX TEMPOREL
    # --------------------------------------------------------

    if not isinstance(
        df.index,
        pd.DatetimeIndex,
    ):

        df.index = pd.to_datetime(
            df.index,
            errors="coerce",
            utc=True,
        )

    else:

        try:

            if df.index.tz is None:

                df.index = df.index.tz_localize(
                    "UTC"
                )

            else:

                df.index = df.index.tz_convert(
                    "UTC"
                )

        except Exception:

            df.index = pd.to_datetime(
                df.index,
                errors="coerce",
                utc=True,
            )

    df = df[
        ~df.index.isna()
    ]

    df = df.sort_index()

    df = df.dropna(
        subset=[
            "open",
            "high",
            "low",
            "close",
        ]
    )

    return df.tail(limit)


# ============================================================
# BIQUOTE
# ============================================================

def _biquote_extract_bars(
    payload: Any,
) -> List[Dict[str, Any]]:

    if isinstance(
        payload,
        list,
    ):

        return payload

    if not isinstance(
        payload,
        dict,
    ):

        return []

    for key in [
        "bars",
        "data",
        "candles",
        "results",
        "items",
    ]:

        value = payload.get(key)

        if isinstance(
            value,
            list,
        ):

            return value

    return []


def _biquote_bar_to_row(
    bar: Dict[str, Any],
) -> Optional[Dict[str, Any]]:

    timestamp = (
        bar.get("openTime")
        or bar.get("open_time")
        or bar.get("timestamp")
        or bar.get("time")
        or bar.get("datetime")
        or bar.get("date")
    )

    open_price = (
        bar.get("open")
        or bar.get("o")
    )

    high = (
        bar.get("high")
        or bar.get("h")
    )

    low = (
        bar.get("low")
        or bar.get("l")
    )

    close = (
        bar.get("close")
        or bar.get("c")
    )

    volume = (
        bar.get("volume")
        or bar.get("v")
        or bar.get("tickVolume")
        or bar.get("tick_volume")
    )

    if (
        timestamp is None
        or open_price is None
        or high is None
        or low is None
        or close is None
    ):

        return None

    return {
        "timestamp": timestamp,
        "open": open_price,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
    }


def fetch_biquote_timeframe(
    symbol: str,
    timeframe: str,
    limit: int,
) -> pd.DataFrame:

    # Plusieurs chemins sont essayés afin de rester compatible
    # avec les variantes publiques d'API BIQUOTE.
    paths = [
        f"/{symbol}/ohlc",
        f"/ohlc/{symbol}",
        f"/market/{symbol}/ohlc",
        f"/markets/{symbol}/ohlc",
    ]

    last_error = None

    for path in paths:

        try:

            url = (
                BIQUOTE_BASE_URL
                + path
            )

            response = session.get(
                url,
                params={
                    "interval": timeframe,
                    "timeframe": timeframe,
                    "limit": limit,
                    "count": limit,
                },
                timeout=12,
            )

            if response.status_code != 200:

                last_error = (
                    f"HTTP {response.status_code}"
                )

                continue

            payload = response.json()

            bars = _biquote_extract_bars(
                payload
            )

            if not bars:

                last_error = (
                    "Aucune barre dans la réponse."
                )

                continue

            rows = []

            for bar in bars:

                if not isinstance(
                    bar,
                    dict,
                ):
                    continue

                row = _biquote_bar_to_row(
                    bar
                )

                if row:
                    rows.append(row)

            if not rows:

                last_error = (
                    "Barres BIQUOTE invalides."
                )

                continue

            df = pd.DataFrame(rows)

            df["timestamp"] = pd.to_datetime(
                df["timestamp"],
                errors="coerce",
                utc=True,
            )

            df = df.set_index(
                "timestamp"
            )

            df = normalize_dataframe(
                df,
                limit,
            )

            if len(df) >= 30:
                return df

            last_error = (
                "Pas assez de bougies."
            )

        except Exception as exc:

            last_error = str(exc)

    raise RuntimeError(
        f"BIQUOTE {symbol} {timeframe} : "
        f"{last_error}"
    )


def fetch_biquote_live_price(
    symbol: str,
) -> Optional[float]:

    paths = [
        f"/{symbol}",
        f"/quote/{symbol}",
        f"/market/{symbol}",
        f"/markets/{symbol}",
    ]

    for path in paths:

        try:

            response = session.get(
                BIQUOTE_BASE_URL + path,
                params={
                    "allowStale": "false",
                },
                timeout=8,
            )

            if response.status_code != 200:
                continue

            payload = response.json()

            if not isinstance(
                payload,
                dict,
            ):
                continue

            for key in [
                "mid",
                "last",
                "price",
                "close",
                "bid",
                "ask",
            ]:

                price = safe_float(
                    payload.get(key)
                )

                if price is not None:
                    return price

        except Exception:
            continue

    return None


def fetch_biquote_all_timeframes(
    symbol: str,
) -> Tuple[
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    Dict[str, Any],
]:

    m15 = fetch_biquote_timeframe(
        symbol,
        "15m",
        M15_CANDLES,
    )

    m5 = fetch_biquote_timeframe(
        symbol,
        "5m",
        M5_CANDLES,
    )

    m1 = fetch_biquote_timeframe(
        symbol,
        "1m",
        M1_CANDLES,
    )

    live_price = (
        fetch_biquote_live_price(symbol)
    )

    if live_price is None:

        live_price = safe_float(
            m1.iloc[-1]["close"]
        )

    return (
        m15,
        m5,
        m1,
        {
            "source": "BIQUOTE",
            "price": live_price,
            "m15_price": safe_float(
                m15.iloc[-1]["close"]
            ),
            "m5_price": safe_float(
                m5.iloc[-1]["close"]
            ),
            "m1_price": safe_float(
                m1.iloc[-1]["close"]
            ),
            "m1_volume": safe_float(
                m1.iloc[-1]["volume"]
            ),
        },
    )


# ============================================================
# YAHOO
# ============================================================

def fetch_yahoo_1m(
    symbol: str,
) -> pd.DataFrame:

    ticker = yf.Ticker(
        ASSETS[symbol]["yahoo"]
    )

    df = ticker.history(
        period="7d",
        interval="1m",
        auto_adjust=False,
        actions=False,
        prepost=False,
    )

    df = normalize_dataframe(
        df,
        M1_CANDLES,
    )

    if len(df) < 30:

        raise RuntimeError(
            f"Yahoo : données 1m insuffisantes pour {symbol}."
        )

    return df


def resample_ohlcv(
    df: pd.DataFrame,
    rule: str,
    limit: int,
) -> pd.DataFrame:

    if df.empty:
        raise ValueError(
            "DataFrame vide pour resampling."
        )

    aggregated = df.resample(
        rule,
        label="right",
        closed="right",
    ).agg(
        {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        }
    )

    aggregated = aggregated.dropna(
        subset=[
            "open",
            "high",
            "low",
            "close",
        ]
    )

    return normalize_dataframe(
        aggregated,
        limit,
    )


def fetch_yahoo_all_timeframes(
    symbol: str,
) -> Tuple[
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    Dict[str, Any],
]:

    base = fetch_yahoo_1m(
        symbol
    )

    m1 = base.tail(
        M1_CANDLES
    )

    m5 = resample_ohlcv(
        base,
        "5min",
        M5_CANDLES,
    )

    m15 = resample_ohlcv(
        base,
        "15min",
        M15_CANDLES,
    )

    price = safe_float(
        m1.iloc[-1]["close"]
    )

    return (
        m15,
        m5,
        m1,
        {
            "source": "Yahoo",
            "price": price,
            "m15_price": safe_float(
                m15.iloc[-1]["close"]
            ),
            "m5_price": safe_float(
                m5.iloc[-1]["close"]
            ),
            "m1_price": price,
            "m1_volume": safe_float(
                m1.iloc[-1]["volume"]
            ),
        },
    )


# ============================================================
# COINGECKO BTC - SECOURS SUPPLÉMENTAIRE
# ============================================================

def fetch_coingecko_btc() -> pd.DataFrame:

    url = (
        "https://api.coingecko.com/api/v3/"
        "coins/bitcoin/market_chart"
    )

    response = session.get(
        url,
        params={
            "vs_currency": "usd",
            "days": "1",
        },
        timeout=15,
    )

    response.raise_for_status()

    payload = response.json()

    prices = payload.get(
        "prices",
        [],
    )

    volumes = payload.get(
        "total_volumes",
        [],
    )

    if len(prices) < 30:

        raise RuntimeError(
            "CoinGecko BTC : données insuffisantes."
        )

    price_df = pd.DataFrame(
        prices,
        columns=[
            "timestamp",
            "price",
        ],
    )

    price_df["timestamp"] = pd.to_datetime(
        price_df["timestamp"],
        unit="ms",
        utc=True,
    )

    price_df = price_df.set_index(
        "timestamp"
    )

    price_df["open"] = price_df["price"]
    price_df["high"] = price_df["price"]
    price_df["low"] = price_df["price"]
    price_df["close"] = price_df["price"]

    if volumes:

        volume_df = pd.DataFrame(
            volumes,
            columns=[
                "timestamp",
                "volume",
            ],
        )

        volume_df["timestamp"] = pd.to_datetime(
            volume_df["timestamp"],
            unit="ms",
            utc=True,
        )

        volume_df = volume_df.set_index(
            "timestamp"
        )

        price_df["volume"] = (
            volume_df["volume"]
            .reindex(
                price_df.index,
                method="nearest",
            )
        )

    else:

        price_df["volume"] = np.nan

    base = price_df[
        [
            "open",
            "high",
            "low",
            "close",
            "volume",
        ]
    ]

    m1 = resample_ohlcv(
        base,
        "1min",
        M1_CANDLES,
    )

    m5 = resample_ohlcv(
        base,
        "5min",
        M5_CANDLES,
    )

    m15 = resample_ohlcv(
        base,
        "15min",
        M15_CANDLES,
    )

    return m15, m5, m1


# ============================================================
# CASCADE
# ============================================================

def get_market_data(
    symbol: str,
) -> Tuple[
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    Dict[str, Any],
]:

    # --------------------------------------------------------
    # SOURCE 1 : BIQUOTE
    # --------------------------------------------------------

    try:

        result = fetch_biquote_all_timeframes(
            symbol
        )

        metadata = result[3]

        logger.info(
            "| [BIQUOTE OK] %s | M15: %s | M5: %s | M1: %s",
            symbol,
            format_price(
                symbol,
                metadata["m15_price"],
            ),
            format_price(
                symbol,
                metadata["m5_price"],
            ),
            format_price(
                symbol,
                metadata["m1_price"],
            ),
        )

        return result

    except Exception as exc:

        logger.warning(
            "[BIQUOTE] %s indisponible : %s",
            symbol,
            exc,
        )

    # --------------------------------------------------------
    # SOURCE 2 : YAHOO
    # --------------------------------------------------------

    try:

        result = fetch_yahoo_all_timeframes(
            symbol
        )

        metadata = result[3]

        logger.info(
            "| [YAHOO OK] %s | M15: %s | M5: %s | M1: %s",
            symbol,
            format_price(
                symbol,
                metadata["m15_price"],
            ),
            format_price(
                symbol,
                metadata["m5_price"],
            ),
            format_price(
                symbol,
                metadata["m1_price"],
            ),
        )

        return result

    except Exception as exc:

        logger.warning(
            "[YAHOO] %s indisponible : %s",
            symbol,
            exc,
        )

    # --------------------------------------------------------
    # SOURCE 3 : COINGECKO BTC
    # --------------------------------------------------------

    if symbol == "BTCUSD":

        try:

            m15, m5, m1 = (
                fetch_coingecko_btc()
            )

            price = safe_float(
                m1.iloc[-1]["close"]
            )

            metadata = {
                "source": "CoinGecko",
                "price": price,
                "m15_price": safe_float(
                    m15.iloc[-1]["close"]
                ),
                "m5_price": safe_float(
                    m5.iloc[-1]["close"]
                ),
                "m1_price": price,
                "m1_volume": safe_float(
                    m1.iloc[-1]["volume"]
                ),
            }

            logger.info(
                "| [COINGECKO OK] %s | "
                "M15: %s | M5: %s | M1: %s",
                symbol,
                format_price(
                    symbol,
                    metadata["m15_price"],
                ),
                format_price(
                    symbol,
                    metadata["m5_price"],
                ),
                format_price(
                    symbol,
                    metadata["m1_price"],
                ),
            )

            return (
                m15,
                m5,
                m1,
                metadata,
            )

        except Exception as exc:

            logger.warning(
                "[COINGECKO] %s indisponible : %s",
                symbol,
                exc,
            )

    # --------------------------------------------------------
    # SOURCE 4 : ALPHA VANTAGE
    # --------------------------------------------------------

    if ALPHA_VANTAGE_API_KEY:

        try:

            # Alpha Vantage est utilisé ici uniquement
            # comme secours supplémentaire.
            result = fetch_alpha_multitimeframe(
                symbol
            )

            logger.info(
                "| [ALPHA OK] %s | M15: %s | "
                "M5: %s | M1: %s",
                symbol,
                format_price(
                    symbol,
                    result[3]["m15_price"],
                ),
                format_price(
                    symbol,
                    result[3]["m5_price"],
                ),
                format_price(
                    symbol,
                    result[3]["m1_price"],
                ),
            )

            return result

        except Exception as exc:

            logger.warning(
                "[ALPHA] %s indisponible : %s",
                symbol,
                exc,
            )

    # --------------------------------------------------------
    # SOURCE 5 : TWELVE DATA
    # --------------------------------------------------------

    if TWELVE_DATA_API_KEY:

        try:

            result = fetch_twelve_multitimeframe(
                symbol
            )

            logger.info(
                "| [TWELVE OK] %s | M15: %s | "
                "M5: %s | M1: %s",
                symbol,
                format_price(
                    symbol,
                    result[3]["m15_price"],
                ),
                format_price(
                    symbol,
                    result[3]["m5_price"],
                ),
                format_price(
                    symbol,
                    result[3]["m1_price"],
                ),
            )

            return result

        except Exception as exc:

            logger.warning(
                "[TWELVE] %s indisponible : %s",
                symbol,
                exc,
            )

    raise RuntimeError(
        f"Toutes les sources ont échoué pour {symbol}."
    )


# ============================================================
# ALPHA VANTAGE SECOURS
# ============================================================

def fetch_alpha_series(
    symbol: str,
) -> pd.DataFrame:

    url = (
        "https://www.alphavantage.co/query"
    )

    if symbol == "BTCUSD":

        params = {
            "function": "CRYPTO_INTRADAY",
            "symbol": "BTC",
            "market": "USD",
            "interval": "5min",
            "outputsize": "full",
            "apikey": ALPHA_VANTAGE_API_KEY,
        }

        series_key = (
            "Time Series Crypto (5min)"
        )

    else:

        params = {
            "function": "FX_INTRADAY",
            "from_symbol": ASSETS[symbol][
                "alpha_from"
            ],
            "to_symbol": ASSETS[symbol][
                "alpha_to"
            ],
            "interval": "5min",
            "outputsize": "full",
            "apikey": ALPHA_VANTAGE_API_KEY,
        }

        series_key = (
            "Time Series FX (5min)"
        )

    response = session.get(
        url,
        params=params,
        timeout=15,
    )

    response.raise_for_status()

    payload = response.json()

    series = payload.get(
        series_key,
        {},
    )

    if not series:
        raise RuntimeError(
            str(
                payload.get(
                    "Note",
                    payload.get(
                        "Information",
                        "Alpha Vantage sans données.",
                    ),
                )
            )
        )

    rows = []

    for timestamp, values in series.items():

        rows.append(
            {
                "timestamp": timestamp,
                "open": values.get("1. open"),
                "high": values.get("2. high"),
                "low": values.get("3. low"),
                "close": values.get("4. close"),
                "volume": values.get(
                    "5. volume"
                ),
            }
        )

    df = pd.DataFrame(rows)

    df["timestamp"] = pd.to_datetime(
        df["timestamp"],
        errors="coerce",
        utc=True,
    )

    df = df.set_index(
        "timestamp"
    )

    return normalize_dataframe(
        df,
        M5_CANDLES,
    )


def fetch_alpha_multitimeframe(
    symbol: str,
):

    m5 = fetch_alpha_series(
        symbol
    )

    m1 = m5.resample(
        "1min"
    ).ffill()

    m15 = resample_ohlcv(
        m5,
        "15min",
        M15_CANDLES,
    )

    m5 = normalize_dataframe(
        m5,
        M5_CANDLES,
    )

    m1 = normalize_dataframe(
        m1,
        M1_CANDLES,
    )

    metadata = {
        "source": "Alpha Vantage",
        "price": safe_float(
            m1.iloc[-1]["close"]
        ),
        "m15_price": safe_float(
            m15.iloc[-1]["close"]
        ),
        "m5_price": safe_float(
            m5.iloc[-1]["close"]
        ),
        "m1_price": safe_float(
            m1.iloc[-1]["close"]
        ),
        "m1_volume": safe_float(
            m1.iloc[-1]["volume"]
        ),
    }

    return (
        m15,
        m5,
        m1,
        metadata,
    )


# ============================================================
# TWELVE DATA SECOURS
# ============================================================

def fetch_twelve_multitimeframe(
    symbol: str,
):

    url = (
        "https://api.twelvedata.com/time_series"
    )

    response = session.get(
        url,
        params={
            "symbol": ASSETS[symbol]["twelve"],
            "interval": "1min",
            "outputsize": M1_CANDLES,
            "apikey": TWELVE_DATA_API_KEY,
            "format": "JSON",
        },
        timeout=15,
    )

    response.raise_for_status()

    payload = response.json()

    values = payload.get(
        "values"
    )

    if not values:

        raise RuntimeError(
            payload.get(
                "message",
                "Twelve Data sans données.",
            )
        )

    rows = []

    for item in values:

        rows.append(
            {
                "timestamp": item.get(
                    "datetime"
                ),
                "open": item.get(
                    "open"
                ),
                "high": item.get(
                    "high"
                ),
                "low": item.get(
                    "low"
                ),
                "close": item.get(
                    "close"
                ),
                "volume": item.get(
                    "volume"
                ),
            }
        )

    df = pd.DataFrame(rows)

    df["timestamp"] = pd.to_datetime(
        df["timestamp"],
        errors="coerce",
        utc=True,
    )

    df = df.set_index(
        "timestamp"
    )

    df = normalize_dataframe(
        df,
        M1_CANDLES,
    )

    m5 = resample_ohlcv(
        df,
        "5min",
        M5_CANDLES,
    )

    m15 = resample_ohlcv(
        df,
        "15min",
        M15_CANDLES,
    )

    metadata = {
        "source": "Twelve Data",
        "price": safe_float(
            df.iloc[-1]["close"]
        ),
        "m15_price": safe_float(
            m15.iloc[-1]["close"]
        ),
        "m5_price": safe_float(
            m5.iloc[-1]["close"]
        ),
        "m1_price": safe_float(
            df.iloc[-1]["close"]
        ),
        "m1_volume": safe_float(
            df.iloc[-1]["volume"]
        ),
    }

    return (
        m15,
        m5,
        df,
        metadata,
    )


# ============================================================
# PRICE ACTION
# ============================================================

def candle_body_ratio(
    candle: pd.Series,
) -> float:

    open_price = safe_float(
        candle["open"]
    )

    close = safe_float(
        candle["close"]
    )

    high = safe_float(
        candle["high"]
    )

    low = safe_float(
        candle["low"]
    )

    if None in (
        open_price,
        close,
        high,
        low,
    ):

        return 0.0

    total_range = high - low

    if total_range <= 0:
        return 0.0

    return abs(
        close - open_price
    ) / total_range


def bullish_engulfing(
    previous: pd.Series,
    current: pd.Series,
) -> bool:

    po = safe_float(previous["open"])
    pc = safe_float(previous["close"])

    co = safe_float(current["open"])
    cc = safe_float(current["close"])

    if None in (
        po,
        pc,
        co,
        cc,
    ):

        return False

    return (
        pc < po
        and cc > co
        and co <= pc
        and cc >= po
    )


def bearish_engulfing(
    previous: pd.Series,
    current: pd.Series,
) -> bool:

    po = safe_float(previous["open"])
    pc = safe_float(previous["close"])

    co = safe_float(current["open"])
    cc = safe_float(current["close"])

    if None in (
        po,
        pc,
        co,
        cc,
    ):

        return False

    return (
        pc > po
        and cc < co
        and co >= pc
        and cc <= po
    )


def bullish_pin_bar(
    candle: pd.Series,
) -> bool:

    o = safe_float(candle["open"])
    c = safe_float(candle["close"])
    h = safe_float(candle["high"])
    l = safe_float(candle["low"])

    if None in (
        o,
        c,
        h,
        l,
    ):

        return False

    body = abs(c - o)
    total = h - l

    if total <= 0:
        return False

    lower_wick = min(o, c) - l
    upper_wick = h - max(o, c)

    return (
        c > o
        and lower_wick >= body * 2
        and lower_wick > upper_wick
    )


def bearish_pin_bar(
    candle: pd.Series,
) -> bool:

    o = safe_float(candle["open"])
    c = safe_float(candle["close"])
    h = safe_float(candle["high"])
    l = safe_float(candle["low"])

    if None in (
        o,
        c,
        h,
        l,
    ):

        return False

    body = abs(c - o)
    total = h - l

    if total <= 0:
        return False

    lower_wick = min(o, c) - l
    upper_wick = h - max(o, c)

    return (
        c < o
        and upper_wick >= body * 2
        and upper_wick > lower_wick
    )


def detect_price_action(
    df: pd.DataFrame,
    direction: str,
) -> Optional[str]:

    if len(df) < 3:
        return None

    previous = df.iloc[-2]
    current = df.iloc[-1]

    ratio = candle_body_ratio(
        current
    )

    # Exigence stricte du cahier des charges.
    if ratio < 0.60:
        return None

    if direction == "BUY":

        if bullish_engulfing(
            previous,
            current,
        ):

            return "AVALement HAUSSIER"

        if bullish_pin_bar(
            current
        ):

            return "PIN BAR HAUSSIER"

    if direction == "SELL":

        if bearish_engulfing(
            previous,
            current,
        ):

            return "AVALement BAISSIER"

        if bearish_pin_bar(
            current
        ):

            return "PIN BAR BAISSIER"

    return None


# ============================================================
# SMC M15 : BOS + ORDER BLOCK
# ============================================================

def detect_swings(
    df: pd.DataFrame,
    left: int = 2,
    right: int = 2,
) -> Tuple[
    List[Tuple[pd.Timestamp, float]],
    List[Tuple[pd.Timestamp, float]],
]:

    highs = []
    lows = []

    if len(df) < left + right + 3:
        return highs, lows

    for i in range(
        left,
        len(df) - right,
    ):

        high = float(
            df.iloc[i]["high"]
        )

        low = float(
            df.iloc[i]["low"]
        )

        previous_highs = (
            df["high"]
            .iloc[
                i - left:i
            ]
            .astype(float)
        )

        next_highs = (
            df["high"]
            .iloc[
                i + 1:i + right + 1
            ]
            .astype(float)
        )

        previous_lows = (
            df["low"]
            .iloc[
                i - left:i
            ]
            .astype(float)
        )

        next_lows = (
            df["low"]
            .iloc[
                i + 1:i + right + 1
            ]
            .astype(float)
        )

        if (
            high > previous_highs.max()
            and high >= next_highs.max()
        ):

            highs.append(
                (
                    df.index[i],
                    high,
                )
            )

        if (
            low < previous_lows.min()
            and low <= next_lows.min()
        ):

            lows.append(
                (
                    df.index[i],
                    low,
                )
            )

    return highs, lows


def find_m15_bos_and_ob(
    df: pd.DataFrame,
) -> Optional[Dict[str, Any]]:

    if len(df) < 30:
        return None

    highs, lows = detect_swings(
        df.tail(80)
    )

    if not highs or not lows:
        return None

    last_close = safe_float(
        df.iloc[-1]["close"]
    )

    if last_close is None:
        return None

    latest_high_time, latest_high = highs[-1]
    latest_low_time, latest_low = lows[-1]

    previous_high = (
        highs[-2][1]
        if len(highs) >= 2
        else latest_high
    )

    previous_low = (
        lows[-2][1]
        if len(lows) >= 2
        else latest_low
    )

    direction = None
    bos_level = None
    bos_time = None

    # BOS haussier confirmé par clôture.
    if (
        latest_high > previous_high
        and last_close > latest_high
    ):

        direction = "BUY"
        bos_level = latest_high
        bos_time = latest_high_time

    # BOS baissier confirmé par clôture.
    elif (
        latest_low < previous_low
        and last_close < latest_low
    ):

        direction = "SELL"
        bos_level = latest_low
        bos_time = latest_low_time

    if direction is None:
        return None

    # --------------------------------------------------------
    # Order Block M15
    #
    # BUY :
    # dernière bougie baissière avant l'impulsion BOS.
    #
    # SELL :
    # dernière bougie haussière avant l'impulsion BOS.
    # --------------------------------------------------------

    bos_position = df.index.get_loc(
        bos_time
    )

    if isinstance(
        bos_position,
        slice,
    ):
        return None

    search_start = max(
        0,
        bos_position - 12,
    )

    candidates = df.iloc[
        search_start:bos_position + 1
    ]

    ob = None

    for i in range(
        len(candidates) - 1,
        -1,
        -1,
    ):

        candle = candidates.iloc[i]

        o = safe_float(
            candle["open"]
        )

        c = safe_float(
            candle["close"]
        )

        if o is None or c is None:
            continue

        if direction == "BUY" and c < o:

            ob = {
                "high": safe_float(
                    candle["high"]
                ),
                "low": safe_float(
                    candle["low"]
                ),
                "time": str(
                    candidates.index[i]
                ),
            }

            break

        if direction == "SELL" and c > o:

            ob = {
                "high": safe_float(
                    candle["high"]
                ),
                "low": safe_float(
                    candle["low"]
                ),
                "time": str(
                    candidates.index[i]
                ),
            }

            break

    if not ob:
        return None

    if (
        ob["high"] is None
        or ob["low"] is None
    ):
        return None

    return {
        "direction": direction,
        "bos_level": bos_level,
        "bos_time": str(bos_time),
        "ob_high": ob["high"],
        "ob_low": ob["low"],
        "ob_time": ob["time"],
    }


def price_in_order_block(
    price: float,
    ob: Dict[str, Any],
) -> bool:

    low = safe_float(
        ob.get("ob_low")
    )

    high = safe_float(
        ob.get("ob_high")
    )

    if low is None or high is None:
        return False

    return (
        low <= price <= high
    )


# ============================================================
# SMV M5
# ============================================================

def validate_smv(
    df: pd.DataFrame,
) -> Tuple[
    bool,
    str,
]:

    if len(df) < 22:
        return False, "Historique M5 insuffisant."

    volumes = pd.to_numeric(
        df["volume"],
        errors="coerce",
    )

    current_volume = safe_float(
        volumes.iloc[-1]
    )

    previous_20 = volumes.iloc[
        -21:-1
    ]

    if current_volume is None:
        return False, "Volume M5 indisponible."

    if previous_20.isna().any():
        return False, "Volumes M5 incomplets."

    average_20 = safe_float(
        previous_20.mean()
    )

    if (
        average_20 is None
        or average_20 <= 0
    ):

        return False, "Moyenne volume invalide."

    if current_volume <= (
        2.0 * average_20
    ):

        return (
            False,
            (
                f"Volume {current_volume:.2f} "
                f"<= 2x moyenne "
                f"{average_20:.2f}"
            ),
        )

    # Spike isolé : le volume actuel doit également
    # être supérieur aux 3 bougies précédentes.
    previous_3 = volumes.iloc[
        -4:-1
    ]

    if previous_3.isna().any():
        return False, "Volumes récents incomplets."

    if current_volume <= previous_3.max():

        return (
            False,
            "Spike M5 non isolé.",
        )

    return (
        True,
        (
            f"Volume {current_volume:.2f} > "
            f"2x moyenne20 {average_20:.2f}."
        ),
    )


# ============================================================
# CHoCH M5
# ============================================================

def detect_m5_choch(
    df: pd.DataFrame,
    expected_direction: str,
) -> Optional[Dict[str, Any]]:

    if len(df) < CHOC_LOOKBACK:
        return None

    window = df.tail(
        CHOC_LOOKBACK
    ).copy()

    highs, lows = detect_swings(
        window,
        left=2,
        right=2,
    )

    if not highs or not lows:
        return None

    latest_close = safe_float(
        window.iloc[-1]["close"]
    )

    if latest_close is None:
        return None

    if expected_direction == "BUY":

        if not highs:
            return None

        resistance = highs[-1][1]

        # Clôture au-dessus de la structure.
        if latest_close <= resistance:
            return None

        return {
            "direction": "BUY",
            "level": resistance,
            "candle_time": str(
                window.index[-1]
            ),
        }

    if expected_direction == "SELL":

        support = lows[-1][1]

        # Clôture sous la structure.
        if latest_close >= support:
            return None

        return {
            "direction": "SELL",
            "level": support,
            "candle_time": str(
                window.index[-1]
            ),
        }

    return None


# ============================================================
# MICRO ORDER BLOCK M1
# ============================================================

def find_micro_order_block(
    df: pd.DataFrame,
    direction: str,
) -> Optional[Dict[str, Any]]:

    if len(df) < 8:
        return None

    # On recherche l'impulsion récente.
    for i in range(
        len(df) - 2,
        max(
            1,
            len(df) - 10,
        ),
        -1,
    ):

        candle = df.iloc[i]
        next_candle = df.iloc[i + 1]

        o = safe_float(
            candle["open"]
        )

        c = safe_float(
            candle["close"]
        )

        h = safe_float(
            candle["high"]
        )

        l = safe_float(
            candle["low"]
        )

        next_o = safe_float(
            next_candle["open"]
        )

        next_c = safe_float(
            next_candle["close"]
        )

        if None in (
            o,
            c,
            h,
            l,
            next_o,
            next_c,
        ):
            continue

        # BUY :
        # dernière bougie baissière suivie d'une
        # impulsion haussière.
        if direction == "BUY":

            if (
                c < o
                and next_c > next_o
                and next_c > h
            ):

                return {
                    "direction": "BUY",
                    "high": h,
                    "low": l,
                    "time": str(
                        df.index[i]
                    ),
                    "impulse_time": str(
                        df.index[i + 1]
                    ),
                }

        # SELL :
        # dernière bougie haussière suivie d'une
        # impulsion baissière.
        if direction == "SELL":

            if (
                c > o
                and next_c < next_o
                and next_c < l
            ):

                return {
                    "direction": "SELL",
                    "high": h,
                    "low": l,
                    "time": str(
                        df.index[i]
                    ),
                    "impulse_time": str(
                        df.index[i + 1]
                    ),
                }

    return None


def micro_ob_entry(
    ob: Dict[str, Any],
) -> Optional[float]:

    high = safe_float(
        ob.get("high")
    )

    low = safe_float(
        ob.get("low")
    )

    if high is None or low is None:
        return None

    if high <= low:
        return None

    # Prix central du micro-OB.
    return (
        high + low
    ) / 2.0


# ============================================================
# SL / TP
# ============================================================

def calculate_trade_levels(
    symbol: str,
    direction: str,
    entry: float,
    m1_df: pd.DataFrame,
) -> Optional[Dict[str, float]]:

    if len(m1_df) < 6:
        return None

    recent = m1_df.tail(
        6
    ).copy()

    buffer = ASSETS[symbol][
        "buffer"
    ]

    if direction == "BUY":

        structure = safe_float(
            recent["low"].min()
        )

        if structure is None:
            return None

        sl = structure - buffer

        if sl >= entry:
            return None

        risk = entry - sl

        tp1 = (
            entry
            + risk * TP1_RR
        )

        tp2 = (
            entry
            + risk * TP2_RR
        )

    else:

        structure = safe_float(
            recent["high"].max()
        )

        if structure is None:
            return None

        sl = structure + buffer

        if sl <= entry:
            return None

        risk = sl - entry

        tp1 = (
            entry
            - risk * TP1_RR
        )

        tp2 = (
            entry
            - risk * TP2_RR
        )

    if risk <= 0:
        return None

    return {
        "entry": entry,
        "sl": sl,
        "risk": risk,
        "tp1": tp1,
        "tp2": tp2,
        "structure": structure,
    }


# ============================================================
# ENTONNOIR COMPLET SMC + PA + SMV
# ============================================================

def analyze_market(
    symbol: str,
    m15: pd.DataFrame,
    m5: pd.DataFrame,
    m1: pd.DataFrame,
    current_price: float,
) -> Optional[Dict[str, Any]]:

    # --------------------------------------------------------
    # ÉTAPE 1 : M15
    # BOS + OB
    # --------------------------------------------------------

    m15_setup = find_m15_bos_and_ob(
        m15
    )

    if not m15_setup:
        return None

    direction = m15_setup[
        "direction"
    ]

    # Le prix actuel doit être dans l'OB M15.
    if not price_in_order_block(
        current_price,
        m15_setup,
    ):

        return None

    # --------------------------------------------------------
    # ÉTAPE 2 : M5
    # CHoCH + SMV
    # --------------------------------------------------------

    smv_ok, smv_reason = (
        validate_smv(m5)
    )

    if not smv_ok:
        return None

    m5_choch = detect_m5_choch(
        m5,
        direction,
    )

    if not m5_choch:
        return None

    # --------------------------------------------------------
    # ÉTAPE 3 : M1
    # Micro OB + PA
    # --------------------------------------------------------

    micro_ob = find_micro_order_block(
        m1,
        direction,
    )

    if not micro_ob:
        return None

    pa = detect_price_action(
        m1,
        direction,
    )

    if not pa:
        return None

    entry = micro_ob_entry(
        micro_ob
    )

    if entry is None:
        return None

    levels = calculate_trade_levels(
        symbol,
        direction,
        entry,
        m1,
    )

    if not levels:
        return None

    # --------------------------------------------------------
    # VALIDATION LIMIT
    # --------------------------------------------------------

    # BUY LIMIT doit être sous le prix actuel.
    if direction == "BUY":

        if entry >= current_price:
            return None

    # SELL LIMIT doit être au-dessus du prix actuel.
    if direction == "SELL":

        if entry <= current_price:
            return None

    return {
        "symbol": symbol,
        "direction": direction,
        "order_type": (
            "BUY LIMIT"
            if direction == "BUY"
            else "SELL LIMIT"
        ),
        "entry": entry,
        "sl": levels["sl"],
        "risk": levels["risk"],
        "tp1": levels["tp1"],
        "tp2": levels["tp2"],
        "m15_bos": m15_setup,
        "m5_choch": m5_choch,
        "micro_ob": micro_ob,
        "price_action": pa,
        "smv_reason": smv_reason,
        "created_at": utc_now(),
    }


# ============================================================
# COOLDOWN
# ============================================================

def is_on_cooldown(
    symbol: str,
) -> bool:

    with state_lock:

        last_signal = cooldown_tracker.get(
            symbol
        )

    if not last_signal:
        return False

    elapsed = (
        utc_now()
        - last_signal
    )

    return (
        elapsed.total_seconds()
        < SIGNAL_COOLDOWN_MINUTES * 60
    )


def set_cooldown(
    symbol: str,
) -> None:

    with state_lock:

        cooldown_tracker[
            symbol
        ] = utc_now()

    save_state()


# ============================================================
# TELEGRAM REQUESTS
# ============================================================

def telegram_request(
    method: str,
    payload: Optional[
        Dict[str, Any]
    ] = None,
    timeout: int = 30,
) -> Optional[
    Dict[str, Any]
]:

    if not TELEGRAM_BOT_TOKEN:
        return None

    try:

        response = session.post(
            f"{TELEGRAM_API_URL}/{method}",
            json=payload or {},
            timeout=timeout,
        )

        if response.status_code != 200:

            logger.warning(
                "Telegram %s HTTP %s : %s",
                method,
                response.status_code,
                response.text[:500],
            )

            return None

        data = response.json()

        if not data.get("ok"):

            logger.warning(
                "Telegram %s erreur : %s",
                method,
                data,
            )

            return None

        return data

    except requests.RequestException as exc:

        logger.warning(
            "Telegram %s connexion : %s",
            method,
            exc,
        )

        return None


def telegram_send_message(
    chat_id: str,
    text: str,
    reply_markup: Optional[
        Dict[str, Any]
    ] = None,
) -> bool:

    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
    }

    if reply_markup:

        payload[
            "reply_markup"
        ] = reply_markup

    result = telegram_request(
        "sendMessage",
        payload,
        timeout=20,
    )

    return result is not None


def telegram_answer_callback(
    callback_id: str,
    text: str,
) -> None:

    telegram_request(
        "answerCallbackQuery",
        {
            "callback_query_id": callback_id,
            "text": text,
            "show_alert": False,
        },
        timeout=15,
    )


def telegram_delete_webhook() -> None:

    telegram_request(
        "deleteWebhook",
        {
            "drop_pending_updates": False,
        },
        timeout=20,
    )


# ============================================================
# MESSAGE SIGNAL
# ============================================================

def build_signal_message(
    signal: Dict[str, Any],
) -> str:

    symbol = signal["symbol"]

    direction = signal[
        "direction"
    ]

    emoji = (
        "🟢"
        if direction == "BUY"
        else "🔴"
    )

    return (
        "🚨 *SIGNAL LIMIT*\n\n"
        f"{emoji} *{signal['order_type']} {symbol}*\n\n"
        f"📍 Entrée : `{format_price(symbol, signal['entry'])}`\n"
        f"🛑 SL : `{format_price(symbol, signal['sl'])}`\n"
        f"🎯 TP1 : `{format_price(symbol, signal['tp1'])}` — RR `1:3`\n"
        f"🎯 TP2 : `{format_price(symbol, signal['tp2'])}` — RR `1:6`\n\n"
        "*Validation :*\n"
        f"• M15 : BOS + OB\n"
        f"• M5 : CHoCH + SMV > 2x moyenne20\n"
        f"• M1 : {signal['price_action']}\n"
        f"• Micro-OB M1 : `{format_price(symbol, signal['micro_ob']['low'])}` → "
        f"`{format_price(symbol, signal['micro_ob']['high'])}`\n\n"
        "⏳ Ordre valable maximum : `15 minutes`\n"
        "🛡️ Sécurisation BE : `RR 1:1.5`\n\n"
        "⚠️ Aucun ordre n'est exécuté automatiquement."
    )


# ============================================================
# ENREGISTREMENT ORDRE LIMIT
# ============================================================

def register_pending_order(
    signal: Dict[str, Any],
) -> bool:

    symbol = signal[
        "symbol"
    ]

    with state_lock:

        if symbol in pending_orders:
            return False

        now = utc_now()

        expires_at = (
            now
            + timedelta(
                minutes=PENDING_ORDER_TIMEOUT_MINUTES
            )
        )

        pending_orders[
            symbol
        ] = {
            **signal,
            "created_at": now,
            "last_update": now,
            "expires_at": expires_at,
            "triggered": False,
            "breakeven_sent": False,
            "cancelled": False,
        }

    save_state()

    return True


def send_new_signal(
    signal: Dict[str, Any],
) -> bool:

    global total_signals

    symbol = signal[
        "symbol"
    ]

    if is_on_cooldown(
        symbol
    ):

        logger.info(
            "%s ignoré : cooldown actif.",
            symbol,
        )

        return False

    if not register_pending_order(
        signal
    ):

        logger.info(
            "%s ignoré : ordre LIMIT déjà actif.",
            symbol,
        )

        return False

    message = build_signal_message(
        signal
    )

    sent = telegram_send_message(
        TELEGRAM_CHAT_ID,
        message,
    )

    if not sent:

        with state_lock:
            pending_orders.pop(
                symbol,
                None,
            )

        save_state()

        return False

    set_cooldown(
        symbol
    )

    with state_lock:
        total_signals += 1

    logger.info(
        "SIGNAL LIMIT envoyé : %s %s",
        symbol,
        signal["order_type"],
    )

    return True


# ============================================================
# SUIVI DES ORDRES LIMIT
# ============================================================

def check_pending_orders() -> None:

    now = utc_now()

    with state_lock:

        orders = {
            symbol: dict(order)
            for symbol, order
            in pending_orders.items()
        }

    for symbol, order in orders.items():

        try:

            if order.get(
                "triggered"
            ):

                continue

            expires_at = order.get(
                "expires_at"
            )

            if isinstance(
                expires_at,
                str,
            ):

                expires_at = datetime.fromisoformat(
                    expires_at
                )

            if (
                expires_at
                and now >= expires_at
            ):

                cancel_pending_order(
                    symbol
                )

                continue

            current = last_prices.get(
                symbol,
                {}
            )

            current_price = safe_float(
                current.get(
                    "price"
                )
            )

            if current_price is None:
                continue

            entry = safe_float(
                order.get(
                    "entry"
                )
            )

            sl = safe_float(
                order.get(
                    "sl"
                )
            )

            if (
                entry is None
                or sl is None
            ):
                continue

            # BUY LIMIT :
            # l'ordre est considéré déclenché lorsque
            # le prix atteint ou traverse l'entrée.
            if (
                order["direction"]
                == "BUY"
                and current_price <= entry
            ):

                mark_order_triggered(
                    symbol,
                    current_price,
                )

                continue

            # SELL LIMIT :
            if (
                order["direction"]
                == "SELL"
                and current_price >= entry
            ):

                mark_order_triggered(
                    symbol,
                    current_price,
                )

        except Exception as exc:

            logger.exception(
                "Erreur suivi ordre %s : %s",
                symbol,
                exc,
            )


def mark_order_triggered(
    symbol: str,
    current_price: float,
) -> None:

    with state_lock:

        order = pending_orders.get(
            symbol
        )

        if not order:
            return

        order["triggered"] = True
        order["triggered_at"] = utc_now()
        order["last_update"] = utc_now()

    save_state()

    logger.info(
        "%s : ordre LIMIT considéré déclenché à %s.",
        symbol,
        current_price,
    )


def cancel_pending_order(
    symbol: str,
) -> None:

    with state_lock:

        order = pending_orders.pop(
            symbol,
            None,
        )

    if not order:
        return

    message = (
        f"❌ *ANNULATION :* L'ordre LIMIT sur "
        f"*{symbol}* n'a pas été déclenché à temps.\n\n"
        "Annulez l'ordre."
    )

    telegram_send_message(
        TELEGRAM_CHAT_ID,
        message,
    )

    logger.info(
        "%s : ordre LIMIT annulé après 15 minutes.",
        symbol,
    )

    save_state()


# ============================================================
# BREAKEVEN
# ============================================================

def check_breakeven() -> None:

    with state_lock:

        orders = {
            symbol: dict(order)
            for symbol, order
            in pending_orders.items()
        }

    for symbol, order in orders.items():

        if not order.get(
            "triggered"
        ):

            continue

        if order.get(
            "breakeven_sent"
        ):

            continue

        current_price = safe_float(
            last_prices.get(
                symbol,
                {},
            ).get(
                "price"
            )
        )

        entry = safe_float(
            order.get(
                "entry"
            )
        )

        sl = safe_float(
            order.get(
                "sl"
            )
        )

        if None in (
            current_price,
            entry,
            sl,
        ):

            continue

        risk = abs(
            entry - sl
        )

        if risk <= 0:
            continue

        if order[
            "direction"
        ] == "BUY":

            trigger_price = (
                entry
                + risk * BE_TRIGGER_RR
            )

            reached = (
                current_price
                >= trigger_price
            )

        else:

            trigger_price = (
                entry
                - risk * BE_TRIGGER_RR
            )

            reached = (
                current_price
                <= trigger_price
            )

        if not reached:
            continue

        message = (
            f"🛡️ *SÉCURISATION :* Déplacez votre "
            f"Stop Loss au prix d'entrée "
            f"(Breakeven) sur *#{symbol}*"
        )

        sent = telegram_send_message(
            TELEGRAM_CHAT_ID,
            message,
        )

        if sent:

            with state_lock:

                if symbol in pending_orders:

                    pending_orders[
                        symbol
                    ][
                        "breakeven_sent"
                    ] = True

                    pending_orders[
                        symbol
                    ][
                        "last_update"
                    ] = utc_now()

            save_state()

            logger.info(
                "%s : alerte Breakeven envoyée.",
                symbol,
            )


# ============================================================
# TELEGRAM ADMIN
# ============================================================

def is_admin(
    user_id: Any,
) -> bool:

    return (
        str(user_id).strip()
        == TELEGRAM_ADMIN_ID
    )


def admin_keyboard() -> Dict[str, Any]:

    return {
        "inline_keyboard": [
            [
                {
                    "text": "📊 Statut Global",
                    "callback_data": "admin_status",
                }
            ],
            [
                {
                    "text": "📊 Voir Prix en Direct",
                    "callback_data": "admin_prices",
                }
            ],
            [
                {
                    "text": "🔄 Forcer un Scan",
                    "callback_data": "admin_scan",
                }
            ],
        ]
    }


def admin_status_text() -> str:

    with state_lock:

        scans = total_scans
        signals = total_signals
        active_orders = len(
            pending_orders
        )

        last_scan = last_scan_at
        duration = last_scan_duration

    uptime = (
        utc_now()
        - bot_started_at
    )

    return (
        "🟢 *STATUT GLOBAL*\n\n"
        "État : `OPÉRATIONNEL`\n"
        f"Uptime : `{str(uptime).split('.')[0]}`\n"
        f"Scans : `{scans}`\n"
        f"Signaux : `{signals}`\n"
        f"Ordres LIMIT actifs : `{active_orders}`\n"
        f"Dernier scan : `{iso_datetime(last_scan)}`\n"
        f"Durée dernier scan : `{duration:.2f}s`\n\n"
        "Timeframes : `M15 / M5 / M1`\n"
        "Cycle : `60 secondes`\n"
        "Source prioritaire : `BIQUOTE`"
    )


def admin_prices_text() -> str:

    with state_lock:

        snapshot = {
            symbol: dict(
                data
            )
            for symbol, data
            in last_prices.items()
        }

    lines = [
        "📊 *PRIX EN DIRECT*",
        "",
    ]

    for symbol in SYMBOLS:

        item = snapshot.get(
            symbol
        )

        if not item:

            lines.append(
                f"*{symbol}* : `N/A`"
            )

            continue

        lines.extend(
            [
                f"*{symbol}*",
                f"M15 : `{format_price(symbol, item.get('m15_price'))}`",
                f"M5 : `{format_price(symbol, item.get('m5_price'))}`",
                f"M1 : `{format_price(symbol, item.get('m1_price'))}`",
                f"Prix : `{format_price(symbol, item.get('price'))}`",
                f"Source : `{item.get('source', 'N/A')}`",
                "",
            ]
        )

    return "\n".join(lines)


def handle_admin_command(
    message: Dict[str, Any],
) -> None:

    user_id = (
        message.get(
            "from",
            {},
        ).get(
            "id"
        )
    )

    chat_id = (
        message.get(
            "chat",
            {},
        ).get(
            "id"
        )
    )

    if not is_admin(
        user_id
    ):

        if chat_id:

            telegram_send_message(
                str(chat_id),
                "⛔ *Accès refusé.*",
            )

        logger.warning(
            "Commande /admin refusée : %s",
            user_id,
        )

        return

    telegram_send_message(
        str(chat_id),
        (
            "🔐 *PANNEAU ADMIN*\n\n"
            "Sélectionnez une action :"
        ),
        admin_keyboard(),
    )


def handle_callback_query(
    callback: Dict[str, Any],
) -> None:

    callback_id = callback.get(
        "id"
    )

    user_id = (
        callback.get(
            "from",
            {},
        ).get(
            "id"
        )
    )

    data = callback.get(
        "data",
        "",
    )

    if not is_admin(
        user_id
    ):

        telegram_answer_callback(
            callback_id,
            "⛔ Accès refusé.",
        )

        logger.warning(
            "Clic admin refusé : %s",
            user_id,
        )

        return

    message_chat_id = (
        callback.get(
            "message",
            {},
        )
        .get(
            "chat",
            {},
        )
        .get(
            "id"
        )
    )

    if not message_chat_id:
        return

    if data == "admin_status":

        telegram_answer_callback(
            callback_id,
            "Statut actualisé.",
        )

        telegram_send_message(
            str(message_chat_id),
            admin_status_text(),
            admin_keyboard(),
        )

        return

    if data == "admin_prices":

        telegram_answer_callback(
            callback_id,
            "Prix actualisés.",
        )

        telegram_send_message(
            str(message_chat_id),
            admin_prices_text(),
            admin_keyboard(),
        )

        return

    if data == "admin_scan":

        started = start_immediate_scan()

        if started:

            telegram_answer_callback(
                callback_id,
                "Scan lancé.",
            )

            text = (
                "🔄 *SCAN FORCÉ LANCÉ*\n\n"
                "Les quatre actifs sont en cours d'analyse."
            )

        else:

            telegram_answer_callback(
                callback_id,
                "Scan déjà en cours.",
            )

            text = (
                "⚠️ *SCAN DÉJÀ EN COURS*\n\n"
                "Aucun scan parallèle n'a été lancé."
            )

        telegram_send_message(
            str(message_chat_id),
            text,
            admin_keyboard(),
        )


# ============================================================
# TELEGRAM POLLING
# ============================================================

def telegram_polling_loop() -> None:

    global telegram_offset

    telegram_delete_webhook()

    logger.info(
        "Thread Telegram démarré."
    )

    consecutive_errors = 0

    while telegram_running:

        try:

            result = telegram_request(
                "getUpdates",
                {
                    "offset": telegram_offset,
                    "timeout": 25,
                    "allowed_updates": [
                        "message",
                        "callback_query",
                    ],
                },
                timeout=35,
            )

            if result is None:

                consecutive_errors += 1

                time.sleep(
                    min(
                        30,
                        2 ** min(
                            consecutive_errors,
                            4,
                        ),
                    )
                )

                continue

            consecutive_errors = 0

            updates = result.get(
                "result",
                [],
            )

            for update in updates:

                telegram_offset = (
                    int(
                        update[
                            "update_id"
                        ]
                    )
                    + 1
                )

                try:

                    message = update.get(
                        "message"
                    )

                    if message:

                        text = str(
                            message.get(
                                "text",
                                "",
                            )
                        ).strip()

                        if text.startswith(
                            "/admin"
                        ):

                            handle_admin_command(
                                message
                            )

                    callback = update.get(
                        "callback_query"
                    )

                    if callback:

                        handle_callback_query(
                            callback
                        )

                except Exception as exc:

                    logger.exception(
                        "Erreur update Telegram : %s",
                        exc,
                    )

        except Exception as exc:

            logger.exception(
                "Erreur polling Telegram : %s",
                exc,
            )

            time.sleep(5)


# ============================================================
# SCAN COMPLET
# ============================================================

def scan_markets() -> None:

    global scan_in_progress
    global last_scan_at
    global last_scan_duration
    global total_scans

    if not scan_lock.acquire(
        blocking=False
    ):

        logger.warning(
            "Un scan est déjà en cours."
        )

        return

    scan_in_progress = True

    started = time.monotonic()

    try:

        logger.info(
            "===================================================="
        )

        logger.info(
            "SCAN MULTI-TIMEFRAME | M15 + M5 + M1"
        )

        logger.info(
            "===================================================="
        )

        # ----------------------------------------------------
        # Récupération et analyse des quatre actifs.
        # ----------------------------------------------------

        for symbol in SYMBOLS:

            try:

                (
                    m15,
                    m5,
                    m1,
                    metadata,
                ) = get_market_data(
                    symbol
                )

                current_price = safe_float(
                    metadata.get(
                        "price"
                    )
                )

                if current_price is None:
                    continue

                # État des prix pour l'admin
                with state_lock:

                    last_prices[
                        symbol
                    ] = {
                        "source": metadata.get(
                            "source"
                        ),
                        "price": current_price,
                        "m15_price": metadata.get(
                            "m15_price"
                        ),
                        "m5_price": metadata.get(
                            "m5_price"
                        ),
                        "m1_price": metadata.get(
                            "m1_price"
                        ),
                        "m1_volume": metadata.get(
                            "m1_volume"
                        ),
                        "updated_at": utc_now(),
                    }

                # ------------------------------------------------
                # Suivi des ordres existants.
                # ------------------------------------------------

                check_pending_orders()

                check_breakeven()

                # ------------------------------------------------
                # Un seul signal par paire toutes les 60 min.
                # ------------------------------------------------

                if is_on_cooldown(
                    symbol
                ):

                    logger.info(
                        "%s : cooldown actif.",
                        symbol,
                    )

                    continue

                # ------------------------------------------------
                # Pas de nouveau setup si une LIMIT existe.
                # ------------------------------------------------

                with state_lock:

                    has_pending = (
                        symbol
                        in pending_orders
                    )

                if has_pending:

                    logger.info(
                        "%s : ordre LIMIT déjà actif.",
                        symbol,
                    )

                    continue

                # ------------------------------------------------
                # ENTONNOIR SMC + PA + SMV.
                # ------------------------------------------------

                signal = analyze_market(
                    symbol,
                    m15,
                    m5,
                    m1,
                    current_price,
                )

                if not signal:

                    logger.info(
                        "%s : aucun setup complet.",
                        symbol,
                    )

                    continue

                logger.info(
                    (
                        "SETUP VALIDÉ | %s | %s | "
                        "Entry=%s | SL=%s | "
                        "TP1=%s | TP2=%s"
                    ),
                    symbol,
                    signal["order_type"],
                    format_price(
                        symbol,
                        signal["entry"],
                    ),
                    format_price(
                        symbol,
                        signal["sl"],
                    ),
                    format_price(
                        symbol,
                        signal["tp1"],
                    ),
                    format_price(
                        symbol,
                        signal["tp2"],
                    ),
                )

                send_new_signal(
                    signal
                )

            except Exception as exc:

                logger.exception(
                    "Erreur analyse %s : %s",
                    symbol,
                    exc,
                )

        # ----------------------------------------------------
        # Suivi global après récupération des quatre actifs.
        # ----------------------------------------------------

        check_pending_orders()

        check_breakeven()

        with state_lock:

            total_scans += 1
            last_scan_at = utc_now()
            last_scan_duration = (
                time.monotonic()
                - started
            )

        save_state()

        logger.info(
            "SCAN TERMINÉ | durée %.2fs",
            time.monotonic() - started,
        )

    finally:

        scan_in_progress = False

        scan_lock.release()


# ============================================================
# SCAN IMMÉDIAT
# ============================================================

def start_immediate_scan() -> bool:

    if scan_in_progress:
        return False

    thread = threading.Thread(
        target=scan_markets,
        name="ImmediateScan",
        daemon=True,
    )

    thread.start()

    return True


# ============================================================
# BOUCLE PRINCIPALE
# ============================================================

def trading_loop() -> None:

    logger.info(
        "Boucle trading démarrée : cycle 60 secondes."
    )

    # Premier scan immédiatement.
    scan_markets()

    while True:

        cycle_started = time.monotonic()

        try:

            # Vérification du cycle de vie des LIMIT
            # même si une nouvelle collecte rencontre
            # temporairement une erreur.
            check_pending_orders()
            check_breakeven()

            scan_markets()

        except Exception as exc:

            logger.exception(
                "Erreur boucle trading : %s",
                exc,
            )

        elapsed = (
            time.monotonic()
            - cycle_started
        )

        sleep_seconds = max(
            1,
            SCAN_INTERVAL_SECONDS
            - elapsed,
        )

        logger.info(
            "Prochain cycle dans %s secondes.",
            int(sleep_seconds),
        )

        time.sleep(
            sleep_seconds
        )


# ============================================================
# MAIN
# ============================================================

def main() -> None:

    validate_environment()

    load_state()

    logger.info(
        "===================================================="
    )

    logger.info(
        "NOVA MULTI-TIMEFRAME TRADING BOT"
    )

    logger.info(
        "Actifs : %s",
        ", ".join(SYMBOLS),
    )

    logger.info(
        "Timeframes : M15 / M5 / M1"
    )

    logger.info(
        "Cycle : 60 secondes"
    )

    logger.info(
        "Source principale : BIQUOTE"
    )

    logger.info(
        "Cooldown : 60 minutes par paire"
    )

    logger.info(
        "Expiration LIMIT : 15 minutes"
    )

    logger.info(
        "Breakeven : RR 1:1.5"
    )

    logger.info(
        "TP1 : RR 1:3"
    )

    logger.info(
        "TP2 : RR 1:6"
    )

    logger.info(
        "===================================================="
    )

    telegram_thread = threading.Thread(
        target=telegram_polling_loop,
        name="TelegramPolling",
        daemon=True,
    )

    telegram_thread.start()

    trading_thread = threading.Thread(
        target=trading_loop,
        name="TradingLoop",
        daemon=True,
    )

    trading_thread.start()

    # Maintient le processus Railway vivant.
    while True:

        time.sleep(60)


if __name__ == "__main__":
    main()