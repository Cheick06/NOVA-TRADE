import os
import time
import logging
import threading
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import numpy as np
import pandas as pd
import requests
import yfinance as yf


# ============================================================
# CONFIGURATION
# ============================================================

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TELEGRAM_ADMIN_ID = os.getenv("TELEGRAM_ADMIN_ID", "").strip()

ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY", "").strip()
TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY", "").strip()

BIQUOTE_BASE_URL = os.getenv(
    "BIQUOTE_BASE_URL",
    "https://biquote.io/api"
).rstrip("/")

SCAN_INTERVAL_SECONDS = 300
TIMEFRAME = "5m"
LOOKBACK_CANDLES = 150

SYMBOLS = [
    "XAUUSD",
    "BTCUSD",
    "GBPUSD",
    "EURUSD",
]

ASSETS = {
    "XAUUSD": {
        "yahoo": "GC=F",
        "alpha_from": "XAU",
        "alpha_to": "USD",
        "twelve": "XAU/USD",
        "decimals": 2,
    },
    "BTCUSD": {
        "yahoo": "BTC-USD",
        "coingecko": "bitcoin",
        "alpha_from": "BTC",
        "alpha_to": "USD",
        "twelve": "BTC/USD",
        "decimals": 2,
    },
    "GBPUSD": {
        "yahoo": "GBPUSD=X",
        "alpha_from": "GBP",
        "alpha_to": "USD",
        "twelve": "GBP/USD",
        "decimals": 5,
    },
    "EURUSD": {
        "yahoo": "EURUSD=X",
        "alpha_from": "EUR",
        "alpha_to": "USD",
        "twelve": "EUR/USD",
        "decimals": 5,
    },
}


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger("TRADING_BOT")


# ============================================================
# GLOBAL STATE
# ============================================================

session = requests.Session()
session.headers.update(
    {
        "User-Agent": "NOVA-TRADING-BOT/1.0",
        "Accept": "application/json",
    }
)

state_lock = threading.Lock()
scan_lock = threading.Lock()

last_prices: Dict[str, Dict[str, Any]] = {}
last_signals: Dict[str, Dict[str, Any]] = {}

bot_started_at = datetime.now(timezone.utc)
last_scan_at: Optional[datetime] = None
last_scan_duration = 0.0
total_scans = 0
total_signals = 0

telegram_offset = 0
telegram_running = True
scan_in_progress = False


# ============================================================
# VALIDATION ENVIRONMENT
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
            "Variables Railway manquantes : " + ", ".join(missing)
        )

    logger.info("Variables Telegram correctement configurées.")


# ============================================================
# UTILS
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


def format_price(symbol: str, price: Any) -> str:
    value = safe_float(price)

    if value is None:
        return "N/A"

    decimals = ASSETS[symbol]["decimals"]

    return f"{value:.{decimals}f}"


def format_volume(volume: Any) -> str:
    value = safe_float(volume)

    if value is None:
        return "N/A"

    if abs(value) >= 1_000_000:
        return f"{value:,.0f}"

    if abs(value) >= 1_000:
        return f"{value:,.2f}"

    return f"{value:.2f}"


# ============================================================
# DATAFRAME NORMALIZATION
# ============================================================

def normalize_dataframe(
    df: pd.DataFrame,
    limit: int = LOOKBACK_CANDLES,
) -> pd.DataFrame:

    if df is None or df.empty:
        raise ValueError("DataFrame vide.")

    df = df.copy()

    # Gestion Yahoo MultiIndex
    if isinstance(df.columns, pd.MultiIndex):
        flattened = []

        for column in df.columns:
            parts = [
                str(part)
                for part in column
                if str(part).lower() not in ("", "nan", "none")
            ]

            if parts:
                flattened.append(parts[0])
            else:
                flattened.append("")

        df.columns = flattened

    # Normalisation noms colonnes
    df.columns = [
        str(column).strip().lower().replace("_", " ")
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

    df = df.rename(columns=rename_map)

    # Suppression des colonnes dupliquées
    df = df.loc[:, ~df.columns.duplicated()]

    # Colonnes obligatoires
    for column in ["open", "high", "low", "close"]:
        if column not in df.columns:
            raise ValueError(
                f"Colonne obligatoire absente : {column}"
            )

    # Volume
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

    # Si volume réel indisponible, utiliser tick volume
    volume = df["volume"].copy()

    if volume.notna().sum() == 0 or volume.fillna(0).sum() == 0:
        volume = df["tickvolume"].copy()

    df["volume"] = volume

    # Index temporel
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(
            df.index,
            errors="coerce",
            utc=True,
        )

    else:
        try:
            if df.index.tz is None:
                df.index = df.index.tz_localize("UTC")
            else:
                df.index = df.index.tz_convert("UTC")
        except Exception:
            df.index = pd.to_datetime(
                df.index,
                errors="coerce",
                utc=True,
            )

    df = df[~df.index.isna()]
    df = df.sort_index()

    # Nettoyage OHLC
    df = df.dropna(
        subset=["open", "high", "low", "close"]
    )

    return df.tail(limit)


# ============================================================
# BIQUOTE - SOURCE PRINCIPALE
# ============================================================

def fetch_biquote_ohlc(
    symbol: str,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:

    url = f"{BIQUOTE_BASE_URL}/{symbol}/ohlc"

    response = session.get(
        url,
        params={
            "interval": TIMEFRAME,
            "limit": LOOKBACK_CANDLES,
        },
        timeout=12,
    )

    response.raise_for_status()

    payload = response.json()

    if not isinstance(payload, dict):
        raise ValueError("Réponse BIQUOTE invalide.")

    bars = payload.get("bars")

    if not isinstance(bars, list) or not bars:
        raise ValueError(
            f"Aucune bougie BIQUOTE pour {symbol}."
        )

    rows = []

    for bar in bars:
        if not isinstance(bar, dict):
            continue

        rows.append(
            {
                "openTime": bar.get("openTime"),
                "open": bar.get("open"),
                "high": bar.get("high"),
                "low": bar.get("low"),
                "close": bar.get("close"),
                "volume": bar.get("volume"),
                "tickVolume": bar.get("tickVolume"),
                "isOpen": bar.get("isOpen", False),
            }
        )

    if not rows:
        raise ValueError(
            f"Barres BIQUOTE invalides pour {symbol}."
        )

    df = pd.DataFrame(rows)

    df["openTime"] = pd.to_datetime(
        df["openTime"],
        errors="coerce",
        utc=True,
    )

    df = df.set_index("openTime")

    df = normalize_dataframe(
        df,
        limit=LOOKBACK_CANDLES,
    )

    # Le moteur de stratégie travaille uniquement
    # sur les bougies clôturées.
    if "isOpen" in df.columns:
        closed_mask = ~df["isOpen"].fillna(False).astype(bool)

        closed_df = df.loc[closed_mask].copy()

        if len(closed_df) >= 30:
            df = closed_df

    # Tick live pour le prix affiché
    tick_url = f"{BIQUOTE_BASE_URL}/{symbol}"

    tick_response = session.get(
        tick_url,
        params={"allowStale": "false"},
        timeout=8,
    )

    tick_response.raise_for_status()

    tick = tick_response.json()

    if not isinstance(tick, dict):
        raise ValueError(
            f"Tick BIQUOTE invalide pour {symbol}."
        )

    price = (
        safe_float(tick.get("mid"))
        or safe_float(tick.get("last"))
        or safe_float(tick.get("bid"))
        or safe_float(tick.get("ask"))
    )

    if price is None:
        raise ValueError(
            f"Prix BIQUOTE indisponible pour {symbol}."
        )

    # Volume de marché si disponible.
    # Pour FX/CFD BIQUOTE expose généralement 0 en volume
    # et fournit tickVolume sur les bougies.
    live_volume = safe_float(tick.get("volume"))

    if live_volume is None or live_volume == 0:
        if not df.empty:
            live_volume = safe_float(
                df.iloc[-1].get("volume")
            )

    metadata = {
        "source": "BIQUOTE",
        "price": price,
        "volume": live_volume,
        "timestamp": tick.get("timestamp"),
        "market_state": tick.get("marketState"),
        "stale": tick.get("stale"),
        "quote_age": tick.get("quoteAgeSeconds"),
    }

    return df, metadata


# ============================================================
# YAHOO FINANCE - SECOURS 1
# ============================================================

def fetch_yahoo(
    symbol: str,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:

    yahoo_symbol = ASSETS[symbol]["yahoo"]

    ticker = yf.Ticker(yahoo_symbol)

    df = ticker.history(
        period="5d",
        interval="5m",
        auto_adjust=False,
        actions=False,
        prepost=False,
    )

    df = normalize_dataframe(
        df,
        limit=LOOKBACK_CANDLES,
    )

    if len(df) < 30:
        raise ValueError(
            f"Pas assez de données Yahoo pour {symbol}."
        )

    latest = df.iloc[-1]

    price = safe_float(latest["close"])
    volume = safe_float(latest["volume"])

    if price is None:
        raise ValueError(
            f"Prix Yahoo indisponible pour {symbol}."
        )

    return df, {
        "source": "Yahoo",
        "price": price,
        "volume": volume,
        "timestamp": str(df.index[-1]),
    }


# ============================================================
# COINGECKO - SECOURS BTC
# ============================================================

def fetch_coingecko(
    symbol: str,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:

    if symbol != "BTCUSD":
        raise ValueError(
            "CoinGecko uniquement disponible pour BTCUSD."
        )

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
        timeout=12,
    )

    response.raise_for_status()

    payload = response.json()

    prices = payload.get("prices", [])
    volumes = payload.get("total_volumes", [])

    if len(prices) < 30:
        raise ValueError(
            "Pas assez de données CoinGecko."
        )

    price_df = pd.DataFrame(
        prices,
        columns=["timestamp", "price"],
    )

    price_df["timestamp"] = pd.to_datetime(
        price_df["timestamp"],
        unit="ms",
        utc=True,
    )

    price_df = price_df.set_index("timestamp")

    price_df["open"] = price_df["price"]
    price_df["high"] = price_df["price"]
    price_df["low"] = price_df["price"]
    price_df["close"] = price_df["price"]

    if volumes:
        volume_df = pd.DataFrame(
            volumes,
            columns=["timestamp", "volume"],
        )

        volume_df["timestamp"] = pd.to_datetime(
            volume_df["timestamp"],
            unit="ms",
            utc=True,
        )

        volume_df = volume_df.set_index("timestamp")

        price_df["volume"] = volume_df["volume"].reindex(
            price_df.index,
            method="nearest",
        )
    else:
        price_df["volume"] = np.nan

    price_df = price_df[
        [
            "open",
            "high",
            "low",
            "close",
            "volume",
        ]
    ]

    # Agrégation 5 minutes
    df = price_df.resample("5min").agg(
        {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        }
    ).dropna(subset=["open", "high", "low", "close"])

    df = normalize_dataframe(
        df,
        limit=LOOKBACK_CANDLES,
    )

    if len(df) < 30:
        raise ValueError(
            "Pas assez de bougies CoinGecko."
        )

    price = safe_float(df.iloc[-1]["close"])
    volume = safe_float(df.iloc[-1]["volume"])

    return df, {
        "source": "CoinGecko",
        "price": price,
        "volume": volume,
        "timestamp": str(df.index[-1]),
    }


# ============================================================
# ALPHA VANTAGE - SECOURS 3
# ============================================================

def fetch_alpha_vantage(
    symbol: str,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:

    if not ALPHA_VANTAGE_API_KEY:
        raise ValueError(
            "ALPHA_VANTAGE_API_KEY non configurée."
        )

    if symbol == "BTCUSD":

        url = "https://www.alphavantage.co/query"

        response = session.get(
            url,
            params={
                "function": "CRYPTO_INTRADAY",
                "symbol": "BTC",
                "market": "USD",
                "interval": "5min",
                "outputsize": "full",
                "apikey": ALPHA_VANTAGE_API_KEY,
            },
            timeout=15,
        )

        response.raise_for_status()

        payload = response.json()

        series = payload.get(
            "Time Series Crypto (5min)",
            {},
        )

        if not series:
            raise ValueError(
                "Alpha Vantage BTCUSD sans données."
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
                    "volume": values.get("5. volume"),
                }
            )

    else:

        url = "https://www.alphavantage.co/query"

        response = session.get(
            url,
            params={
                "function": "FX_INTRADAY",
                "from_symbol": ASSETS[symbol]["alpha_from"],
                "to_symbol": ASSETS[symbol]["alpha_to"],
                "interval": "5min",
                "outputsize": "full",
                "apikey": ALPHA_VANTAGE_API_KEY,
            },
            timeout=15,
        )

        response.raise_for_status()

        payload = response.json()

        series = payload.get(
            "Time Series FX (5min)",
            {},
        )

        if not series:
            raise ValueError(
                "Alpha Vantage FX sans données."
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
                    "volume": np.nan,
                }
            )

    df = pd.DataFrame(rows)

    df["timestamp"] = pd.to_datetime(
        df["timestamp"],
        errors="coerce",
        utc=True,
    )

    df = df.set_index("timestamp")

    df = normalize_dataframe(
        df,
        limit=LOOKBACK_CANDLES,
    )

    if len(df) < 30:
        raise ValueError(
            f"Pas assez de données Alpha Vantage pour {symbol}."
        )

    latest = df.iloc[-1]

    return df, {
        "source": "Alpha Vantage",
        "price": safe_float(latest["close"]),
        "volume": safe_float(latest["volume"]),
        "timestamp": str(df.index[-1]),
    }


# ============================================================
# TWELVE DATA - SECOURS 4
# ============================================================

def fetch_twelve_data(
    symbol: str,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:

    if not TWELVE_DATA_API_KEY:
        raise ValueError(
            "TWELVE_DATA_API_KEY non configurée."
        )

    url = "https://api.twelvedata.com/time_series"

    response = session.get(
        url,
        params={
            "symbol": ASSETS[symbol]["twelve"],
            "interval": "5min",
            "outputsize": LOOKBACK_CANDLES,
            "apikey": TWELVE_DATA_API_KEY,
            "format": "JSON",
        },
        timeout=15,
    )

    response.raise_for_status()

    payload = response.json()

    values = payload.get("values")

    if not isinstance(values, list) or not values:
        message = payload.get(
            "message",
            "Twelve Data sans données.",
        )
        raise ValueError(message)

    rows = []

    for item in values:
        rows.append(
            {
                "timestamp": item.get("datetime"),
                "open": item.get("open"),
                "high": item.get("high"),
                "low": item.get("low"),
                "close": item.get("close"),
                "volume": item.get("volume"),
            }
        )

    df = pd.DataFrame(rows)

    df["timestamp"] = pd.to_datetime(
        df["timestamp"],
        errors="coerce",
        utc=True,
    )

    df = df.set_index("timestamp")

    df = normalize_dataframe(
        df,
        limit=LOOKBACK_CANDLES,
    )

    if len(df) < 30:
        raise ValueError(
            f"Pas assez de données Twelve Data pour {symbol}."
        )

    latest = df.iloc[-1]

    return df, {
        "source": "Twelve Data",
        "price": safe_float(latest["close"]),
        "volume": safe_float(latest["volume"]),
        "timestamp": str(df.index[-1]),
    }


# ============================================================
# DATA PROVIDER - CASCADE
# ============================================================

def get_market_data(
    symbol: str,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:

    providers = [
        ("BIQUOTE", fetch_biquote_ohlc),
        ("Yahoo", fetch_yahoo),
    ]

    if symbol == "BTCUSD":
        providers.append(
            ("CoinGecko", fetch_coingecko)
        )

    providers.extend(
        [
            ("Alpha Vantage", fetch_alpha_vantage),
            ("Twelve Data", fetch_twelve_data),
        ]
    )

    errors = []

    for provider_name, provider in providers:

        try:
            df, metadata = provider(symbol)

            if df is None or df.empty:
                raise ValueError("Données vides.")

            price = safe_float(metadata.get("price"))

            if price is None:
                raise ValueError(
                    "Prix indisponible."
                )

            metadata["source"] = provider_name

            logger.info(
                "| [%s OK] %s | Prix: %s | Volume: %s",
                provider_name.upper(),
                symbol,
                format_price(symbol, price),
                format_volume(metadata.get("volume")),
            )

            return df, metadata

        except Exception as exc:

            error_text = str(exc)

            errors.append(
                f"{provider_name}: {error_text}"
            )

            logger.warning(
                "[%s] %s indisponible pour %s : %s",
                provider_name,
                symbol,
                symbol,
                error_text,
            )

    raise RuntimeError(
        f"Aucune source disponible pour {symbol}. "
        + " | ".join(errors)
    )


# ============================================================
# PRICE STATE
# ============================================================

def update_price_state(
    symbol: str,
    metadata: Dict[str, Any],
) -> None:

    with state_lock:

        last_prices[symbol] = {
            "symbol": symbol,
            "price": metadata.get("price"),
            "volume": metadata.get("volume"),
            "source": metadata.get("source"),
            "timestamp": metadata.get(
                "timestamp",
                utc_now().isoformat(),
            ),
            "updated_at": utc_now(),
        }


# ============================================================
# PRICE ACTION
# ============================================================

def candle_body_ratio(row: pd.Series) -> float:

    high = safe_float(row["high"])
    low = safe_float(row["low"])
    open_price = safe_float(row["open"])
    close = safe_float(row["close"])

    if None in (
        high,
        low,
        open_price,
        close,
    ):
        return 0.0

    total_range = high - low

    if total_range <= 0:
        return 0.0

    real_body = abs(close - open_price)

    return real_body / total_range


def is_bullish_engulfing(
    previous: pd.Series,
    current: pd.Series,
) -> bool:

    prev_open = safe_float(previous["open"])
    prev_close = safe_float(previous["close"])

    cur_open = safe_float(current["open"])
    cur_close = safe_float(current["close"])

    if None in (
        prev_open,
        prev_close,
        cur_open,
        cur_close,
    ):
        return False

    previous_bearish = prev_close < prev_open
    current_bullish = cur_close > cur_open

    body_engulfs = (
        cur_open <= prev_close
        and cur_close >= prev_open
    )

    return (
        previous_bearish
        and current_bullish
        and body_engulfs
    )


def is_bearish_engulfing(
    previous: pd.Series,
    current: pd.Series,
) -> bool:

    prev_open = safe_float(previous["open"])
    prev_close = safe_float(previous["close"])

    cur_open = safe_float(current["open"])
    cur_close = safe_float(current["close"])

    if None in (
        prev_open,
        prev_close,
        cur_open,
        cur_close,
    ):
        return False

    previous_bullish = prev_close > prev_open
    current_bearish = cur_close < cur_open

    body_engulfs = (
        cur_open >= prev_close
        and cur_close <= prev_open
    )

    return (
        previous_bullish
        and current_bearish
        and body_engulfs
    )


# ============================================================
# SMV
# ============================================================

def validate_smv(
    df: pd.DataFrame,
) -> Tuple[bool, str]:

    if len(df) < 22:
        return False, "Pas assez de bougies."

    volumes = pd.to_numeric(
        df["volume"],
        errors="coerce",
    )

    current_volume = safe_float(volumes.iloc[-1])

    previous_20 = volumes.iloc[-21:-1]

    previous_3 = volumes.iloc[-4:-1]

    if current_volume is None:
        return False, "Volume actuel indisponible."

    if previous_20.isna().any():
        return False, "Historique volume incomplet."

    if previous_3.isna().any():
        return False, "Historique volume récent incomplet."

    average_20 = safe_float(
        previous_20.mean()
    )

    if average_20 is None or average_20 <= 0:
        return False, "Moyenne volume invalide."

    condition_multiplier = (
        current_volume > 2.0 * average_20
    )

    condition_isolated_spike = (
        current_volume > previous_3.max()
    )

    if not condition_multiplier:
        return (
            False,
            "Volume inférieur ou égal à 2x moyenne 20."
        )

    if not condition_isolated_spike:
        return (
            False,
            "Volume non supérieur aux 3 bougies précédentes."
        )

    return True, (
        f"Volume {current_volume:.2f} > "
        f"2x moyenne20 {average_20:.2f} "
        f"et supérieur aux 3 précédents."
    )


# ============================================================
# STRUCTURE / CHoCH
# ============================================================

def find_recent_structure(
    df: pd.DataFrame,
    lookback: int = 15,
) -> Dict[str, float]:

    window = df.tail(lookback).copy()

    if len(window) < 7:
        return {}

    highs = window["high"].astype(float)
    lows = window["low"].astype(float)

    pivot_highs = []
    pivot_lows = []

    for i in range(2, len(window) - 2):

        high = highs.iloc[i]
        low = lows.iloc[i]

        previous_highs = highs.iloc[i - 2:i]
        next_highs = highs.iloc[i + 1:i + 3]

        previous_lows = lows.iloc[i - 2:i]
        next_lows = lows.iloc[i + 1:i + 3]

        if high > previous_highs.max() and high >= next_highs.max():
            pivot_highs.append(high)

        if low < previous_lows.min() and low <= next_lows.min():
            pivot_lows.append(low)

    latest_pivot_high = (
        pivot_highs[-1]
        if pivot_highs
        else float(highs.iloc[:-1].max())
    )

    latest_pivot_low = (
        pivot_lows[-1]
        if pivot_lows
        else float(lows.iloc[:-1].min())
    )

    previous_pivot_high = (
        pivot_highs[-2]
        if len(pivot_highs) >= 2
        else latest_pivot_high
    )

    previous_pivot_low = (
        pivot_lows[-2]
        if len(pivot_lows) >= 2
        else latest_pivot_low
    )

    return {
        "latest_high": float(latest_pivot_high),
        "latest_low": float(latest_pivot_low),
        "previous_high": float(previous_pivot_high),
        "previous_low": float(previous_pivot_low),
    }


def detect_choch(
    df: pd.DataFrame,
) -> Tuple[Optional[str], str, Dict[str, float]]:

    if len(df) < 15:
        return None, "Pas assez de bougies pour CHoCH.", {}

    window = df.tail(15).copy()

    structure = find_recent_structure(
        window,
        lookback=15,
    )

    if not structure:
        return None, "Structure indisponible.", {}

    latest_close = safe_float(
        window.iloc[-1]["close"]
    )

    if latest_close is None:
        return None, "Clôture indisponible.", structure

    latest_high = structure["latest_high"]
    latest_low = structure["latest_low"]

    previous_high = structure["previous_high"]
    previous_low = structure["previous_low"]

    bullish_structure = (
        latest_high > previous_high
        or latest_low > previous_low
    )

    bearish_structure = (
        latest_high < previous_high
        or latest_low < previous_low
    )

    # CHoCH haussier :
    # le corps clôture au-dessus de la dernière résistance.
    bullish_break = (
        latest_close > latest_high
    )

    # CHoCH baissier :
    # le corps clôture sous le dernier support.
    bearish_break = (
        latest_close < latest_low
    )

    if bearish_structure and bullish_break:
        return (
            "BUY",
            (
                "CHoCH haussier confirmé par clôture "
                "du corps au-dessus de la structure."
            ),
            structure,
        )

    if bullish_structure and bearish_break:
        return (
            "SELL",
            (
                "CHoCH baissier confirmé par clôture "
                "du corps sous la structure."
            ),
            structure,
        )

    return (
        None,
        "Aucun CHoCH confirmé par clôture.",
        structure,
    )


# ============================================================
# STRATÉGIE SMC + PA + SMV
# ============================================================

def analyze_smc_pa_smv(
    df: pd.DataFrame,
) -> Optional[Dict[str, Any]]:

    if df is None or len(df) < 30:
        return None

    df = df.copy()

    # Retirer la bougie en formation si elle existe.
    if "isOpen" in df.columns:
        closed_df = df[
            ~df["isOpen"].fillna(False).astype(bool)
        ].copy()

        if len(closed_df) >= 30:
            df = closed_df

    if len(df) < 30:
        return None

    # --------------------------------------------------------
    # 1. SMV
    # --------------------------------------------------------

    smv_ok, smv_reason = validate_smv(df)

    if not smv_ok:
        return None

    # --------------------------------------------------------
    # 2. CHoCH SMC
    # --------------------------------------------------------

    direction, choch_reason, structure = detect_choch(df)

    if direction not in ("BUY", "SELL"):
        return None

    # --------------------------------------------------------
    # 3. PRICE ACTION
    # --------------------------------------------------------

    previous = df.iloc[-2]
    current = df.iloc[-1]

    body_ratio = candle_body_ratio(current)

    if body_ratio < 0.60:
        return None

    bullish_engulfing = is_bullish_engulfing(
        previous,
        current,
    )

    bearish_engulfing = is_bearish_engulfing(
        previous,
        current,
    )

    if direction == "BUY" and not bullish_engulfing:
        return None

    if direction == "SELL" and not bearish_engulfing:
        return None

    # --------------------------------------------------------
    # 4. ENTRY
    # --------------------------------------------------------

    entry = safe_float(current["close"])

    if entry is None:
        return None

    # --------------------------------------------------------
    # 5. SL SERRÉ SUR STRUCTURE RÉCENTE
    # --------------------------------------------------------

    recent_structure = df.iloc[-6:-1]

    if recent_structure.empty:
        return None

    if direction == "BUY":

        structure_low = safe_float(
            recent_structure["low"].min()
        )

        if structure_low is None:
            return None

        sl = structure_low

        # SL doit réellement être sous l'entrée.
        if sl >= entry:
            return None

    else:

        structure_high = safe_float(
            recent_structure["high"].max()
        )

        if structure_high is None:
            return None

        sl = structure_high

        # SL doit réellement être au-dessus de l'entrée.
        if sl <= entry:
            return None

    risk = abs(entry - sl)

    if risk <= 0:
        return None

    # --------------------------------------------------------
    # 6. TP
    # --------------------------------------------------------

    if direction == "BUY":

        tp1 = entry + (risk * 2.5)
        tp2 = entry + (risk * 5.0)

    else:

        tp1 = entry - (risk * 2.5)
        tp2 = entry - (risk * 5.0)

    return {
        "direction": direction,
        "entry": entry,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "risk": risk,
        "rr_tp1": 2.5,
        "rr_tp2": 5.0,
        "smv": smv_reason,
        "smc": choch_reason,
        "pa": (
            f"Englobante {'haussière' if direction == 'BUY' else 'baissière'} "
            f"avec corps réel {body_ratio * 100:.1f}% de la range."
        ),
        "candle_time": str(df.index[-1]),
    }


# ============================================================
# TELEGRAM
# ============================================================

TELEGRAM_API_URL = (
    f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
)


def telegram_request(
    method: str,
    payload: Optional[Dict[str, Any]] = None,
    timeout: int = 30,
) -> Optional[Dict[str, Any]]:

    url = f"{TELEGRAM_API_URL}/{method}"

    try:

        response = session.post(
            url,
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
            "Telegram %s connexion impossible : %s",
            method,
            exc,
        )

        return None


def telegram_send_message(
    chat_id: str,
    text: str,
    reply_markup: Optional[Dict[str, Any]] = None,
) -> bool:

    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
    }

    if reply_markup:
        payload["reply_markup"] = reply_markup

    result = telegram_request(
        "sendMessage",
        payload,
        timeout=20,
    )

    return result is not None


def telegram_answer_callback(
    callback_query_id: str,
    text: str,
) -> None:

    telegram_request(
        "answerCallbackQuery",
        {
            "callback_query_id": callback_query_id,
            "text": text,
            "show_alert": False,
        },
        timeout=15,
    )


def telegram_delete_webhook() -> None:

    result = telegram_request(
        "deleteWebhook",
        {
            "drop_pending_updates": False,
        },
        timeout=20,
    )

    if result:
        logger.info(
            "Webhook Telegram supprimé : polling prêt."
        )


# ============================================================
# ADMIN SECURITY
# ============================================================

def is_admin(user_id: Any) -> bool:

    if not TELEGRAM_ADMIN_ID:
        return False

    return str(user_id).strip() == TELEGRAM_ADMIN_ID


def admin_keyboard() -> Dict[str, Any]:

    return {
        "inline_keyboard": [
            [
                {
                    "text": "📊 Statut Global du Bot",
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
                    "text": "🔄 Forcer un Scan Immédiat",
                    "callback_data": "admin_scan",
                }
            ],
        ]
    }


# ============================================================
# ADMIN STATUS
# ============================================================

def build_admin_status() -> str:

    with state_lock:
        scan_count = total_scans
        signal_count = total_signals
        scan_time = last_scan_at
        duration = last_scan_duration

    uptime = utc_now() - bot_started_at

    status = (
        "🟢 *STATUT GLOBAL DU BOT*\n\n"
        f"État : `OPÉRATIONNEL`\n"
        f"Uptime : `{str(uptime).split('.')[0]}`\n"
        f"Scans effectués : `{scan_count}`\n"
        f"Signaux générés : `{signal_count}`\n"
        f"Dernier scan : "
        f"`{scan_time.isoformat() if scan_time else 'Aucun'}`\n"
        f"Durée dernier scan : `{duration:.2f}s`\n\n"
        "*Marchés surveillés :*\n"
        "• XAUUSD\n"
        "• BTCUSD\n"
        "• GBPUSD\n"
        "• EURUSD\n\n"
        "Unité : `5 minutes`\n"
        "Source prioritaire : `BIQUOTE`"
    )

    return status


def build_admin_prices() -> str:

    with state_lock:
        snapshot = dict(last_prices)

    lines = [
        "📊 *PRIX EN DIRECT*",
        "",
    ]

    for symbol in SYMBOLS:

        item = snapshot.get(symbol)

        if not item:

            lines.append(
                f"*{symbol}* : `Aucune donnée`"
            )

            continue

        price = format_price(
            symbol,
            item.get("price"),
        )

        volume = format_volume(
            item.get("volume")
        )

        source = item.get(
            "source",
            "N/A",
        )

        lines.append(
            f"*{symbol}*\n"
            f"Prix : `{price}`\n"
            f"Volume : `{volume}`\n"
            f"Source : `{source}`"
        )

    return "\n\n".join(lines)


# ============================================================
# TELEGRAM SIGNAL
# ============================================================

def build_signal_message(
    symbol: str,
    signal: Dict[str, Any],
    source: str,
) -> str:

    direction = signal["direction"]

    if direction == "BUY":
        direction_text = "🟢 ACHAT"
    else:
        direction_text = "🔴 VENTE"

    decimals = ASSETS[symbol]["decimals"]

    entry = f"{signal['entry']:.{decimals}f}"
    sl = f"{signal['sl']:.{decimals}f}"
    tp1 = f"{signal['tp1']:.{decimals}f}"
    tp2 = f"{signal['tp2']:.{decimals}f}"

    message = (
        "🚨 *SIGNAL TRADING*\n\n"
        f"*{symbol}* — {direction_text}\n"
        f"⏱️ Unité : `5m`\n\n"
        f"*Entrée :* `{entry}`\n"
        f"*SL :* `{sl}`\n"
        f"*TP1 :* `{tp1}` — RR `1:2.5`\n"
        f"*TP2 :* `{tp2}` — RR `1:5.0`\n\n"
        "*Confirmation SMC + PA + SMV :*\n"
        f"• SMV : {signal['smv']}\n"
        f"• SMC : {signal['smc']}\n"
        f"• PA : {signal['pa']}\n\n"
        f"📡 Source : `{source}`\n"
        f"🕐 Bougie : `{signal['candle_time']}`"
    )

    return message


def signal_identifier(
    symbol: str,
    signal: Dict[str, Any],
) -> str:

    return (
        f"{symbol}|"
        f"{signal['direction']}|"
        f"{signal['candle_time']}"
    )


def send_signal(
    symbol: str,
    signal: Dict[str, Any],
    source: str,
) -> bool:

    global total_signals

    identifier = signal_identifier(
        symbol,
        signal,
    )

    with state_lock:

        if identifier in last_signals:
            logger.info(
                "Signal déjà envoyé : %s",
                identifier,
            )
            return False

    message = build_signal_message(
        symbol,
        signal,
        source,
    )

    sent = telegram_send_message(
        TELEGRAM_CHAT_ID,
        message,
    )

    if sent:

        with state_lock:

            last_signals[identifier] = {
                "symbol": symbol,
                "direction": signal["direction"],
                "entry": signal["entry"],
                "sent_at": utc_now(),
            }

            total_signals += 1

        logger.info(
            "Signal Telegram envoyé : %s %s",
            symbol,
            signal["direction"],
        )

        return True

    logger.error(
        "Échec envoi signal Telegram : %s",
        symbol,
    )

    return False


# ============================================================
# SCAN
# ============================================================

def scan_markets() -> None:

    global last_scan_at
    global last_scan_duration
    global total_scans
    global scan_in_progress

    if not scan_lock.acquire(blocking=False):

        logger.warning(
            "Scan déjà en cours. Nouveau scan ignoré."
        )

        return

    scan_in_progress = True
    started = time.monotonic()

    try:

        logger.info(
            "================================================"
        )

        logger.info(
            "DÉBUT SCAN MARCHÉS | 5 MINUTES"
        )

        for symbol in SYMBOLS:

            try:

                logger.info(
                    "Analyse de %s...",
                    symbol,
                )

                df, metadata = get_market_data(
                    symbol
                )

                update_price_state(
                    symbol,
                    metadata,
                )

                signal = analyze_smc_pa_smv(
                    df
                )

                if signal:

                    logger.info(
                        "SIGNAL VALIDÉ | %s | %s | "
                        "Entry=%s | SL=%s | TP1=%s | TP2=%s",
                        symbol,
                        signal["direction"],
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

                    send_signal(
                        symbol,
                        signal,
                        metadata["source"],
                    )

                else:

                    logger.info(
                        "Aucun signal valide pour %s.",
                        symbol,
                    )

            except Exception as exc:

                logger.exception(
                    "Erreur pendant analyse %s : %s",
                    symbol,
                    exc,
                )

        with state_lock:
            total_scans += 1
            last_scan_at = utc_now()
            last_scan_duration = (
                time.monotonic() - started
            )

        logger.info(
            "FIN SCAN | durée %.2fs",
            time.monotonic() - started,
        )

        logger.info(
            "================================================"
        )

    finally:

        scan_in_progress = False
        scan_lock.release()


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
# TELEGRAM ADMIN CALLBACKS
# ============================================================

def handle_callback_query(
    callback: Dict[str, Any],
) -> None:

    callback_id = callback.get("id")

    from_user = callback.get(
        "from",
        {},
    )

    user_id = from_user.get("id")

    data = callback.get(
        "data",
        "",
    )

    if not is_admin(user_id):

        telegram_answer_callback(
            callback_id,
            "⛔ Accès refusé.",
        )

        logger.warning(
            "Tentative accès admin refusée. "
            "Utilisateur Telegram : %s",
            user_id,
        )

        return

    if data == "admin_status":

        telegram_answer_callback(
            callback_id,
            "Statut actualisé.",
        )

        message = build_admin_status()

        message_chat = callback.get(
            "message",
            {},
        ).get(
            "chat",
            {},
        ).get(
            "id"
        )

        if message_chat:

            telegram_send_message(
                str(message_chat),
                message,
                admin_keyboard(),
            )

        return

    if data == "admin_prices":

        telegram_answer_callback(
            callback_id,
            "Prix actualisés.",
        )

        message = build_admin_prices()

        message_chat = callback.get(
            "message",
            {},
        ).get(
            "chat",
            {},
        ).get(
            "id"
        )

        if message_chat:

            telegram_send_message(
                str(message_chat),
                message,
                admin_keyboard(),
            )

        return

    if data == "admin_scan":

        telegram_answer_callback(
            callback_id,
            "Scan immédiat lancé.",
        )

        started = start_immediate_scan()

        message_chat = callback.get(
            "message",
            {},
        ).get(
            "chat",
            {},
        ).get(
            "id"
        )

        if message_chat:

            if started:

                message = (
                    "🔄 *SCAN IMMÉDIAT LANCÉ*\n\n"
                    "Le scan des 4 marchés vient "
                    "d'être déclenché."
                )

            else:

                message = (
                    "⚠️ *SCAN DÉJÀ EN COURS*\n\n"
                    "Aucun nouveau scan parallèle "
                    "n'a été lancé."
                )

            telegram_send_message(
                str(message_chat),
                message,
                admin_keyboard(),
            )

        return


# ============================================================
# TELEGRAM /admin
# ============================================================

def handle_admin_command(
    message: Dict[str, Any],
) -> None:

    from_user = message.get(
        "from",
        {},
    )

    user_id = from_user.get("id")

    chat = message.get(
        "chat",
        {},
    )

    chat_id = chat.get("id")

    if not is_admin(user_id):

        if chat_id:

            telegram_send_message(
                str(chat_id),
                "⛔ *Accès refusé.*",
            )

        logger.warning(
            "Commande /admin refusée pour utilisateur %s",
            user_id,
        )

        return

    telegram_send_message(
        str(chat_id),
        (
            "🔐 *PANNEAU ADMINISTRATION*\n\n"
            "Bienvenue dans l'interface privée "
            "du bot.\n\n"
            "Sélectionnez une action :"
        ),
        admin_keyboard(),
    )


# ============================================================
# TELEGRAM POLLING
# ============================================================

def telegram_polling_loop() -> None:

    global telegram_offset

    telegram_delete_webhook()

    logger.info(
        "Thread Telegram propriétaire démarré."
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
                    int(update["update_id"]) + 1
                )

                try:

                    message = update.get("message")

                    if message:

                        text = str(
                            message.get(
                                "text",
                                "",
                            )
                        ).strip()

                        if text.startswith("/admin"):

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
                        "Erreur traitement update Telegram : %s",
                        exc,
                    )

        except Exception as exc:

            logger.exception(
                "Erreur polling Telegram : %s",
                exc,
            )

            time.sleep(5)


# ============================================================
# TRADING LOOP
# ============================================================

def trading_loop() -> None:

    logger.info(
        "Thread principal de trading démarré."
    )

    # Premier scan immédiat au démarrage.
    scan_markets()

    while True:

        cycle_started = time.monotonic()

        try:

            scan_markets()

        except Exception as exc:

            logger.exception(
                "Erreur boucle trading : %s",
                exc,
            )

        elapsed = time.monotonic() - cycle_started

        sleep_for = max(
            1,
            SCAN_INTERVAL_SECONDS - elapsed,
        )

        logger.info(
            "Prochain scan dans %.0f secondes.",
            sleep_for,
        )

        time.sleep(sleep_for)


# ============================================================
# MAIN
# ============================================================

def main() -> None:

    validate_environment()

    logger.info(
        "================================================"
    )

    logger.info(
        "BOT TRADING SMC + PA + SMV"
    )

    logger.info(
        "Source principale : BIQUOTE"
    )

    logger.info(
        "Marchés : %s",
        ", ".join(SYMBOLS),
    )

    logger.info(
        "Timeframe : %s",
        TIMEFRAME,
    )

    logger.info(
        "Intervalle scan : %s secondes",
        SCAN_INTERVAL_SECONDS,
    )

    logger.info(
        "================================================"
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

    # Le processus principal reste vivant sur Railway.
    while True:

        time.sleep(60)


if __name__ == "__main__":
    main()