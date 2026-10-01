import os
import time
import logging
from typing import Optional, Tuple, Dict, Any

import requests
import pandas as pd
import numpy as np
import yfinance as yf


# ============================================================
# CONFIGURATION
# ============================================================

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY", "")
TWELVE_DATA_API_KEY = os.getenv("TWELVE_DATA_API_KEY", "")

POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "60"))

TIMEFRAME = "5m"
LOOKBACK_CANDLES = 100

ASSETS = {
    "XAUUSD": {
        "yahoo": "XAUUSD=X",
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

logger = logging.getLogger("trading-bot")


# ============================================================
# HTTP SESSION
# ============================================================

SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": (
            "Mozilla/5.0 (compatible; TradingSignalBot/1.0; "
            "+https://railway.app)"
        )
    }
)


# ============================================================
# DATA NORMALIZATION
# ============================================================

def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalise les données OHLCV dans un format homogène.
    """

    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy()

    # Gestion des MultiIndex de yfinance.
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [
            column[0] if isinstance(column, tuple) else column
            for column in df.columns
        ]

    rename_map = {
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Adj Close": "close",
        "Volume": "volume",
        "open": "open",
        "high": "high",
        "low": "low",
        "close": "close",
        "volume": "volume",
    }

    df = df.rename(columns=rename_map)

    required = ["open", "high", "low", "close", "volume"]

    for column in required:
        if column not in df.columns:
            df[column] = np.nan

    df = df[required].copy()

    for column in required:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    df = df.dropna(subset=["open", "high", "low", "close"])

    # Certains flux FX n'ont pas de volume réel.
    # On conserve NaN plutôt que d'inventer un volume.
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")

    if df.index.tz is not None:
        df.index = df.index.tz_convert("UTC").tz_localize(None)

    df = df[~df.index.duplicated(keep="last")]
    df = df.sort_index()

    return df.tail(LOOKBACK_CANDLES)


# ============================================================
# SOURCE 1 — YAHOO FINANCE
# ============================================================

def fetch_yahoo(asset: str) -> pd.DataFrame:
    """
    Source principale.
    Yahoo Finance limite historiquement les données intraday
    à une fenêtre récente, ce qui est suffisant pour le bot.
    """

    symbol = ASSETS[asset]["yahoo"]

    try:
        ticker = yf.Ticker(symbol)

        df = ticker.history(
            period="5d",
            interval=TIMEFRAME,
            auto_adjust=False,
            actions=False,
        )

        df = normalize_dataframe(df)

        if len(df) < 30:
            raise ValueError(
                f"Yahoo Finance: données insuffisantes pour {asset}"
            )

        logger.info("Yahoo Finance OK | %s | %d candles", asset, len(df))

        return df

    except Exception as exc:
        logger.warning(
            "Yahoo Finance FAILED | %s | %s",
            asset,
            exc,
        )
        return pd.DataFrame()


# ============================================================
# SOURCE 2 — COINGECKO
# ============================================================

def fetch_coingecko_btc() -> pd.DataFrame:
    """
    CoinGecko est utilisé exclusivement pour BTCUSD.

    L'API publique fournit des données de marché.
    Lorsque le volume OHLCV exact en 5m n'est pas disponible,
    les valeurs de volume restent NaN : aucun faux volume
    n'est fabriqué.
    """

    try:
        url = (
            "https://api.coingecko.com/api/v3/coins/bitcoin/"
            "market_chart"
        )

        params = {
            "vs_currency": "usd",
            "days": "1",
        }

        response = SESSION.get(
            url,
            params=params,
            timeout=15,
        )

        response.raise_for_status()

        payload = response.json()

        prices = payload.get("prices", [])

        if not prices:
            raise ValueError("CoinGecko: aucune donnée de prix")

        df = pd.DataFrame(
            prices,
            columns=["timestamp", "close"],
        )

        df["timestamp"] = pd.to_datetime(
            df["timestamp"],
            unit="ms",
            utc=True,
        )

        df = df.set_index("timestamp")

        # Construction OHLC à partir des observations de prix.
        # Le volume reste inconnu afin de ne pas fabriquer une donnée.
        df["open"] = df["close"].shift(1)
        df["high"] = df[["open", "close"]].max(axis=1)
        df["low"] = df[["open", "close"]].min(axis=1)
        df["volume"] = np.nan

        df = df[
            ["open", "high", "low", "close", "volume"]
        ].dropna(subset=["open", "high", "low", "close"])

        # Approximation temporelle en 5 minutes.
        df = df.resample("5min").agg(
            {
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "last",
            }
        )

        df = df.dropna(subset=["open", "high", "low", "close"])

        df = normalize_dataframe(df)

        if len(df) < 30:
            raise ValueError("CoinGecko: données insuffisantes")

        logger.info(
            "CoinGecko OK | BTCUSD | %d candles",
            len(df),
        )

        return df

    except Exception as exc:
        logger.warning(
            "CoinGecko FAILED | BTCUSD | %s",
            exc,
        )
        return pd.DataFrame()


# ============================================================
# SOURCE 3 — ALPHA VANTAGE
# ============================================================

def fetch_alpha_vantage(asset: str) -> pd.DataFrame:
    """
    Alpha Vantage fallback.

    Pour FX :
        FX_INTRADAY

    Pour BTC :
        DIGITAL_CURRENCY_INTRADAY
    """

    if not ALPHA_VANTAGE_API_KEY:
        logger.warning(
            "Alpha Vantage SKIPPED | clé API absente"
        )
        return pd.DataFrame()

    config = ASSETS[asset]

    try:
        if asset == "BTCUSD":
            url = "https://www.alphavantage.co/query"

            params = {
                "function": "DIGITAL_CURRENCY_INTRADAY",
                "symbol": "BTC",
                "market": "USD",
                "interval": "5min",
                "apikey": ALPHA_VANTAGE_API_KEY,
            }

            response = SESSION.get(
                url,
                params=params,
                timeout=20,
            )

            response.raise_for_status()

            payload = response.json()

            series = payload.get(
                "Time Series Crypto (5min)",
                {},
            )

            if not series:
                raise ValueError(
                    payload.get(
                        "Note",
                        payload.get(
                            "Information",
                            "Aucune donnée Alpha Vantage",
                        ),
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
                        "volume": values.get("5. volume"),
                    }
                )

            df = pd.DataFrame(rows)

        else:
            url = "https://www.alphavantage.co/query"

            params = {
                "function": "FX_INTRADAY",
                "from_symbol": config["alpha_from"],
                "to_symbol": config["alpha_to"],
                "interval": "5min",
                "outputsize": "compact",
                "apikey": ALPHA_VANTAGE_API_KEY,
            }

            response = SESSION.get(
                url,
                params=params,
                timeout=20,
            )

            response.raise_for_status()

            payload = response.json()

            series = payload.get(
                "Time Series FX (5min)",
                {},
            )

            if not series:
                raise ValueError(
                    payload.get(
                        "Note",
                        payload.get(
                            "Information",
                            "Aucune donnée Alpha Vantage",
                        ),
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
                        "volume": np.nan,
                    }
                )

            df = pd.DataFrame(rows)

        if df.empty:
            raise ValueError("DataFrame Alpha Vantage vide")

        df["timestamp"] = pd.to_datetime(
            df["timestamp"],
            utc=True,
        )

        df = df.set_index("timestamp")

        df = normalize_dataframe(df)

        if len(df) < 30:
            raise ValueError(
                "Alpha Vantage: données insuffisantes"
            )

        logger.info(
            "Alpha Vantage OK | %s | %d candles",
            asset,
            len(df),
        )

        return df

    except Exception as exc:
        logger.warning(
            "Alpha Vantage FAILED | %s | %s",
            asset,
            exc,
        )
        return pd.DataFrame()


# ============================================================
# SOURCE 4 — TWELVE DATA
# ============================================================

def fetch_twelve_data(asset: str) -> pd.DataFrame:
    """
    Dernier niveau de failover.
    """

    if not TWELVE_DATA_API_KEY:
        logger.warning(
            "Twelve Data SKIPPED | clé API absente"
        )
        return pd.DataFrame()

    symbol = ASSETS[asset]["twelve"]

    try:
        url = "https://api.twelvedata.com/time_series"

        params = {
            "symbol": symbol,
            "interval": "5min",
            "outputsize": 500,
            "apikey": TWELVE_DATA_API_KEY,
            "format": "JSON",
        }

        response = SESSION.get(
            url,
            params=params,
            timeout=20,
        )

        response.raise_for_status()

        payload = response.json()

        if "values" not in payload:
            raise ValueError(
                payload.get(
                    "message",
                    "Aucune donnée Twelve Data",
                )
            )

        rows = []

        for candle in payload["values"]:
            rows.append(
                {
                    "timestamp": candle.get("datetime"),
                    "open": candle.get("open"),
                    "high": candle.get("high"),
                    "low": candle.get("low"),
                    "close": candle.get("close"),
                    "volume": candle.get("volume", np.nan),
                }
            )

        df = pd.DataFrame(rows)

        if df.empty:
            raise ValueError("Twelve Data: DataFrame vide")

        df["timestamp"] = pd.to_datetime(
            df["timestamp"],
            utc=True,
        )

        df = df.set_index("timestamp")

        df = normalize_dataframe(df)

        if len(df) < 30:
            raise ValueError(
                "Twelve Data: données insuffisantes"
            )

        logger.info(
            "Twelve Data OK | %s | %d candles",
            asset,
            len(df),
        )

        return df

    except Exception as exc:
        logger.warning(
            "Twelve Data FAILED | %s | %s",
            asset,
            exc,
        )
        return pd.DataFrame()


# ============================================================
# DATA CASCADE
# ============================================================

def get_market_data(asset: str) -> Tuple[pd.DataFrame, str]:
    """
    Cascade stricte :

    1. Yahoo Finance
    2. CoinGecko pour BTCUSD
    3. Alpha Vantage
    4. Twelve Data
    """

    # --------------------------------------------------------
    # SOURCE 1
    # --------------------------------------------------------

    df = fetch_yahoo(asset)

    if not df.empty:
        return df, "Yahoo Finance"

    # --------------------------------------------------------
    # SOURCE 2
    # --------------------------------------------------------

    if asset == "BTCUSD":
        df = fetch_coingecko_btc()

        if not df.empty:
            return df, "CoinGecko"

    # --------------------------------------------------------
    # SOURCE 3
    # --------------------------------------------------------

    df = fetch_alpha_vantage(asset)

    if not df.empty:
        return df, "Alpha Vantage"

    # --------------------------------------------------------
    # SOURCE 4
    # --------------------------------------------------------

    df = fetch_twelve_data(asset)

    if not df.empty:
        return df, "Twelve Data"

    return pd.DataFrame(), "NONE"


# ============================================================
# PRICE ACTION HELPERS
# ============================================================

def candle_body(row: pd.Series) -> float:
    return abs(float(row["close"]) - float(row["open"]))


def candle_range(row: pd.Series) -> float:
    return float(row["high"]) - float(row["low"])


def bullish_engulfing(df: pd.DataFrame) -> bool:
    if len(df) < 2:
        return False

    previous = df.iloc[-2]
    current = df.iloc[-1]

    previous_bearish = previous["close"] < previous["open"]
    current_bullish = current["close"] > current["open"]

    if not (previous_bearish and current_bullish):
        return False

    previous_body_low = min(
        previous["open"],
        previous["close"],
    )

    previous_body_high = max(
        previous["open"],
        previous["close"],
    )

    current_body_low = min(
        current["open"],
        current["close"],
    )

    current_body_high = max(
        current["open"],
        current["close"],
    )

    engulfed = (
        current_body_low <= previous_body_low
        and current_body_high >= previous_body_high
    )

    total_range = candle_range(current)

    if total_range <= 0:
        return False

    body_ratio = candle_body(current) / total_range

    return bool(engulfed and body_ratio >= 0.60)


def bearish_engulfing(df: pd.DataFrame) -> bool:
    if len(df) < 2:
        return False

    previous = df.iloc[-2]
    current = df.iloc[-1]

    previous_bullish = previous["close"] > previous["open"]
    current_bearish = current["close"] < current["open"]

    if not (previous_bullish and current_bearish):
        return False

    previous_body_low = min(
        previous["open"],
        previous["close"],
    )

    previous_body_high = max(
        previous["open"],
        previous["close"],
    )

    current_body_low = min(
        current["open"],
        current["close"],
    )

    current_body_high = max(
        current["open"],
        current["close"],
    )

    engulfed = (
        current_body_low <= previous_body_low
        and current_body_high >= previous_body_high
    )

    total_range = candle_range(current)

    if total_range <= 0:
        return False

    body_ratio = candle_body(current) / total_range

    return bool(engulfed and body_ratio >= 0.60)


# ============================================================
# SMC — STRUCTURE / CHoCH
# ============================================================

def detect_market_structure(
    df: pd.DataFrame,
) -> Dict[str, Any]:
    """
    Détecte une structure simplifiée HH/HL/LH/LL
    à partir de pivots locaux.

    La validation du breakout utilise exclusivement
    le Close de la bougie, jamais la mèche.
    """

    if len(df) < 15:
        return {
            "direction": None,
            "level": None,
            "structure": None,
        }

    data = df.iloc[-15:].copy()

    highs = data["high"].values
    lows = data["low"].values
    closes = data["close"].values

    pivot_highs = []
    pivot_lows = []

    # Pivots avec fenêtre 3 :
    # gauche / pivot / droite.
    for i in range(1, len(data) - 1):
        if (
            highs[i] > highs[i - 1]
            and highs[i] > highs[i + 1]
        ):
            pivot_highs.append(
                {
                    "index": i,
                    "price": highs[i],
                }
            )

        if (
            lows[i] < lows[i - 1]
            and lows[i] < lows[i + 1]
        ):
            pivot_lows.append(
                {
                    "index": i,
                    "price": lows[i],
                }
            )

    if len(pivot_highs) < 2 or len(pivot_lows) < 2:
        return {
            "direction": None,
            "level": None,
            "structure": None,
        }

    previous_high = pivot_highs[-2]["price"]
    last_high = pivot_highs[-1]["price"]

    previous_low = pivot_lows[-2]["price"]
    last_low = pivot_lows[-1]["price"]

    bullish_structure = (
        last_high > previous_high
        and last_low > previous_low
    )

    bearish_structure = (
        last_high < previous_high
        and last_low < previous_low
    )

    last_close = closes[-1]

    # --------------------------------------------------------
    # CHoCH haussier :
    # structure précédente baissière,
    # puis clôture au-dessus du dernier swing high.
    # --------------------------------------------------------

    prior_bearish = (
        last_high < previous_high
        or last_low < previous_low
    )

    bullish_choch = (
        prior_bearish
        and last_close > last_high
    )

    # --------------------------------------------------------
    # CHoCH baissier :
    # structure précédente haussière,
    # puis clôture sous le dernier swing low.
    # --------------------------------------------------------

    prior_bullish = (
        last_high > previous_high
        or last_low > previous_low
    )

    bearish_choch = (
        prior_bullish
        and last_close < last_low
    )

    if bullish_choch:
        return {
            "direction": "BUY",
            "level": float(last_high),
            "structure": "Bullish CHoCH",
        }

    if bearish_choch:
        return {
            "direction": "SELL",
            "level": float(last_low),
            "structure": "Bearish CHoCH",
        }

    # --------------------------------------------------------
    # Aucun CHoCH confirmé
    # --------------------------------------------------------

    if bullish_structure:
        structure = "Bullish structure"
    elif bearish_structure:
        structure = "Bearish structure"
    else:
        structure = "Neutral structure"

    return {
        "direction": None,
        "level": None,
        "structure": structure,
    }


# ============================================================
# SMV — SMART MARKET VOLUME
# ============================================================

def validate_smv(df: pd.DataFrame) -> bool:
    """
    Conditions strictes :

    1. volume actuel > 2x moyenne des 20 précédentes bougies
    2. volume actuel > volume des 3 bougies précédentes
    """

    if len(df) < 24:
        return False

    volumes = pd.to_numeric(
        df["volume"],
        errors="coerce",
    )

    current_volume = volumes.iloc[-1]

    if pd.isna(current_volume):
        return False

    previous_20 = volumes.iloc[-21:-1]

    previous_3 = volumes.iloc[-4:-1]

    if previous_20.isna().any():
        return False

    if previous_3.isna().any():
        return False

    average_volume = previous_20.mean()

    if average_volume <= 0:
        return False

    condition_average = (
        current_volume > 2.0 * average_volume
    )

    condition_peak = (
        current_volume > previous_3.max()
    )

    return bool(
        condition_average
        and condition_peak
    )


# ============================================================
# SMC + PA + SMV COMPOSITE
# ============================================================

def analyze_smc_pa_smv(
    df: pd.DataFrame,
) -> Optional[Dict[str, Any]]:
    """
    Triple Composite :

        SMV + SMC + PA

    Un signal n'est retourné que si les trois blocs
    sont confirmés simultanément.
    """

    if df is None or len(df) < 30:
        return None

    df = normalize_dataframe(df)

    if len(df) < 30:
        return None

    # --------------------------------------------------------
    # 1. SMV
    # --------------------------------------------------------

    smv_valid = validate_smv(df)

    if not smv_valid:
        return None

    # --------------------------------------------------------
    # 2. SMC / CHoCH
    # --------------------------------------------------------

    structure = detect_market_structure(df)

    direction = structure["direction"]

    if direction is None:
        return None

    # --------------------------------------------------------
    # 3. PRICE ACTION
    # --------------------------------------------------------

    bullish_pa = bullish_engulfing(df)
    bearish_pa = bearish_engulfing(df)

    if direction == "BUY" and not bullish_pa:
        return None

    if direction == "SELL" and not bearish_pa:
        return None

    # --------------------------------------------------------
    # Prix d'entrée
    # --------------------------------------------------------

    signal_candle = df.iloc[-1]

    entry = float(signal_candle["close"])

    # --------------------------------------------------------
    # Structure récente pour SL
    #
    # On utilise les 5 dernières bougies précédant
    # la bougie de signal.
    # --------------------------------------------------------

    recent_structure = df.iloc[-6:-1]

    if len(recent_structure) < 5:
        return None

    if direction == "BUY":
        structure_low = float(
            recent_structure["low"].min()
        )

        sl = structure_low

        if sl >= entry:
            return None

        risk = entry - sl

        tp1 = entry + risk * 2.5
        tp2 = entry + risk * 5.0

    else:
        structure_high = float(
            recent_structure["high"].max()
        )

        sl = structure_high

        if sl <= entry:
            return None

        risk = sl - entry

        tp1 = entry - risk * 2.5
        tp2 = entry - risk * 5.0

    # --------------------------------------------------------
    # Ratio de risque
    # --------------------------------------------------------

    if risk <= 0:
        return None

    # --------------------------------------------------------
    # Volume info
    # --------------------------------------------------------

    current_volume = float(
        df["volume"].iloc[-1]
    )

    average_volume = float(
        df["volume"].iloc[-21:-1].mean()
    )

    volume_multiple = (
        current_volume / average_volume
        if average_volume > 0
        else 0
    )

    return {
        "direction": direction,
        "entry": entry,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "risk": risk,
        "structure": structure["structure"],
        "smv": True,
        "smc": True,
        "pa": True,
        "volume": current_volume,
        "average_volume": average_volume,
        "volume_multiple": volume_multiple,
        "candle_time": df.index[-1],
    }


# ============================================================
# TELEGRAM
# ============================================================

def escape_markdown(text: str) -> str:
    """
    Échappement minimal compatible Telegram Markdown classique.
    """

    characters = [
        "_",
        "*",
        "`",
        "[",
    ]

    for character in characters:
        text = text.replace(
            character,
            "\\" + character,
        )

    return text


def format_price(
    value: float,
    decimals: int,
) -> str:
    return f"{value:.{decimals}f}"


def send_telegram_signal(
    asset: str,
    signal: Dict[str, Any],
    source: str,
) -> bool:

    if not TELEGRAM_BOT_TOKEN:
        logger.error(
            "TELEGRAM_BOT_TOKEN absent"
        )
        return False

    if not TELEGRAM_CHAT_ID:
        logger.error(
            "TELEGRAM_CHAT_ID absent"
        )
        return False

    decimals = ASSETS[asset]["decimals"]

    direction = signal["direction"]

    order_type = (
        "ACHAT"
        if direction == "BUY"
        else "VENTE"
    )

    emoji = (
        "🟢"
        if direction == "BUY"
        else "🔴"
    )

    entry = format_price(
        signal["entry"],
        decimals,
    )

    sl = format_price(
        signal["sl"],
        decimals,
    )

    tp1 = format_price(
        signal["tp1"],
        decimals,
    )

    tp2 = format_price(
        signal["tp2"],
        decimals,
    )

    volume_multiple = (
        signal["volume_multiple"]
    )

    timestamp = signal["candle_time"]

    rationale = (
        f"SMV confirmé : volume = "
        f"{volume_multiple:.2f}x la moyenne des 20 "
        f"bougies précédentes et supérieur aux "
        f"3 volumes précédents.\n"
        f"SMC confirmé : {signal['structure']} "
        f"avec cassure validée par la clôture.\n"
        f"PA confirmé : bougie d'avalement avec "
        f"corps réel ≥ 60 % de la taille totale."
    )

    message = (
        f"{emoji} *SIGNAL TRIPLE COMPOSITE*\n\n"
        f"*Actif :* {asset}\n"
        f"*Ordre :* {order_type}\n"
        f"*Timeframe :* 5m\n"
        f"*Source :* {source}\n\n"
        f"*Entrée :* `{entry}`\n"
        f"*SL :* `{sl}`\n"
        f"*TP1 :* `{tp1}` — RR 1:2.5\n"
        f"*TP2 :* `{tp2}` — RR 1:5.0\n\n"
        f"*Rationale :*\n"
        f"{rationale}\n\n"
        f"_Bougie : {timestamp}_"
    )

    url = (
        f"https://api.telegram.org/bot"
        f"{TELEGRAM_BOT_TOKEN}/sendMessage"
    )

    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
    }

    try:
        response = SESSION.post(
            url,
            json=payload,
            timeout=15,
        )

        response.raise_for_status()

        result = response.json()

        if not result.get("ok"):
            raise RuntimeError(
                result.get(
                    "description",
                    "Erreur Telegram inconnue",
                )
            )

        logger.info(
            "Telegram signal envoyé | %s | %s",
            asset,
            order_type,
        )

        return True

    except Exception as exc:
        logger.error(
            "Telegram FAILED | %s | %s",
            asset,
            exc,
        )

        return False


# ============================================================
# DUPLICATE SIGNAL PROTECTION
# ============================================================

last_signals: Dict[str, str] = {}


def signal_is_new(
    asset: str,
    signal: Dict[str, Any],
) -> bool:

    candle_time = str(
        signal["candle_time"]
    )

    direction = signal["direction"]

    signature = (
        f"{asset}|"
        f"{candle_time}|"
        f"{direction}"
    )

    if last_signals.get(asset) == signature:
        return False

    last_signals[asset] = signature

    return True


# ============================================================
# SINGLE ASSET ANALYSIS
# ============================================================

def process_asset(asset: str) -> None:

    try:
        logger.info(
            "Analyse | %s | 5m",
            asset,
        )

        df, source = get_market_data(asset)

        if df.empty:
            logger.error(
                "Aucune donnée disponible | %s",
                asset,
            )
            return

        signal = analyze_smc_pa_smv(df)

        if signal is None:
            logger.info(
                "Aucun signal triple | %s",
                asset,
            )
            return

        if not signal_is_new(
            asset,
            signal,
        ):
            logger.info(
                "Signal déjà envoyé | %s",
                asset,
            )
            return

        send_telegram_signal(
            asset,
            signal,
            source,
        )

    except Exception:
        logger.exception(
            "Erreur pendant l'analyse | %s",
            asset,
        )


# ============================================================
# HEALTH / STARTUP TELEGRAM
# ============================================================

def send_startup_message() -> None:

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning(
            "Telegram non configuré."
        )
        return

    message = (
        "🤖 *Trading Signal Bot démarré*\n\n"
        "*Timeframe :* 5m\n"
        "*Actifs :* XAUUSD, BTCUSD, GBPUSD, EURUSD\n"
        "*Mode :* Triple Composite SMV + SMC + PA\n"
        "*TP :* 1:2.5 / 1:5.0"
    )

    url = (
        f"https://api.telegram.org/bot"
        f"{TELEGRAM_BOT_TOKEN}/sendMessage"
    )

    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
    }

    try:
        response = SESSION.post(
            url,
            json=payload,
            timeout=15,
        )

        response.raise_for_status()

        logger.info(
            "Message de démarrage Telegram envoyé."
        )

    except Exception as exc:
        logger.warning(
            "Impossible d'envoyer le message de démarrage : %s",
            exc,
        )


# ============================================================
# MAIN LOOP
# ============================================================

def main() -> None:

    logger.info(
        "================================================"
    )
    logger.info(
        "Trading Signal Bot — démarrage"
    )
    logger.info(
        "Timeframe : %s",
        TIMEFRAME,
    )
    logger.info(
        "Actifs : %s",
        ", ".join(ASSETS.keys()),
    )
    logger.info(
        "================================================"
    )

    send_startup_message()

    while True:

        cycle_start = time.time()

        for asset in ASSETS:

            process_asset(asset)

            # Petite pause pour éviter de marteler
            # les différentes APIs.
            time.sleep(2)

        elapsed = time.time() - cycle_start

        sleep_time = max(
            5,
            POLL_INTERVAL_SECONDS - elapsed,
        )

        logger.info(
            "Cycle terminé | durée %.1fs | prochain cycle dans %.1fs",
            elapsed,
            sleep_time,
        )

        time.sleep(sleep_time)


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()