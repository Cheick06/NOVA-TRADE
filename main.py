import os
import time
import json
import logging
import math
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests
from flask import Flask


# ==========================================
# CONFIGURATION ET LOGS
# ==========================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Nettoyage des variables Telegram
TELEGRAM_TOKEN = os.environ.get(
    "TELEGRAM_TOKEN",
    ""
).strip()

TELEGRAM_CHANNEL_ID = os.environ.get(
    "TELEGRAM_CHANNEL_ID",
    ""
).strip()

TELEGRAM_OWNER_ID = "5459538739"

# Les 4 actifs surveillés
SYMBOLS = [
    "BTCUSD",
    "XAUUSD",
    "EURUSD",
    "GBPUSD"
]

# Fichiers de persistance locale
TRADES_FILE = "active_trades.json"
SIGNALS_FILE = "processed_signals.json"
TRADE_HISTORY_FILE = "trade_history.json"
WEEKLY_REPORTS_FILE = "weekly_reports.json"

# Opportunités M15 en attente de surveillance M5/M1
OPPORTUNITIES_FILE = "pending_opportunities.json"

# Paramètres de la stratégie SMC + Price Action
M15_TRIGGER_PROXIMITY_PCT = float(
    os.environ.get("M15_TRIGGER_PROXIMITY_PCT", "0.25")
)
M15_PIVOT_LEFT = int(
    os.environ.get("M15_PIVOT_LEFT", "2")
)
M15_PIVOT_RIGHT = int(
    os.environ.get("M15_PIVOT_RIGHT", "2")
)
M5_PIVOT_LEFT = int(
    os.environ.get("M5_PIVOT_LEFT", "2")
)
M5_PIVOT_RIGHT = int(
    os.environ.get("M5_PIVOT_RIGHT", "2")
)
M1_PIVOT_LEFT = int(
    os.environ.get("M1_PIVOT_LEFT", "2")
)
M1_PIVOT_RIGHT = int(
    os.environ.get("M1_PIVOT_RIGHT", "2")
)
OPPORTUNITY_EXPIRY_CANDLES = int(
    os.environ.get("OPPORTUNITY_EXPIRY_CANDLES", "60")
)
SCAN_WORKERS = max(
    1,
    min(len(SYMBOLS), int(os.environ.get("SCAN_WORKERS", str(len(SYMBOLS)))))
)
JSON_LOCK = threading.RLock()
MARKET_EXECUTION_LOCK = threading.RLock()

# API BiQuote
BIQUOTE_BASE_URL = "https://biquote.io/api"


# ==========================================
# ÉTAT DES THREADS
# ==========================================

_threads_started = False
_threads_lock = threading.Lock()


# ==========================================
# TELEGRAM — DIAGNOSTIC CONFIGURATION
# ==========================================

def telegram_configuration_diagnostic():
    """
    Diagnostic sécurisé de la configuration Telegram.

    IMPORTANT :
    - Le token complet n'est jamais affiché.
    - Aucun secret Telegram n'est écrit dans les logs.
    - Ce diagnostic sert uniquement à vérifier ce que
      l'application reçoit depuis les variables d'environnement.
    """

    token_present = bool(TELEGRAM_TOKEN)
    channel_id_present = bool(TELEGRAM_CHANNEL_ID)

    token_length = len(TELEGRAM_TOKEN)

    token_has_colon = (
        TELEGRAM_TOKEN.count(":") == 1
    )

    token_has_whitespace = any(
        character.isspace()
        for character in TELEGRAM_TOKEN
    )

    token_has_bot_prefix = (
        TELEGRAM_TOKEN.lower().startswith("bot")
    )

    token_structure_ok = (
        token_present
        and token_has_colon
        and not token_has_whitespace
        and not token_has_bot_prefix
    )

    channel_id_numeric = False

    if channel_id_present:
        try:
            int(TELEGRAM_CHANNEL_ID)
            channel_id_numeric = True
        except (ValueError, TypeError):
            channel_id_numeric = False

    logging.info(
        "========== DIAGNOSTIC TELEGRAM =========="
    )

    logging.info(
        f"TELEGRAM_TOKEN présent : "
        f"{'OUI' if token_present else 'NON'}"
    )

    logging.info(
        f"TELEGRAM_TOKEN longueur : "
        f"{token_length}"
    )

    logging.info(
        f"TELEGRAM_TOKEN contient ':' : "
        f"{'OUI' if token_has_colon else 'NON'}"
    )

    logging.info(
        f"TELEGRAM_TOKEN contient des espaces : "
        f"{'OUI' if token_has_whitespace else 'NON'}"
    )

    logging.info(
        f"TELEGRAM_TOKEN commence par 'bot' : "
        f"{'OUI' if token_has_bot_prefix else 'NON'}"
    )

    logging.info(
        f"Structure générale du token : "
        f"{'OK' if token_structure_ok else 'INATTENDUE'}"
    )

    logging.info(
        f"TELEGRAM_CHANNEL_ID présent : "
        f"{'OUI' if channel_id_present else 'NON'}"
    )

    logging.info(
        f"TELEGRAM_CHANNEL_ID numérique : "
        f"{'OUI' if channel_id_numeric else 'NON'}"
    )

    logging.info(
        "=========================================="
    )

    return {
        "token_present": token_present,
        "token_length": token_length,
        "token_has_colon": token_has_colon,
        "token_has_whitespace": token_has_whitespace,
        "token_has_bot_prefix": token_has_bot_prefix,
        "token_structure_ok": token_structure_ok,
        "channel_id_present": channel_id_present,
        "channel_id_numeric": channel_id_numeric
    }


# ==========================================
# TELEGRAM
# ==========================================

def telegram_is_configured():
    return bool(
        TELEGRAM_TOKEN
        and TELEGRAM_CHANNEL_ID
    )


def telegram_owner_is_configured():
    return bool(
        TELEGRAM_TOKEN
        and TELEGRAM_OWNER_ID
    )


def telegram_api_url(method):
    return (
        f"https://api.telegram.org/"
        f"bot{TELEGRAM_TOKEN}/"
        f"{method}"
    )


def telegram_validate_and_prepare():
    """
    Vérifie le token Telegram et prépare le polling.

    getUpdates ne doit pas être utilisé avec un webhook actif.
    Le webhook est donc supprimé avant le démarrage du polling.
    """

    telegram_configuration_diagnostic()

    if not TELEGRAM_TOKEN:
        logging.warning(
            "Telegram non configuré : "
            "TELEGRAM_TOKEN manquant."
        )
        return False

    try:

        response = requests.get(
            telegram_api_url("getMe"),
            timeout=10
        )

        if response.status_code != 200:

            logging.error(
                f"Telegram getMe HTTP "
                f"{response.status_code}: "
                f"{response.text}"
            )

            return False

        data = response.json()

        if not data.get("ok", False):

            logging.error(
                f"Token Telegram refusé : {data}"
            )

            return False

        bot = data.get(
            "result",
            {}
        )

        logging.info(
            f"Telegram connecté : "
            f"@{bot.get('username', 'inconnu')} "
            f"(id={bot.get('id', 'inconnu')})"
        )

        delete_response = requests.post(
            telegram_api_url("deleteWebhook"),
            json={
                "drop_pending_updates": False
            },
            timeout=10
        )

        if delete_response.status_code != 200:

            logging.warning(
                f"Telegram deleteWebhook HTTP "
                f"{delete_response.status_code}: "
                f"{delete_response.text}"
            )

        else:

            delete_data = (
                delete_response.json()
            )

            if delete_data.get("ok", False):

                logging.info(
                    "Telegram polling préparé : "
                    "webhook supprimé."
                )

            else:

                logging.warning(
                    f"Telegram deleteWebhook refusé : "
                    f"{delete_data}"
                )

        return True

    except requests.RequestException as e:

        logging.error(
            f"Impossible de vérifier Telegram : {e}"
        )

    except ValueError as e:

        logging.error(
            f"Réponse Telegram invalide : {e}"
        )

    except Exception as e:

        logging.error(
            f"Erreur préparation Telegram : {e}"
        )

    return False


def format_telegram_price(symbol, price):
    try:
        value = float(price)
    except (TypeError, ValueError):
        return str(price)

    symbol = str(symbol).upper()
    decimals = 5 if symbol in ("EURUSD", "GBPUSD") else 2
    return f"{value:.{decimals}f}"


def send_telegram_message(
    message,
    reply_markup=None
):
    """Envoie un message de trading uniquement au canal/groupe Telegram."""

    if not telegram_is_configured():
        logging.warning(
            "Telegram non configuré : "
            "TELEGRAM_TOKEN ou TELEGRAM_CHANNEL_ID manquant."
        )
        return False

    payload = {
        "chat_id": TELEGRAM_CHANNEL_ID,
        "text": str(message)
    }

    if reply_markup is not None:
        payload["reply_markup"] = reply_markup

    try:
        response = requests.post(
            telegram_api_url("sendMessage"),
            json=payload,
            timeout=10
        )

        if response.status_code != 200:
            logging.error(
                f"Erreur Telegram HTTP {response.status_code}: {response.text}"
            )
            return False

        try:
            data = response.json()
        except ValueError as e:
            logging.error(
                f"Réponse Telegram invalide : {e}; corps={response.text}"
            )
            return False

        if not data.get("ok", False):
            logging.error(f"Telegram a refusé le message : {data}")
            return False

        logging.info(
            f"Message Telegram de trading envoyé avec succès vers {TELEGRAM_CHANNEL_ID}."
        )
        return True

    except requests.RequestException as e:
        logging.error(f"Erreur envoi Telegram : {e}")
    except Exception as e:
        logging.error(f"Erreur Telegram : {e}")

    return False


def send_telegram_owner_message(
    message,
    reply_markup=None
):
    """Envoie un message uniquement à l'interface personnelle du propriétaire."""

    if not telegram_owner_is_configured():
        logging.warning("Interface propriétaire Telegram non configurée : TELEGRAM_TOKEN manquant.")
        return False

    payload = {
        "chat_id": TELEGRAM_OWNER_ID,
        "text": str(message)
    }

    if reply_markup is not None:
        payload["reply_markup"] = reply_markup

    try:
        response = requests.post(
            telegram_api_url("sendMessage"),
            json=payload,
            timeout=10
        )

        if response.status_code != 200:
            logging.error(
                f"Erreur Telegram propriétaire HTTP {response.status_code}: {response.text}"
            )
            return False

        try:
            data = response.json()
        except ValueError as e:
            logging.error(
                f"Réponse Telegram propriétaire invalide : {e}; corps={response.text}"
            )
            return False

        if not data.get("ok", False):
            logging.error(f"Telegram a refusé le message propriétaire : {data}")
            return False

        logging.info("Message Telegram propriétaire envoyé avec succès.")
        return True

    except requests.RequestException as e:
        logging.error(f"Erreur envoi Telegram propriétaire : {e}")
    except Exception as e:
        logging.error(f"Erreur Telegram propriétaire : {e}")

    return False

def telegram_owner_id():
    """Retourne l'identifiant Telegram fixe du propriétaire."""
    return str(TELEGRAM_OWNER_ID).strip()


def is_telegram_owner(chat_id):
    """Vérifie que l'utilisateur est le propriétaire."""
    return (
        telegram_owner_is_configured()
        and str(chat_id).strip() == telegram_owner_id()
    )

def telegram_menu_keyboard():
    """
    Menu Telegram réservé au propriétaire.
    """

    return {
        "inline_keyboard": [
            [
                {
                    "text": "📊 Signaux actifs",
                    "callback_data": "active_signals"
                }
            ],
            [
                {
                    "text": "📈 Paires surveillées",
                    "callback_data": "watched_pairs"
                }
            ],
            [
                {
                    "text": "📊 Statistiques",
                    "callback_data": "stats"
                }
            ],
            [
                {
                    "text": "📖 Journal",
                    "callback_data": "journal"
                }
            ],
            [
                {
                    "text": "🤖 État du bot",
                    "callback_data": "bot_status"
                }
            ],
            [
                {
                    "text": "🔄 Actualiser",
                    "callback_data": "refresh"
                }
            ]
        ]
    }


def send_telegram_menu():
    """
    Affiche le menu propriétaire.
    """

    message = (
        "🤖 *NOVA TRADE*\n\n"
        "Menu propriétaire.\n"
        "Choisissez une action :"
    )

    return send_telegram_owner_message(
        message,
        reply_markup=telegram_menu_keyboard()
    )


def telegram_answer_callback(
    callback_query_id
):
    """
    Accuse réception d'un clic Telegram.
    """

    if not TELEGRAM_TOKEN:
        return False

    try:

        response = requests.post(
            telegram_api_url(
                "answerCallbackQuery"
            ),
            json={
                "callback_query_id":
                    callback_query_id
            },
            timeout=10
        )

        if response.status_code != 200:
            logging.error(
                f"Erreur Telegram answerCallbackQuery HTTP {response.status_code}: {response.text}"
            )
            return False

        try:
            data = response.json()
        except ValueError as e:
            logging.error(f"Réponse Telegram answerCallbackQuery invalide : {e}")
            return False

        if not data.get("ok", False):
            logging.error(f"Telegram a refusé answerCallbackQuery : {data}")
            return False

        return True

    except Exception as e:

        logging.error(
            f"Erreur answerCallbackQuery Telegram : {e}"
        )

        return False


def telegram_edit_message(
    chat_id,
    message_id,
    text
):
    """
    Modifie un message Telegram.
    """

    if not telegram_owner_is_configured():
        return False

    payload = {
        "chat_id": TELEGRAM_OWNER_ID,
        "message_id": message_id,
        "text": str(text),
        "reply_markup": telegram_menu_keyboard()
    }

    try:

        response = requests.post(
            telegram_api_url(
                "editMessageText"
            ),
            json=payload,
            timeout=10
        )

        if response.status_code != 200:

            logging.error(
                f"Erreur édition Telegram HTTP "
                f"{response.status_code}: "
                f"{response.text}"
            )

            return False

        try:
            data = response.json()
        except ValueError as e:
            logging.error(
                f"Réponse Telegram édition invalide : {e}; corps={response.text}"
            )
            return False

        if not data.get("ok", False):
            logging.error(f"Telegram a refusé l'édition du message : {data}")
            return False

        return True

    except Exception as e:

        logging.error(
            f"Erreur édition message Telegram : {e}"
        )

        return False


# ==========================================
# INFORMATIONS TELEGRAM
# ==========================================

def get_active_signals_message():
    """
    Affiche les signaux actuellement suivis.
    """

    active_trades = load_json(
        TRADES_FILE
    )

    if not active_trades:

        return (
            "📊 *SIGNAUX ACTIFS*\n\n"
            "Aucun signal actif actuellement."
        )

    lines = [
        "📊 *SIGNAUX ACTIFS*",
        ""
    ]

    count = 0

    for trade_id, trade in active_trades.items():

        try:

            symbol = trade.get(
                "symbol",
                "INCONNU"
            )

            direction = trade.get(
                "direction",
                "INCONNUE"
            )

            entry = float(
                trade.get(
                    "entry_price",
                    0
                )
            )

            current_sl = float(
                trade.get(
                    "current_sl",
                    0
                )
            )

            tp1 = float(
                trade.get(
                    "tp1",
                    0
                )
            )

            tp2 = float(
                trade.get(
                    "tp2",
                    0
                )
            )

            tp3 = float(
                trade.get(
                    "tp3",
                    0
                )
            )

            status = trade.get(
                "status",
                "ACTIVE"
            )

            lines.append(
                f"*{symbol}* — {direction}\n"
                f"Entrée : `{entry:.5f}`\n"
                f"SL actuel : `{current_sl:.5f}`\n"
                f"TP1 : `{tp1:.5f}`\n"
                f"TP2 : `{tp2:.5f}`\n"
                f"TP3 : `{tp3:.5f}`\n"
                f"Statut : `{status}`\n"
            )

            count += 1

        except Exception as e:

            logging.error(
                f"Erreur lecture signal "
                f"{trade_id}: {e}"
            )

    if count == 0:

        lines.append(
            "Aucun signal actif actuellement."
        )

    return "\n".join(lines)


def get_trading_journal_messages():
    """Retourne le journal complet en messages Telegram découpés."""
    history = _load_trade_history_list()

    if not history:
        return ["📖 *JOURNAL DE TRADING*\n\nAucun signal enregistré."]

    lines = ["📖 *JOURNAL DE TRADING*", f"Total : `{len(history)}`", ""]
    messages = []

    for trade in history:
        direction = trade.get("direction", "INCONNUE")
        result = trade.get("result", "UNKNOWN")
        status = trade.get("status", "UNKNOWN")
        lines.extend([
            f"*{trade.get('symbol', 'INCONNU')}* — {direction}",
            f"ID : `{trade.get('trade_id', '-')}`",
            f"Date : `{trade.get('datetime', trade.get('created_at', '-'))}`",
            f"Entrée : `{float(trade.get('entry', trade.get('entry_price', 0.0))):.8f}`",
            f"SL : `{float(trade.get('sl', trade.get('initial_sl', 0.0))):.8f}`",
            f"TP1 : `{float(trade.get('tp1', 0.0)):.8f}`",
            f"TP2 : `{float(trade.get('tp2', 0.0)):.8f}`",
            f"TP3 : `{float(trade.get('tp3', 0.0)):.8f}`",
            f"RR : `1:{float(trade.get('rr_theory', trade.get('rr_theoretical', 0.0))):.2f}`",
            f"Statut : `{status}`",
            f"Résultat : `{result}`",
            f"Résultat prix : `{float(trade.get('result_pips', 0.0)):.8f}`",
            ""
        ])

        if sum(len(x) + 1 for x in lines) > 3400:
            block = "\n".join(lines[:-13])
            if block.strip():
                messages.append(block)
            lines = lines[-13:]

    final = "\n".join(lines)
    if final.strip():
        messages.append(final)

    return messages


def get_watched_pairs_message():
    """
    Affiche les quatre actifs.
    """

    lines = [
        "📈 *PAIRES SURVEILLÉES*",
        ""
    ]

    for symbol in SYMBOLS:

        lines.append(
            f"✅ {symbol}"
        )

    lines.extend([
        "",
        "Contexte : H1",
        "Opportunités : M15",
        "Surveillance : M5",
        "Précision : M1 si nécessaire",
        "",
        "Indicateurs : EMA20/EMA50, RSI14, ATR14, ADX14",
        "Filtre : contextuel/contributif"
    ])

    return "\n".join(lines)


def get_bot_status_message():
    """
    Affiche l'état du bot.
    """

    active_trades = load_json(
        TRADES_FILE
    )

    telegram_status = (
        "🟢 Connecté"
        if telegram_is_configured()
        else
        "🔴 Non configuré"
    )

    lines = [
        "🤖 *ÉTAT DU BOT*",
        "",
        "🟢 Application : opérationnelle",
        "🟢 BiQuote : configuré",
        f"📨 Telegram : {telegram_status}",
        "🟢 Surveillance : active",
        "",
        f"📊 Signaux actifs : {len(active_trades)}",
        f"📈 Paires surveillées : {len(SYMBOLS)}",
        "",
        "📐 EMA20 / EMA50",
        "📐 RSI14",
        "📐 ATR14",
        "📐 ADX14",
        "🧭 Filtre contextuel : actif",
        ""
    ]

    for symbol in SYMBOLS:

        lines.append(
            f"• {symbol}"
        )

    return "\n".join(lines)


# ==========================================
# HISTORIQUE ET STATISTIQUES
# ==========================================

def utc_now_iso():
    """
    Retourne l'heure UTC actuelle au format ISO.
    """

    return datetime.utcnow().isoformat()


def _load_trade_history_list():
    with JSON_LOCK:
        if not os.path.exists(TRADE_HISTORY_FILE):
            try:
                save_json(TRADE_HISTORY_FILE, [])
            except Exception:
                pass
            return []

        try:
            with open(TRADE_HISTORY_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)

            if isinstance(data, list):
                return data

            if isinstance(data, dict):
                migrated = []
                for trade_id, trade in data.items():
                    if isinstance(trade, dict):
                        item = dict(trade)
                        item.setdefault("trade_id", str(trade_id))
                        migrated.append(item)
                try:
                    save_json(TRADE_HISTORY_FILE, migrated)
                except Exception:
                    pass
                return migrated

        except Exception as e:
            logging.error(f"Impossible de charger {TRADE_HISTORY_FILE}: {e}")

        return []


def add_to_trading_journal(
    symbol,
    direction,
    entry_price,
    stop_loss,
    tp1,
    tp2,
    tp3,
    rr_macro
):
    """Enregistre un signal dans trade_history.json sans bloquer le bot."""
    try:
        symbol = str(symbol).strip().upper()
        direction = str(direction).strip().upper()
        entry_price = float(entry_price)
        stop_loss = float(stop_loss)
        tp1 = float(tp1)
        tp2 = float(tp2)
        tp3 = float(tp3)
        rr_macro = float(rr_macro)

        if direction == "BUY":
            direction = "HAUSSIER"
        elif direction == "SELL":
            direction = "BAISSIER"

        trade_id = f"TRD-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8].upper()}"
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        record = {
            "trade_id": trade_id,
            "datetime": now,
            "symbol": symbol,
            "direction": direction,
            "entry": entry_price,
            "sl": stop_loss,
            "tp1": tp1,
            "tp2": tp2,
            "tp3": tp3,
            "rr_theory": rr_macro,
            "status": "PENDING_LIMIT",
            "result": "UNKNOWN",
            "result_pips": 0.0,
            "events": []
        }

        with JSON_LOCK:
            history = _load_trade_history_list()
            history.append(record)
            save_json(TRADE_HISTORY_FILE, history)

        return trade_id

    except Exception as e:
        logging.error(f"Erreur journal trading : {e}")
        return None


def _find_trade_history_record(history, trade_id):
    trade_id = str(trade_id)
    for trade in history:
        if str(trade.get("trade_id")) == trade_id:
            return trade
    return None


def ensure_trade_history_record(trade_id, trade):
    try:
        direction = "HAUSSIER" if trade.get("direction") == "BUY" else "BAISSIER"
        journal_id = add_to_trading_journal(
            symbol=trade.get("symbol", "INCONNU"),
            direction=direction,
            entry_price=trade.get("entry_price", 0.0),
            stop_loss=trade.get("initial_sl", trade.get("current_sl", 0.0)),
            tp1=trade.get("tp1", 0.0),
            tp2=trade.get("tp2", 0.0),
            tp3=trade.get("tp3", 0.0),
            rr_macro=trade.get("rr_theoretical", 0.0)
        )

        if journal_id is None:
            return

        with JSON_LOCK:
            history = _load_trade_history_list()
            record = _find_trade_history_record(history, journal_id)
            if record is not None:
                record["trade_id"] = trade_id
                record["created_at"] = trade.get("created_at", utc_now_iso())
                record["current_sl"] = trade.get("current_sl", trade.get("initial_sl"))
                record["pattern"] = trade.get("pattern")
                record["filter_score"] = trade.get("filter_score")
                record["indicators"] = trade.get("indicators", {})
                record["active_trade_id"] = trade_id
                save_json(TRADE_HISTORY_FILE, history)

    except Exception as e:
        logging.error(f"Erreur création journal pour {trade_id}: {e}")


def record_trade_event(trade_id, event, price=None):
    try:
        with JSON_LOCK:
            history = _load_trade_history_list()
            trade = _find_trade_history_record(history, trade_id)
            if trade is None:
                return

            event_data = {
                "event": event,
                "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            }
            if price is not None:
                event_data["price"] = float(price)

            trade.setdefault("events", []).append(event_data)

            if event == "LIMIT_FILLED":
                trade["status"] = "ACTIVE"
            elif event == "TP1_HIT":
                trade["tp1_hit"] = True
            elif event == "TP2_HIT":
                trade["tp2_hit"] = True
            elif event == "TP3_HIT":
                trade["tp3_hit"] = True

            save_json(TRADE_HISTORY_FILE, history)

    except Exception as e:
        logging.error(f"Erreur événement journal {trade_id}: {e}")


def close_trade_in_history(trade_id, result, close_price):
    try:
        with JSON_LOCK:
            history = _load_trade_history_list()
            trade = _find_trade_history_record(history, trade_id)
            if trade is None:
                logging.warning(f"Historique absent pour le trade {trade_id}.")
                return

            trade["status"] = "CLOSED"
            trade["result"] = str(result)
            trade["close_price"] = float(close_price)
            trade["closed_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

            entry = float(trade.get("entry", 0.0))
            direction = trade.get("direction", "")
            if direction == "HAUSSIER":
                trade["result_pips"] = float(close_price - entry)
            else:
                trade["result_pips"] = float(entry - close_price)

            trade.setdefault("events", []).append({
                "event": str(result),
                "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                "price": float(close_price)
            })
            save_json(TRADE_HISTORY_FILE, history)

    except Exception as e:
        logging.error(f"Erreur clôture journal {trade_id}: {e}")


def get_statistics(
    start_datetime=None,
    end_datetime=None
):
    """
    Calcule les statistiques sur l'historique.

    Les signaux générés sont comptés selon leur date
    de création.

    Les résultats TP/SL sont comptés selon leur date
    de clôture.
    """

    history = _load_trade_history_list()

    statistics = {
        "signals_generated": 0,
        "buy_signals": 0,
        "sell_signals": 0,
        "tp1_hits": 0,
        "tp2_hits": 0,
        "tp3_hits": 0,
        "break_even": 0,
        "closed_tp3": 0,
        "closed_sl": 0,
        "closed_other": 0,
        "still_active": 0,
        "symbols": {},
        "trades": []
    }

    for trade in history:

        trade_id = trade.get("trade_id")

        try:

            created_at = trade.get(
                "created_at"
            )

            closed_at = trade.get(
                "closed_at"
            )

            created_dt = None
            closed_dt = None

            if created_at:

                created_dt = (
                    datetime.fromisoformat(
                        created_at
                    )
                )

            if closed_at:

                closed_dt = (
                    datetime.fromisoformat(
                        closed_at
                    )
                )

            created_in_period = True

            if start_datetime is not None:

                created_in_period = (
                    created_dt is not None
                    and
                    created_dt >= start_datetime
                )

            if end_datetime is not None:

                created_in_period = (
                    created_in_period
                    and
                    created_dt is not None
                    and
                    created_dt < end_datetime
                )

            closed_in_period = True

            if start_datetime is not None:

                closed_in_period = (
                    closed_dt is not None
                    and
                    closed_dt >= start_datetime
                )

            if end_datetime is not None:

                closed_in_period = (
                    closed_in_period
                    and
                    closed_dt is not None
                    and
                    closed_dt < end_datetime
                )

            events = trade.get(
                "events",
                []
            )

            if created_in_period:

                statistics[
                    "signals_generated"
                ] += 1

                direction = trade.get(
                    "direction"
                )

                if direction in ("BUY", "HAUSSIER"):

                    statistics[
                        "buy_signals"
                    ] += 1

                elif direction in ("SELL", "BAISSIER"):

                    statistics[
                        "sell_signals"
                    ] += 1

                symbol = trade.get(
                    "symbol",
                    "INCONNU"
                )

                if symbol not in statistics[
                    "symbols"
                ]:

                    statistics[
                        "symbols"
                    ][symbol] = 0

                statistics[
                    "symbols"
                ][symbol] += 1

            for event in events:

                event_name = event.get(
                    "event"
                )

                event_datetime = None

                event_timestamp = event.get(
                    "timestamp"
                )

                if event_timestamp:

                    try:

                        event_datetime = (
                            datetime.fromisoformat(
                                event_timestamp
                            )
                        )

                    except Exception:
                        event_datetime = None

                event_in_period = True

                if start_datetime is not None:

                    event_in_period = (
                        event_datetime is not None
                        and
                        event_datetime >= start_datetime
                    )

                if end_datetime is not None:

                    event_in_period = (
                        event_in_period
                        and
                        event_datetime is not None
                        and
                        event_datetime < end_datetime
                    )

                if not event_in_period:
                    continue

                if event_name == "TP1_HIT":

                    statistics[
                        "tp1_hits"
                    ] += 1

                elif event_name == "TP2_HIT":

                    statistics[
                        "tp2_hits"
                    ] += 1

                elif event_name == "TP3_HIT":

                    statistics[
                        "tp3_hits"
                    ] += 1

                elif event_name == "BREAK_EVEN":

                    statistics[
                        "break_even"
                    ] += 1

            if trade.get("status") == "CLOSED":

                if not closed_in_period:
                    continue

                result = trade.get(
                    "result"
                )

                if result == "TP3":

                    statistics[
                        "closed_tp3"
                    ] += 1

                elif result == "SL":

                    statistics[
                        "closed_sl"
                    ] += 1

                else:

                    statistics[
                        "closed_other"
                    ] += 1

            else:

                if created_in_period:

                    statistics[
                        "still_active"
                    ] += 1

            statistics[
                "trades"
            ].append({
                "trade_id": trade_id,
                "symbol": trade.get(
                    "symbol",
                    "INCONNU"
                ),
                "direction": trade.get(
                    "direction",
                    "INCONNUE"
                ),
                "created_at": created_at,
                "closed_at": closed_at,
                "result": trade.get(
                    "result"
                )
            })

        except Exception as e:

            logging.error(
                f"Erreur calcul statistiques "
                f"du trade {trade_id}: {e}"
            )

    total_closed = (
        statistics["closed_tp3"]
        + statistics["closed_sl"]
        + statistics["closed_other"]
    )

    if total_closed > 0:

        statistics["tp3_rate"] = (
            statistics["closed_tp3"]
            / total_closed
            * 100
        )

        statistics["sl_rate"] = (
            statistics["closed_sl"]
            / total_closed
            * 100
        )

    else:

        statistics["tp3_rate"] = 0
        statistics["sl_rate"] = 0

    return statistics


def format_statistics_message(
    statistics,
    title="📊 STATISTIQUES NOVA TRADE"
):
    """
    Formate les statistiques pour Telegram.
    """

    total_closed = (
        statistics["closed_tp3"]
        + statistics["closed_sl"]
        + statistics["closed_other"]
    )

    lines = [
        f"*{title}*",
        "",
        "📡 *SIGNAUX*",
        f"• Signaux générés : "
        f"{statistics['signals_generated']}",
        f"• BUY : "
        f"{statistics['buy_signals']}",
        f"• SELL : "
        f"{statistics['sell_signals']}",
        "",
        "🎯 *RÉSULTATS*",
        f"• TP3 : "
        f"{statistics['closed_tp3']}",
        f"• SL : "
        f"{statistics['closed_sl']}",
        f"• Autres clôtures : "
        f"{statistics['closed_other']}",
        f"• Encore actifs : "
        f"{statistics['still_active']}",
        "",
        "📈 *ÉTAPES ATTEINTES*",
        f"• TP1 : "
        f"{statistics['tp1_hits']}",
        f"• TP2 : "
        f"{statistics['tp2_hits']}",
        f"• TP3 : "
        f"{statistics['tp3_hits']}",
        f"• Break-Even : "
        f"{statistics['break_even']}",
        "",
        "📊 *TAUX DE CLÔTURE*",
        f"• Trades clôturés : "
        f"{total_closed}",
        f"• TP3 : "
        f"{statistics['tp3_rate']:.1f} %",
        f"• SL : "
        f"{statistics['sl_rate']:.1f} %"
    ]

    if statistics["symbols"]:

        lines.extend([
            "",
            "📋 *PAR ACTIF*"
        ])

        for symbol in SYMBOLS:

            count = statistics[
                "symbols"
            ].get(
                symbol,
                0
            )

            lines.append(
                f"• {symbol} : {count}"
            )

    return "\n".join(lines)


def get_all_time_statistics_message():
    """
    Retourne les statistiques depuis le début
    de l'historique.
    """

    statistics = get_statistics()

    return format_statistics_message(
        statistics,
        title="📊 STATISTIQUES NOVA TRADE"
    )


def generate_weekly_report():
    """
    Génère et envoie le rapport de fin de semaine.

    Le rapport est exécuté le samedi à 00:00 UTC,
    correspondant à la fin de la semaine de trading
    vendredi 23:59.

    Période :
    lundi 00:00 -> samedi 00:00.
    """

    now = datetime.utcnow()

    monday = (
        now
        - timedelta(
            days=now.weekday()
        )
    ).replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0
    )

    saturday = monday + timedelta(
        days=5
    )

    if now.weekday() != 5:
        return False

    if now.hour != 0 or now.minute != 0:
        return False

    report_key = saturday.strftime(
        "%Y-%m-%d"
    )

    weekly_reports = load_json(
        WEEKLY_REPORTS_FILE
    )

    if report_key in weekly_reports:

        return False

    statistics = get_statistics(
        start_datetime=monday,
        end_datetime=saturday
    )

    period_text = (
        f"{monday.strftime('%d/%m/%Y')} "
        f"→ "
        f"{(saturday - timedelta(seconds=1)).strftime('%d/%m/%Y')}"
    )

    report_message = (
        "📊 *RAPPORT HEBDOMADAIRE "
        "NOVA TRADE*\n\n"
        f"Période : {period_text}\n\n"
        + format_statistics_message(
            statistics,
            title="📊 RÉSULTATS DE LA SEMAINE"
        )
    )

    sent = send_telegram_message(
        report_message
    )

    if not sent:

        logging.error(
            "Impossible d'envoyer le rapport "
            f"hebdomadaire {report_key}."
        )

        return False

    weekly_reports[report_key] = {
        "period_start": monday.isoformat(),
        "period_end": saturday.isoformat(),
        "generated_at": now.isoformat(),
        "statistics": statistics
    }

    save_json(
        WEEKLY_REPORTS_FILE,
        weekly_reports
    )

    logging.info(
        f"Rapport hebdomadaire envoyé : "
        f"{report_key}"
    )

    return True


def handle_telegram_callback(
    callback_query
):
    """
    Traite uniquement les boutons du propriétaire.
    """

    try:

        callback_id = callback_query.get(
            "id"
        )

        message = callback_query.get(
            "message",
            {}
        )

        chat = message.get(
            "chat",
            {}
        )

        chat_id = chat.get(
            "id"
        )

        message_id = message.get(
            "message_id"
        )

        data = callback_query.get(
            "data",
            ""
        )

        if not is_telegram_owner(
            chat_id
        ):

            telegram_answer_callback(
                callback_id
            )

            logging.warning(
                f"Accès Telegram refusé "
                f"pour chat_id={chat_id}"
            )

            return

        telegram_answer_callback(
            callback_id
        )

        if data == "active_signals":

            text = (
                get_active_signals_message()
            )

        elif data == "watched_pairs":

            text = (
                get_watched_pairs_message()
            )

        elif data == "stats":

            text = (
                get_all_time_statistics_message()
            )

        elif data == "journal":

            journal_messages = get_trading_journal_messages()
            text = journal_messages[0]

            for extra_message in journal_messages[1:]:
                send_telegram_owner_message(extra_message, reply_markup=telegram_menu_keyboard())

        elif data == "bot_status":

            text = (
                get_bot_status_message()
            )

        elif data == "refresh":

            text = (
                get_bot_status_message()
                + "\n\n"
                + get_active_signals_message()
            )

        else:

            return

        if message_id is not None:

            telegram_edit_message(
                chat_id,
                message_id,
                text
            )

    except Exception as e:

        logging.exception(
            f"Erreur traitement bouton Telegram : {e}"
        )


def telegram_polling_loop():
    """
    Écoute les commandes et boutons Telegram.
    """

    telegram_configuration_diagnostic()

    if not telegram_owner_is_configured():

        logging.warning(
            "Thread Telegram non démarré : "
            "TELEGRAM_TOKEN manquant."
        )

        return

    if not telegram_validate_and_prepare():

        logging.error(
            "Thread Telegram arrêté : "
            "token Telegram invalide ou API inaccessible."
        )

        return

    logging.info(
        "Thread de contrôle Telegram démarré."
    )

    offset = None

    while True:

        try:

            params = {
                "timeout": 20
            }

            if offset is not None:

                params["offset"] = offset

            response = requests.get(
                telegram_api_url(
                    "getUpdates"
                ),
                params=params,
                timeout=30
            )

            if response.status_code != 200:

                logging.error(
                    f"Telegram getUpdates HTTP "
                    f"{response.status_code}: "
                    f"{response.text}"
                )

                time.sleep(5)
                continue

            data = response.json()

            if not data.get("ok", False):

                logging.error(
                    f"Telegram getUpdates invalide : "
                    f"{data}"
                )

                time.sleep(5)
                continue

            updates = data.get(
                "result",
                []
            )

            for update in updates:

                update_id = update.get(
                    "update_id"
                )

                if update_id is not None:

                    offset = update_id + 1

                if "callback_query" in update:

                    handle_telegram_callback(
                        update["callback_query"]
                    )

                    continue

                message = update.get(
                    "message"
                )

                if not message:
                    continue

                chat = message.get(
                    "chat",
                    {}
                )

                chat_id = chat.get(
                    "id"
                )

                text = message.get(
                    "text",
                    ""
                )

                if not is_telegram_owner(
                    chat_id
                ):

                    logging.warning(
                        f"Message Telegram ignoré : "
                        f"chat_id={chat_id}"
                    )

                    continue

                if text in [
                    "/start",
                    "/menu"
                ]:

                    send_telegram_menu()

                elif text == "/stats":

                    send_telegram_owner_message(
                        get_all_time_statistics_message(),
                        reply_markup=telegram_menu_keyboard()
                    )

        except requests.RequestException as e:

            logging.error(
                f"Erreur connexion Telegram polling : {e}"
            )

            time.sleep(5)

        except Exception as e:

            logging.exception(
                f"Erreur boucle Telegram : {e}"
            )

            time.sleep(5)


# ==========================================
# PERSISTANCE JSON THREAD-SAFE
# ==========================================

def load_json(filename):
    with JSON_LOCK:
        if not os.path.exists(filename):
            return {}

        try:
            with open(
                filename,
                "r",
                encoding="utf-8"
            ) as f:
                data = json.load(f)

            if isinstance(data, dict):
                return data

            logging.warning(
                f"Le fichier {filename} ne contient pas un objet JSON valide."
            )

        except Exception as e:
            logging.error(
                f"Impossible de charger {filename}: {e}"
            )

        return {}


def save_json(
    filename,
    data
):
    with JSON_LOCK:
        temp_filename = f"{filename}.tmp"

        try:
            with open(
                temp_filename,
                "w",
                encoding="utf-8"
            ) as f:
                json.dump(
                    data,
                    f,
                    indent=4,
                    ensure_ascii=False
                )
                f.flush()
                os.fsync(f.fileno())

            os.replace(
                temp_filename,
                filename
            )

        except Exception as e:
            logging.error(
                f"Impossible de sauvegarder {filename}: {e}"
            )

            try:
                if os.path.exists(temp_filename):
                    os.remove(temp_filename)
            except OSError:
                pass


# ==========================================
# BIQUOTE — OHLC
# ==========================================

def fetch_biquote_ohlcv(
    symbol,
    timeframe="15m",
    count=100
):
    """
    Récupère les bougies OHLC depuis BiQuote.
    """

    url = (
        f"{BIQUOTE_BASE_URL}/"
        f"{symbol}/ohlc"
    )

    params = {
        "interval": timeframe,
        "limit": count
    }

    try:

        response = requests.get(
            url,
            params=params,
            timeout=15
        )

        if response.status_code != 200:

            logging.error(
                f"Erreur API BiQuote "
                f"({response.status_code}) "
                f"pour {symbol} {timeframe}: "
                f"{response.text}"
            )

            return pd.DataFrame()

        data = response.json()

        bars = data.get(
            "bars"
        )

        if not isinstance(
            bars,
            list
        ):

            logging.error(
                f"Réponse BiQuote invalide pour "
                f"{symbol} {timeframe}: "
                f"clé 'bars' absente."
            )

            return pd.DataFrame()

        if not bars:

            logging.warning(
                f"BiQuote ne retourne aucune bougie "
                f"pour {symbol} {timeframe}."
            )

            return pd.DataFrame()

        df = pd.DataFrame(
            bars
        )

        if "openTime" in df.columns:

            df["timestamp"] = df[
                "openTime"
            ]

        required_columns = [
            "timestamp",
            "open",
            "high",
            "low",
            "close"
        ]

        missing = [
            column
            for column in required_columns
            if column not in df.columns
        ]

        if missing:

            logging.error(
                f"Colonnes manquantes dans "
                f"BiQuote {symbol} {timeframe}: "
                f"{missing}"
            )

            return pd.DataFrame()

        for column in [
            "open",
            "high",
            "low",
            "close"
        ]:

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

        df = (
            df.sort_values(
                "timestamp"
            )
            .reset_index(
                drop=True
            )
        )

        return df

    except requests.RequestException as e:

        logging.error(
            f"Impossible de joindre "
            f"l'API BiQuote : {e}"
        )

    except ValueError as e:

        logging.error(
            f"Réponse JSON BiQuote invalide : {e}"
        )

    except Exception as e:

        logging.error(
            f"Erreur récupération OHLC BiQuote : {e}"
        )

    return pd.DataFrame()


# ==========================================
# BIQUOTE — PRIX LIVE
# ==========================================

def fetch_biquote_live_price(
    symbol
):
    """
    Récupère le prix mid actuel.
    """

    url = (
        f"{BIQUOTE_BASE_URL}/"
        f"{symbol}"
    )

    try:

        response = requests.get(
            url,
            timeout=10
        )

        if response.status_code != 200:

            logging.error(
                f"Erreur prix BiQuote HTTP "
                f"{response.status_code} "
                f"pour {symbol}: "
                f"{response.text}"
            )

            return None

        data = response.json()

        mid = data.get(
            "mid"
        )

        if mid is None:

            logging.error(
                f"BiQuote ne retourne pas "
                f"de 'mid' pour {symbol}."
            )

            return None

        return float(mid)

    except requests.RequestException as e:

        logging.error(
            f"Erreur connexion prix live "
            f"BiQuote {symbol} : {e}"
        )

    except (
        ValueError,
        TypeError
    ) as e:

        logging.error(
            f"Prix BiQuote invalide "
            f"pour {symbol}: {e}"
        )

    except Exception as e:

        logging.error(
            f"Erreur prix live BiQuote "
            f"{symbol}: {e}"
        )

    return None


# ==========================================
# RECONNAISSANCE DES CHANDELIERS
# ==========================================

def detect_patterns(
    df
):

    if len(df) < 4:
        return None

    try:

        c_open = float(
            df["open"].iloc[-2]
        )

        c_close = float(
            df["close"].iloc[-2]
        )

        c_high = float(
            df["high"].iloc[-2]
        )

        c_low = float(
            df["low"].iloc[-2]
        )

        p_open = float(
            df["open"].iloc[-3]
        )

        p_close = float(
            df["close"].iloc[-3]
        )

    except (
        ValueError,
        TypeError,
        KeyError
    ):

        return None

    body = abs(
        c_close - c_open
    )

    if body <= 0:
        return None

    if (
        p_close < p_open
        and c_close > c_open
        and c_close >= p_open
        and c_open <= p_close
    ):

        return "AVALEMENT_HAUSSIER"

    if (
        p_close > p_open
        and c_close < c_open
        and c_close <= p_open
        and c_open >= p_close
    ):

        return "AVALEMENT_BAISSIER"

    lower_shadow = (
        min(c_open, c_close)
        - c_low
    )

    upper_shadow = (
        c_high
        - max(c_open, c_close)
    )

    if (
        c_close > c_open
        and lower_shadow >= (2 * body)
        and upper_shadow < (0.5 * body)
    ):

        return "MARTEAU"

    if (
        c_close < c_open
        and upper_shadow >= (2 * body)
        and lower_shadow < (0.5 * body)
    ):

        return "ETOILE_FILANTE"

    return None


# ==========================================
# EXTRACTION DES ZONES H1
# ==========================================

def get_h1_zones(
    symbol
):

    df = fetch_biquote_ohlcv(
        symbol,
        timeframe="1h",
        count=100
    )

    if df.empty:
        return [], []

    try:

        df["tr"] = (
            df["high"]
            - df["low"]
        )

        atr = (
            df["tr"]
            .rolling(14)
            .mean()
            .iloc[-1]
        )

        if (
            pd.isna(atr)
            or atr <= 0
        ):

            logging.warning(
                f"ATR H1 invalide "
                f"pour {symbol}."
            )

            return [], []

        thickness = (
            0.25
            * float(atr)
        )

        supports = []
        resistances = []

        for i in range(
            5,
            len(df) - 5
        ):

            if (
                df["low"].iloc[i]
                ==
                df["low"]
                .iloc[
                    i - 5:i + 6
                ]
                .min()
            ):

                level = float(
                    df["low"].iloc[i]
                )

                supports.append({
                    "level": level,
                    "low_band": (
                        level
                        - thickness
                    ),
                    "high_band": (
                        level
                        + thickness
                    )
                })

            if (
                df["high"].iloc[i]
                ==
                df["high"]
                .iloc[
                    i - 5:i + 6
                ]
                .max()
            ):

                level = float(
                    df["high"].iloc[i]
                )

                resistances.append({
                    "level": level,
                    "low_band": (
                        level
                        - thickness
                    ),
                    "high_band": (
                        level
                        + thickness
                    )
                })

        return (
            supports[-3:],
            resistances[-3:]
        )

    except Exception as e:

        logging.error(
            f"Erreur extraction zones H1 "
            f"{symbol}: {e}"
        )

        return [], []


# ==========================================
# INDICATEURS M15
# ==========================================

def calculate_indicators(
    df
):
    """
    Calcule :
    - EMA20
    - EMA50
    - RSI14
    - ATR14
    - ADX14
    """

    if (
        df.empty
        or len(df) < 50
    ):

        return df

    result = df.copy()

    close = result["close"]
    high = result["high"]
    low = result["low"]

    result["ema20"] = (
        close
        .ewm(
            span=20,
            adjust=False,
            min_periods=20
        )
        .mean()
    )

    result["ema50"] = (
        close
        .ewm(
            span=50,
            adjust=False,
            min_periods=50
        )
        .mean()
    )

    delta = close.diff()

    gain = delta.clip(
        lower=0
    )

    loss = -delta.clip(
        upper=0
    )

    avg_gain = (
        gain
        .ewm(
            alpha=1 / 14,
            adjust=False,
            min_periods=14
        )
        .mean()
    )

    avg_loss = (
        loss
        .ewm(
            alpha=1 / 14,
            adjust=False,
            min_periods=14
        )
        .mean()
    )

    rs = (
        avg_gain
        / avg_loss.replace(
            0,
            pd.NA
        )
    )

    result["rsi14"] = (
        100
        - (
            100
            / (
                1
                + rs
            )
        )
    )

    previous_close = (
        close.shift(1)
    )

    tr = pd.concat(
        [
            high - low,
            (
                high
                - previous_close
            ).abs(),
            (
                low
                - previous_close
            ).abs()
        ],
        axis=1
    ).max(
        axis=1
    )

    result["atr14"] = (
        tr
        .rolling(
            14,
            min_periods=14
        )
        .mean()
    )

    up_move = high.diff()

    down_move = -low.diff()

    plus_dm = pd.Series(
        0.0,
        index=result.index
    )

    minus_dm = pd.Series(
        0.0,
        index=result.index
    )

    plus_condition = (
        (up_move > down_move)
        & (up_move > 0)
    )

    minus_condition = (
        (down_move > up_move)
        & (down_move > 0)
    )

    plus_dm.loc[
        plus_condition
    ] = up_move.loc[
        plus_condition
    ]

    minus_dm.loc[
        minus_condition
    ] = down_move.loc[
        minus_condition
    ]

    atr_wilder = (
        tr
        .ewm(
            alpha=1 / 14,
            adjust=False,
            min_periods=14
        )
        .mean()
    )

    plus_di = (
        100
        * plus_dm
        .ewm(
            alpha=1 / 14,
            adjust=False,
            min_periods=14
        )
        .mean()
        / atr_wilder.replace(
            0,
            pd.NA
        )
    )

    minus_di = (
        100
        * minus_dm
        .ewm(
            alpha=1 / 14,
            adjust=False,
            min_periods=14
        )
        .mean()
        / atr_wilder.replace(
            0,
            pd.NA
        )
    )

    dx = (
        100
        * (
            plus_di
            - minus_di
        ).abs()
        / (
            plus_di
            + minus_di
        ).replace(
            0,
            pd.NA
        )
    )

    result["adx14"] = (
        dx
        .ewm(
            alpha=1 / 14,
            adjust=False,
            min_periods=14
        )
        .mean()
    )

    return result


# ==========================================
# FILTRE CONTEXTUEL CONTRIBUTIF
# ==========================================

def evaluate_market_filter(
    df
):
    """
    Filtre souple.

    Les indicateurs ne sont pas des veto individuels.
    Le contexte doit simplement atteindre un minimum
    de cohérence avant qu'un signal soit créé.
    """

    if (
        df.empty
        or len(df) < 50
    ):

        return None

    candle = df.iloc[-2]

    ema20 = candle.get("ema20")
    ema50 = candle.get("ema50")
    rsi14 = candle.get("rsi14")
    atr14 = candle.get("atr14")
    adx14 = candle.get("adx14")

    values = [
        ema20,
        ema50,
        rsi14,
        atr14,
        adx14
    ]

    if any(
        pd.isna(value)
        for value in values
    ):

        return None

    recent_atr = (
        df["atr14"]
        .dropna()
        .tail(30)
    )

    if recent_atr.empty:
        return None

    ema20 = float(ema20)
    ema50 = float(ema50)
    rsi14 = float(rsi14)
    atr14 = float(atr14)
    adx14 = float(adx14)

    atr_reference = float(
        recent_atr.median()
    )

    buy_score = 0
    sell_score = 0

    buy_reasons = []
    sell_reasons = []

    if ema20 > ema50:

        buy_score += 1

        buy_reasons.append(
            "EMA20 > EMA50"
        )

    elif ema20 < ema50:

        sell_score += 1

        sell_reasons.append(
            "EMA20 < EMA50"
        )

    if 50 <= rsi14 <= 70:

        buy_score += 1

        buy_reasons.append(
            "RSI14 favorable BUY"
        )

    elif 30 <= rsi14 < 50:

        sell_score += 1

        sell_reasons.append(
            "RSI14 favorable SELL"
        )

    if adx14 >= 20:

        buy_score += 1
        sell_score += 1

        buy_reasons.append(
            "ADX14 >= 20"
        )

        sell_reasons.append(
            "ADX14 >= 20"
        )

    if (
        atr_reference > 0
        and atr14 >= (
            atr_reference * 0.80
        )
    ):

        buy_score += 1
        sell_score += 1

        buy_reasons.append(
            "ATR14 actif"
        )

        sell_reasons.append(
            "ATR14 actif"
        )

    return {
        "ema20": ema20,
        "ema50": ema50,
        "rsi14": rsi14,
        "atr14": atr14,
        "adx14": adx14,
        "atr_reference": atr_reference,
        "buy_score": min(
            buy_score,
            4
        ),
        "sell_score": min(
            sell_score,
            4
        ),
        "buy_reasons": buy_reasons,
        "sell_reasons": sell_reasons
    }


# ==========================================
# STRATÉGIE SMC + PRICE ACTION MULTI-TIMEFRAME
# ==========================================

def utc_datetime():
    return datetime.now(timezone.utc)


def strategy_timestamp(value=None):
    if value is None:
        value = utc_datetime()
    return value.strftime("%Y-%m-%d %H:%M:%S")


def detect_pivots(
    df,
    left=2,
    right=2
):
    """
    Détecte les pivots confirmés sans utiliser les bougies
    situées après le pivot pour prendre une décision prématurée.

    Un pivot high est le plus haut de sa fenêtre.
    Un pivot low est le plus bas de sa fenêtre.
    """

    if df is None or df.empty:
        return []

    if len(df) < left + right + 3:
        return []

    highs = pd.to_numeric(df["high"], errors="coerce")
    lows = pd.to_numeric(df["low"], errors="coerce")

    pivots = []

    for i in range(left, len(df) - right):
        high_window = highs.iloc[i - left:i + right + 1]
        low_window = lows.iloc[i - left:i + right + 1]

        high_value = highs.iloc[i]
        low_value = lows.iloc[i]

        if pd.isna(high_value) or pd.isna(low_value):
            continue

        if high_value == high_window.max():
            pivots.append({
                "type": "HIGH",
                "index": i,
                "price": float(high_value),
                "timestamp": str(df["timestamp"].iloc[i])
            })

        if low_value == low_window.min():
            pivots.append({
                "type": "LOW",
                "index": i,
                "price": float(low_value),
                "timestamp": str(df["timestamp"].iloc[i])
            })

    return sorted(
        pivots,
        key=lambda item: item["index"]
    )


def _last_pivot_before(
    pivots,
    pivot_type,
    before_index=None
):
    candidates = [
        pivot
        for pivot in pivots
        if pivot["type"] == pivot_type
        and (
            before_index is None
            or pivot["index"] < before_index
        )
    ]

    return candidates[-1] if candidates else None


def check_m15_structure(
    df
):
    """
    Détermine le dernier BOS M15 et la zone macro à surveiller.

    HAUSSIER :
        clôture M15 au-dessus du dernier sommet pivot.
        trigger = dernier creux pivot avant le BOS.

    BAISSIER :
        clôture M15 sous le dernier creux pivot.
        trigger = dernier sommet pivot avant le BOS.
    """

    if df is None or len(df) < 20:
        return None

    pivots = detect_pivots(
        df,
        M15_PIVOT_LEFT,
        M15_PIVOT_RIGHT
    )

    if not pivots:
        return None

    closed_index = len(df) - 2
    closed_candle = df.iloc[closed_index]
    close_price = float(closed_candle["close"])

    highs = [
        pivot
        for pivot in pivots
        if pivot["type"] == "HIGH"
        and pivot["index"] < closed_index
    ]

    lows = [
        pivot
        for pivot in pivots
        if pivot["type"] == "LOW"
        and pivot["index"] < closed_index
    ]

    if not highs or not lows:
        return None

    last_high = highs[-1]
    last_low = lows[-1]

    bullish_break = close_price > last_high["price"]
    bearish_break = close_price < last_low["price"]

    if bullish_break:
        macro_low = _last_pivot_before(
            pivots,
            "LOW",
            last_high["index"]
        )

        if macro_low is None:
            macro_low = last_low

        return {
            "direction": "HAUSSIER",
            "bos_price": last_high["price"],
            "bos_timestamp": last_high["timestamp"],
            "trigger_price": float(macro_low["price"]),
            "trigger_type": "SUPPORT",
            "trigger_timestamp": macro_low["timestamp"],
            "closed_timestamp": str(
                closed_candle["timestamp"]
            )
        }

    if bearish_break:
        macro_high = _last_pivot_before(
            pivots,
            "HIGH",
            last_low["index"]
        )

        if macro_high is None:
            macro_high = last_high

        return {
            "direction": "BAISSIER",
            "bos_price": last_low["price"],
            "bos_timestamp": last_low["timestamp"],
            "trigger_price": float(macro_high["price"]),
            "trigger_type": "RESISTANCE",
            "trigger_timestamp": macro_high["timestamp"],
            "closed_timestamp": str(
                closed_candle["timestamp"]
            )
        }

    return None


def _price_is_near_level(
    price,
    level,
    proximity_pct
):
    if price <= 0 or level <= 0:
        return False

    distance_pct = (
        abs(price - level)
        / level
        * 100.0
    )

    return distance_pct <= proximity_pct


def _opportunity_exists_for_m15_setup(
    opportunities,
    symbol,
    direction,
    trigger_price
):
    for opportunity in opportunities.values():
        if (
            opportunity.get("symbol") == symbol
            and opportunity.get("direction") == direction
            and opportunity.get("status") in {
                "WAITING_M5_LIQUIDITY",
                "WAITING_M1_CHOCH",
                "WAITING_M1_BOS"
            }
        ):
            try:
                old_trigger = float(
                    opportunity.get("trigger_price")
                )
                if (
                    abs(old_trigger - trigger_price)
                    <= max(
                        abs(trigger_price) * 0.00001,
                        1e-12
                    )
                ):
                    return True
            except (TypeError, ValueError):
                continue

    return False


def create_pending_opportunity(
    symbol,
    direction,
    trigger_price,
    m15_structure
):
    """
    Crée exactement une opportunité M15 indépendante par actif/setup.
    """

    candidate_id = (
        f"OPP_{direction}_{symbol}_"
        f"{int(time.time() * 1000)}"
    )

    opportunity = {
        "candidate_id": candidate_id,
        "symbol": symbol,
        "direction": direction,
        "trigger_price": float(trigger_price),
        "detected_at": strategy_timestamp(),
        "status": "WAITING_M5_LIQUIDITY",
        "m15_bos_price": float(
            m15_structure["bos_price"]
        ),
        "m15_bos_timestamp": str(
            m15_structure["bos_timestamp"]
        ),
        "trigger_type": m15_structure["trigger_type"],
        "trigger_timestamp": str(
            m15_structure["trigger_timestamp"]
        ),
        "m15_closed_timestamp": str(
            m15_structure["closed_timestamp"]
        ),
        "stage_started_at": strategy_timestamp(),
        "last_processed_m5_timestamp": None,
        "last_processed_m1_timestamp": None,
        "m1_choch_timestamp": None,
        "m1_choch_level": None,
        "m1_sl": None,
        "m1_bos_timestamp": None,
        "m1_bos_level": None,
        "order_block": None,
        "created_at": strategy_timestamp()
    }

    with JSON_LOCK:
        opportunities = load_json(
            OPPORTUNITIES_FILE
        )

        if _opportunity_exists_for_m15_setup(
            opportunities,
            symbol,
            direction,
            trigger_price
        ):
            return None

        opportunities[candidate_id] = opportunity

        save_json(
            OPPORTUNITIES_FILE,
            opportunities
        )

    logging.info(
        f"[M15] {symbol} {direction} | "
        f"BOS={m15_structure['bos_price']:.8f} | "
        f"Trigger={trigger_price:.8f} | "
        f"status=WAITING_M5_LIQUIDITY"
    )

    return candidate_id


def scan_market_m15(
    symbol
):
    """
    Analyse M15 indépendante pour un seul symbole.
    """

    try:
        df_m15 = fetch_biquote_ohlcv(
            symbol,
            timeframe="15m",
            count=200
        )

        if df_m15.empty or len(df_m15) < 30:
            logging.warning(
                f"[M15] Données insuffisantes pour {symbol}."
            )
            return

        structure = check_m15_structure(
            df_m15
        )

        if structure is None:
            logging.info(
                f"[M15] Aucun BOS exploitable pour {symbol}."
            )
            return

        current_price = float(
            df_m15["close"].iloc[-1]
        )

        trigger_price = float(
            structure["trigger_price"]
        )

        if not _price_is_near_level(
            current_price,
            trigger_price,
            M15_TRIGGER_PROXIMITY_PCT
        ):
            return

        create_pending_opportunity(
            symbol=symbol,
            direction=structure["direction"],
            trigger_price=trigger_price,
            m15_structure=structure
        )

    except Exception as e:
        logging.exception(
            f"[M15] Erreur scan {symbol}: {e}"
        )


def _last_closed_timestamp(
    df
):
    if df is None or len(df) < 2:
        return None

    return str(
        df["timestamp"].iloc[-2]
    )


def _get_newest_closed_candle(
    df,
    last_timestamp
):
    if df is None or len(df) < 3:
        return None

    closed_df = df.iloc[:-1].copy()

    if last_timestamp is None:
        return closed_df.iloc[-1]

    matches = closed_df[
        closed_df["timestamp"].astype(str)
        != str(last_timestamp)
    ]

    if matches.empty:
        return None

    return matches.iloc[-1]


def check_m5_liquidity(
    df,
    opp
):
    """
    Validation du Stop Hunt M5 sur la bougie M5 clôturée.

    HAUSSIER :
        low < trigger puis close > trigger.

    BAISSIER :
        high > trigger puis close < trigger.
    """

    if df is None or len(df) < 5:
        return False, None

    trigger = float(
        opp["trigger_price"]
    )
    direction = opp["direction"]

    last_closed = df.iloc[-2]

    candle_timestamp = str(
        last_closed["timestamp"]
    )

    if (
        opp.get("last_processed_m5_timestamp")
        == candle_timestamp
    ):
        return False, None

    high = float(last_closed["high"])
    low = float(last_closed["low"])
    close = float(last_closed["close"])

    if direction == "HAUSSIER":
        confirmed = (
            low < trigger
            and close > trigger
        )
    else:
        confirmed = (
            high > trigger
            and close < trigger
        )

    return confirmed, candle_timestamp


def _find_last_opposite_pivot(
    pivots,
    direction,
    before_index
):
    wanted = (
        "LOW"
        if direction == "HAUSSIER"
        else "HIGH"
    )

    candidates = [
        p
        for p in pivots
        if p["type"] == wanted
        and p["index"] < before_index
    ]

    return candidates[-1] if candidates else None


def check_m1_choch(
    df,
    opp
):
    """
    Détection du CHoCH M1 après la liquidité M5.

    Le niveau cassé est le dernier pivot local opposé.
    Le SL est le dernier pivot local dans le sens du retracement.
    """

    if df is None or len(df) < 10:
        return None

    pivots = detect_pivots(
        df,
        M1_PIVOT_LEFT,
        M1_PIVOT_RIGHT
    )

    if not pivots:
        return None

    closed_index = len(df) - 2
    candle = df.iloc[closed_index]
    close = float(candle["close"])

    direction = opp["direction"]

    m5_timestamp = opp.get("m5_liquidity_timestamp")

    if m5_timestamp:
        eligible_pivots = [
            p for p in pivots
            if str(p["timestamp"]) > str(m5_timestamp)
        ]
    else:
        eligible_pivots = pivots

    if direction == "HAUSSIER":
        candidates = [
            p for p in eligible_pivots
            if p["type"] == "HIGH"
            and p["index"] < closed_index
        ]
        if not candidates:
            return None

        broken = candidates[-1]

        if close <= broken["price"]:
            return None

        sl_pivot = _find_last_opposite_pivot(
            eligible_pivots,
            direction,
            closed_index
        )

    else:
        candidates = [
            p for p in eligible_pivots
            if p["type"] == "LOW"
            and p["index"] < closed_index
        ]
        if not candidates:
            return None

        broken = candidates[-1]

        if close >= broken["price"]:
            return None

        sl_pivot = _find_last_opposite_pivot(
            eligible_pivots,
            direction,
            closed_index
        )

    if sl_pivot is None:
        return None

    return {
        "timestamp": str(candle["timestamp"]),
        "broken_level": float(broken["price"]),
        "sl": float(sl_pivot["price"]),
        "sl_timestamp": str(sl_pivot["timestamp"]),
        "pivot_index": int(broken["index"])
    }


def _find_order_block(
    df,
    bos_index,
    direction
):
    """
    Order Block = dernière bougie opposée au mouvement
    immédiatement avant le déplacement ayant produit le BOS.
    """

    if bos_index <= 0:
        return None

    for i in range(
        bos_index - 1,
        max(-1, bos_index - 8),
        -1
    ):
        candle = df.iloc[i]
        open_price = float(candle["open"])
        close_price = float(candle["close"])

        if direction == "HAUSSIER":
            if close_price < open_price:
                return {
                    "index": int(i),
                    "timestamp": str(
                        candle["timestamp"]
                    ),
                    "open": open_price,
                    "high": float(candle["high"]),
                    "low": float(candle["low"]),
                    "close": close_price,
                    "type": "ORDER_BLOCK_HAUSSIER"
                }

        else:
            if close_price > open_price:
                return {
                    "index": int(i),
                    "timestamp": str(
                        candle["timestamp"]
                    ),
                    "open": open_price,
                    "high": float(candle["high"]),
                    "low": float(candle["low"]),
                    "close": close_price,
                    "type": "ORDER_BLOCK_BAISSIER"
                }

    return None


def check_m1_bos(
    df,
    opp
):
    """
    Après CHoCH, recherche un nouveau pivot puis sa cassure.
    """

    if df is None or len(df) < 12:
        return None

    pivots = detect_pivots(
        df,
        M1_PIVOT_LEFT,
        M1_PIVOT_RIGHT
    )

    if not pivots:
        return None

    choch_timestamp = str(
        opp.get("m1_choch_timestamp")
    )

    closed_df = df.iloc[:-1].copy()

    choch_positions = [
        i
        for i, value in enumerate(
            closed_df["timestamp"].astype(str)
        )
        if value == choch_timestamp
    ]

    if not choch_positions:
        return None

    choch_index = choch_positions[-1]
    closed_index = len(df) - 2
    direction = opp["direction"]

    if direction == "HAUSSIER":
        candidates = [
            p
            for p in pivots
            if p["type"] == "HIGH"
            and choch_index < p["index"] < closed_index
        ]

        if not candidates:
            return None

        target = candidates[-1]

        if float(df["close"].iloc[closed_index]) <= target["price"]:
            return None

    else:
        candidates = [
            p
            for p in pivots
            if p["type"] == "LOW"
            and choch_index < p["index"] < closed_index
        ]

        if not candidates:
            return None

        target = candidates[-1]

        if float(df["close"].iloc[closed_index]) >= target["price"]:
            return None

    order_block = _find_order_block(
        df,
        target["index"],
        direction
    )

    if order_block is None:
        return None

    entry = float(
        order_block["open"]
    )

    sl = float(
        opp["m1_sl"]
    )

    if direction == "HAUSSIER":
        if sl >= entry:
            return None
    else:
        if sl <= entry:
            return None

    return {
        "timestamp": str(
            df["timestamp"].iloc[closed_index]
        ),
        "bos_level": float(target["price"]),
        "bos_pivot_timestamp": str(
            target["timestamp"]
        ),
        "order_block": order_block,
        "entry_price": entry,
        "sl": sl
    }


def calculate_take_profits(
    direction: str,
    entry_price: float,
    stop_loss: float,
    df_m15: pd.DataFrame
) -> dict:
    direction = str(direction).strip().upper()

    if direction not in ("HAUSSIER", "BAISSIER"):
        raise ValueError("Direction invalide.")

    if df_m15 is None or df_m15.empty:
        raise ValueError("Données M15 insuffisantes.")

    if not {"high", "low"}.issubset(df_m15.columns):
        raise ValueError("Les colonnes 'high' et 'low' sont requises.")

    entry_price = float(entry_price)
    stop_loss = float(stop_loss)

    if not math.isfinite(entry_price) or not math.isfinite(stop_loss):
        raise ValueError("Prix invalides.")

    high = pd.to_numeric(df_m15["high"], errors="coerce").dropna()
    low = pd.to_numeric(df_m15["low"], errors="coerce").dropna()

    if high.empty or low.empty:
        raise ValueError("Données M15 high/low insuffisantes.")

    if direction == "HAUSSIER":
        if stop_loss >= entry_price:
            raise ValueError("SL invalide pour une position HAUSSIER.")

        risk = entry_price - stop_loss
        tp1 = entry_price + risk
        tp3 = float(high.max()) * (1.0 - 0.0002)
        tp2 = (tp1 + tp3) / 2.0
        reward = tp3 - entry_price
        rr_tp3 = reward / risk

    else:
        if stop_loss <= entry_price:
            raise ValueError("SL invalide pour une position BAISSIER.")

        risk = stop_loss - entry_price
        tp1 = entry_price - risk
        tp3 = float(low.min()) * (1.0 + 0.0002)
        tp2 = (tp1 + tp3) / 2.0
        reward = entry_price - tp3
        rr_tp3 = reward / risk

    if not all(math.isfinite(value) for value in (tp1, tp2, tp3, rr_tp3)):
        raise ValueError("Niveaux TP invalides.")

    if rr_tp3 < 3.0:
        raise ValueError(
            f"Trade annulé : RR TP3 = {rr_tp3:.2f}, inférieur au minimum 1:3."
        )

    return {
        "tp1": float(tp1),
        "tp2": float(tp2),
        "tp3": float(tp3),
        "rr_tp3": float(rr_tp3),
    }


def _build_active_trade(
    candidate_id,
    opp,
    execution
):
    symbol = opp["symbol"]
    direction = opp["direction"]

    entry = float(execution["entry_price"])
    sl = float(execution["sl"])

    df_m15 = fetch_biquote_ohlcv(
        symbol,
        timeframe="15m",
        count=200
    )

    if df_m15.empty or len(df_m15) < 30:
        logging.info(
            f"[M1] {symbol} {direction}: données M15 insuffisantes pour TP."
        )
        return None

    try:
        tp_levels = calculate_take_profits(
            direction=direction,
            entry_price=entry,
            stop_loss=sl,
            df_m15=df_m15
        )
    except ValueError as exc:
        logging.info(
            f"[M1] {symbol} {direction}: trade invalidé — {exc}"
        )
        return None

    if direction == "HAUSSIER":
        trade_direction = "BUY"
    else:
        trade_direction = "SELL"

    trade_id = (
        f"TRADE_{trade_direction}_{symbol}_"
        f"{int(time.time() * 1000)}"
    )

    order_block = execution["order_block"]

    return trade_id, {
        "symbol": symbol,
        "direction": trade_direction,
        "entry_price": entry,
        "initial_sl": sl,
        "current_sl": sl,
        "tp1": tp_levels["tp1"],
        "tp2": tp_levels["tp2"],
        "tp3": tp_levels["tp3"],
        "tp": tp_levels["tp3"],
        "rr_theoretical": tp_levels["rr_tp3"],
        "tp1_hit": False,
        "tp2_hit": False,
        "tp3_hit": False,
        "status": "PENDING_LIMIT",
        "execution_type": "SIMULATED_LIMIT",
        "order_block": order_block,
        "m15_trigger_price": float(opp["trigger_price"]),
        "m15_bos_price": float(opp["m15_bos_price"]),
        "m1_choch_level": opp.get("m1_choch_level"),
        "m1_bos_level": execution.get("bos_level"),
        "candidate_id": candidate_id,
        "created_at": strategy_timestamp()
    }


def execute_m1_order(
    candidate_id,
    opp,
    execution
):
    """
    Enregistre le limit simulé dans active_trades.json.
    L'alerte Telegram est envoyée uniquement après sauvegarde réussie.
    """

    result = _build_active_trade(
        candidate_id,
        opp,
        execution
    )

    if result is None:
        return False

    trade_id, trade = result

    with MARKET_EXECUTION_LOCK:
        with JSON_LOCK:
            active_trades = load_json(
                TRADES_FILE
            )

            for existing in active_trades.values():
                if (
                    existing.get("candidate_id")
                    == candidate_id
                ):
                    return False

            active_trades[trade_id] = trade
            save_json(
                TRADES_FILE,
                active_trades
            )

        ensure_trade_history_record(
            trade_id,
            trade
        )

    entry = float(trade["entry_price"])
    sl = float(trade["initial_sl"])
    tp1 = float(trade["tp1"])
    tp2 = float(trade["tp2"])
    tp3 = float(trade["tp3"])
    rr = float(trade["rr_theoretical"])

    direction_text = (
        "ACHAT"
        if trade["direction"] == "BUY"
        else "VENTE"
    )

    symbol = trade["symbol"]

    message = (
        f"{'🟢' if trade['direction'] == 'BUY' else '🔴'} "
        f"*{direction_text} — {symbol}*\n\n"
        f"🎯 Entrée : {format_telegram_price(symbol, entry)}\n"
        f"🛑 SL : {format_telegram_price(symbol, sl)}\n\n"
        f"TP1 : {format_telegram_price(symbol, tp1)}\n"
        f"TP2 : {format_telegram_price(symbol, tp2)}\n"
        f"TP3 : {format_telegram_price(symbol, tp3)}\n\n"
        f"⚖️ RR : 1:{rr:.2f}"
    )

    if send_telegram_message(message):
        logging.info(
            f"[EXECUTION] {trade_id} enregistré et "
            f"alerte Telegram envoyée."
        )
    else:
        logging.error(
            f"[EXECUTION] {trade_id} enregistré, "
            f"mais Telegram n'a pas confirmé l'envoi."
        )

    return True


def _stage_age_expired(
    opportunity,
    df_m1=None
):
    """
    Expiration basée sur le nombre de bougies M1 clôturées
    depuis le début de l'étape active.
    """

    if df_m1 is None or df_m1.empty:
        return False

    stage_started = opportunity.get(
        "stage_started_at"
    )

    if not stage_started:
        return False

    try:
        stage_dt = datetime.strptime(
            stage_started,
            "%Y-%m-%d %H:%M:%S"
        ).replace(tzinfo=timezone.utc)
    except Exception:
        return False

    now = utc_datetime()
    minutes = max(
        0,
        int(
            (now - stage_dt).total_seconds()
            / 60
        )
    )

    elapsed_m1_candles = minutes

    return (
        elapsed_m1_candles
        >= OPPORTUNITY_EXPIRY_CANDLES
    )


def _update_opportunity(
    candidate_id,
    updates
):
    with JSON_LOCK:
        opportunities = load_json(
            OPPORTUNITIES_FILE
        )

        opportunity = opportunities.get(
            candidate_id
        )

        if opportunity is None:
            return None

        opportunity.update(updates)
        opportunities[candidate_id] = opportunity
        save_json(
            OPPORTUNITIES_FILE,
            opportunities
        )

        return opportunity


def _delete_opportunity(
    candidate_id
):
    with JSON_LOCK:
        opportunities = load_json(
            OPPORTUNITIES_FILE
        )
        opportunities.pop(
            candidate_id,
            None
        )
        save_json(
            OPPORTUNITIES_FILE,
            opportunities
        )


def _process_pending_opportunity(
    candidate_id,
    opportunity
):
    symbol = opportunity.get("symbol")

    if symbol not in SYMBOLS:
        _delete_opportunity(candidate_id)
        return

    status = opportunity.get("status")

    try:
        if status == "WAITING_M5_LIQUIDITY":
            df_m5 = fetch_biquote_ohlcv(
                symbol,
                timeframe="5m",
                count=100
            )

            if df_m5.empty:
                return

            if _stage_age_expired(
                opportunity,
                df_m5
            ):
                logging.info(
                    f"[EXPIRATION] {candidate_id} expiré en M5."
                )
                _delete_opportunity(candidate_id)
                return

            confirmed, candle_timestamp = (
                check_m5_liquidity(
                    df_m5,
                    opportunity
                )
            )

            if candle_timestamp is not None:
                _update_opportunity(
                    candidate_id,
                    {
                        "last_processed_m5_timestamp":
                            candle_timestamp
                    }
                )

            if not confirmed:
                return

            updated = _update_opportunity(
                candidate_id,
                {
                    "status": "WAITING_M1_CHOCH",
                    "stage_started_at":
                        strategy_timestamp(),
                    "m5_liquidity_timestamp":
                        candle_timestamp
                }
            )

            logging.info(
                f"[M5] {symbol} | "
                f"liquidité validée | "
                f"status=WAITING_M1_CHOCH"
            )

            opportunity = updated or opportunity
            status = "WAITING_M1_CHOCH"

        if status == "WAITING_M1_CHOCH":
            df_m1 = fetch_biquote_ohlcv(
                symbol,
                timeframe="1m",
                count=150
            )

            if df_m1.empty:
                return

            if _stage_age_expired(
                opportunity,
                df_m1
            ):
                logging.info(
                    f"[EXPIRATION] {candidate_id} expiré en CHoCH."
                )
                _delete_opportunity(candidate_id)
                return

            choch = check_m1_choch(
                df_m1,
                opportunity
            )

            last_m1 = _last_closed_timestamp(
                df_m1
            )

            if choch is None:
                if last_m1 is not None:
                    _update_opportunity(
                        candidate_id,
                        {
                            "last_processed_m1_timestamp":
                                last_m1
                        }
                    )
                return

            updated = _update_opportunity(
                candidate_id,
                {
                    "status": "WAITING_M1_BOS",
                    "stage_started_at":
                        strategy_timestamp(),
                    "m1_choch_timestamp":
                        choch["timestamp"],
                    "m1_choch_level":
                        choch["broken_level"],
                    "m1_sl":
                        choch["sl"],
                    "m1_sl_timestamp":
                        choch["sl_timestamp"]
                }
            )

            logging.info(
                f"[M1-CHOCH] {symbol} | "
                f"{opportunity['direction']} | "
                f"niveau={choch['broken_level']:.8f} | "
                f"SL={choch['sl']:.8f} | "
                f"status=WAITING_M1_BOS"
            )

            opportunity = updated or opportunity
            status = "WAITING_M1_BOS"

        if status == "WAITING_M1_BOS":
            df_m1 = fetch_biquote_ohlcv(
                symbol,
                timeframe="1m",
                count=200
            )

            if df_m1.empty:
                return

            if _stage_age_expired(
                opportunity,
                df_m1
            ):
                logging.info(
                    f"[EXPIRATION] {candidate_id} expiré en BOS."
                )
                _delete_opportunity(candidate_id)
                return

            bos = check_m1_bos(
                df_m1,
                opportunity
            )

            last_m1 = _last_closed_timestamp(
                df_m1
            )

            if bos is None:
                if last_m1 is not None:
                    _update_opportunity(
                        candidate_id,
                        {
                            "last_processed_m1_timestamp":
                                last_m1
                        }
                    )
                return

            updated = _update_opportunity(
                candidate_id,
                {
                    "m1_bos_timestamp":
                        bos["timestamp"],
                    "m1_bos_level":
                        bos["bos_level"],
                    "order_block":
                        bos["order_block"]
                }
            )

            if updated is None:
                return

            logging.info(
                f"[M1-BOS] {symbol} | "
                f"BOS={bos['bos_level']:.8f} | "
                f"OB={bos['order_block']['open']:.8f}"
            )

            if execute_m1_order(
                candidate_id,
                updated,
                bos
            ):
                _delete_opportunity(
                    candidate_id
                )

    except Exception as e:
        logging.exception(
            f"[SMC] Erreur opportunité "
            f"{candidate_id}/{symbol}: {e}"
        )


def scan_pending_opportunities_m5():
    """
    Traite chaque opportunité indépendamment.
    """

    with JSON_LOCK:
        opportunities = load_json(
            OPPORTUNITIES_FILE
        )

    if not opportunities:
        return

    snapshot = [
        (candidate_id, opportunity.copy())
        for candidate_id, opportunity
        in opportunities.items()
    ]

    workers = max(
        1,
        min(
            SCAN_WORKERS,
            len(snapshot)
        )
    )

    with ThreadPoolExecutor(
        max_workers=workers,
        thread_name_prefix="smc-pending"
    ) as executor:
        futures = [
            executor.submit(
                _process_pending_opportunity,
                candidate_id,
                opportunity
            )
            for candidate_id, opportunity in snapshot
        ]

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.exception(
                    f"[SMC] Erreur worker pending: {e}"
                )


def scan_all_symbols_m15():
    """
    Les quatre marchés sont analysés en parallèle.
    Une panne d'un actif n'arrête jamais les trois autres.
    """

    with ThreadPoolExecutor(
        max_workers=SCAN_WORKERS,
        thread_name_prefix="m15-symbol"
    ) as executor:
        futures = {
            executor.submit(
                scan_market_m15,
                symbol
            ): symbol
            for symbol in SYMBOLS
        }

        for future in as_completed(futures):
            symbol = futures[future]
            try:
                future.result()
            except Exception as e:
                logging.exception(
                    f"[M15] Worker {symbol} en erreur: {e}"
                )


# ==========================================
# SCHEDULER MULTI-TIMEFRAME
# ==========================================

def main_scheduler():
    logging.info(
        "Thread scheduler SMC multi-timeframe démarré."
    )

    last_m15_slot = None
    last_m5_slot = None

    while True:
        try:
            now = datetime.now(timezone.utc)

            m15_slot = now.replace(
                minute=(now.minute // 15) * 15,
                second=0,
                microsecond=0
            )

            if (
                now.second >= 5
                and m15_slot != last_m15_slot
            ):
                logging.info(
                    "[SCHEDULER] Scan M15 parallèle des 4 actifs."
                )

                scan_all_symbols_m15()

                last_m15_slot = m15_slot

            m5_slot = now.replace(
                minute=(now.minute // 5) * 5,
                second=0,
                microsecond=0
            )

            if (
                now.second >= 5
                and m5_slot != last_m5_slot
            ):
                logging.info(
                    "[SCHEDULER] Scan M5/M1 parallèle des opportunités."
                )

                scan_pending_opportunities_m5()

                last_m5_slot = m5_slot

            time.sleep(1)

        except Exception as e:
            logging.exception(
                f"Erreur scheduler SMC multi-timeframe: {e}"
            )
            time.sleep(5)

# ==========================================
# SUIVI DES TRADES
# ==========================================

def track_active_trades():
    logging.info(
        "Thread de suivi des trades démarré."
    )

    while True:
        try:
            generate_weekly_report()

            active_trades = load_json(
                TRADES_FILE
            )

            if not active_trades:
                time.sleep(10)
                continue

            prices = {}
            symbols_in_trades = {
                trade.get("symbol")
                for trade in active_trades.values()
                if trade.get("symbol")
            }

            for symbol in symbols_in_trades:
                prices[symbol] = fetch_biquote_live_price(
                    symbol
                )

            trades_to_delete = []

            for trade_id, trade in list(
                active_trades.items()
            ):
                try:
                    symbol = trade.get("symbol")
                    if not symbol:
                        continue

                    current_price = prices.get(symbol)
                    if current_price is None:
                        continue

                    direction = trade["direction"]
                    entry = float(trade["entry_price"])
                    current_sl = float(trade["current_sl"])
                    tp1 = float(trade.get("tp1", trade.get("tp")))
                    tp2 = float(trade.get("tp2", tp1))
                    tp3 = float(trade.get("tp3", trade.get("tp", tp2)))
                    status = trade.get("status", "ACTIVE")

                    tp1_hit = bool(trade.get("tp1_hit", False))
                    tp2_hit = bool(trade.get("tp2_hit", False))
                    tp3_hit = bool(trade.get("tp3_hit", False))

                    if status == "PENDING_LIMIT":
                        filled = (
                            current_price <= entry
                            if direction == "BUY"
                            else current_price >= entry
                        )

                        if filled:
                            trade["status"] = "ACTIVE"
                            record_trade_event(
                                trade_id,
                                "LIMIT_FILLED",
                                current_price
                            )
                            send_telegram_message(
                                f"🟢 *Ordre limite exécuté* — "
                                f"{symbol}\n"
                                f"Entrée : `{format_telegram_price(symbol, entry)}`"
                            )

                        continue

                    if direction == "BUY":
                        if current_price <= current_sl:
                            send_telegram_message(
                                f"❌ *SL Touché* sur {symbol} "
                                f"à `{current_price:.8f}`.\n"
                                f"Trade clos."
                            )

                            close_trade_in_history(
                                trade_id,
                                "SL",
                                current_price
                            )
                            trades_to_delete.append(
                                trade_id
                            )

                        else:
                            if not tp1_hit and current_price >= tp1:
                                trade["tp1_hit"] = True
                                record_trade_event(
                                    trade_id,
                                    "TP1_HIT",
                                    current_price
                                )
                                send_telegram_message(
                                    f"🎯 *TP1 atteint* — {symbol}\n"
                                    f"Prix : `{format_telegram_price(symbol, current_price)}`"
                                )

                            if not tp2_hit and current_price >= tp2:
                                trade["tp2_hit"] = True
                                record_trade_event(
                                    trade_id,
                                    "TP2_HIT",
                                    current_price
                                )
                                send_telegram_message(
                                    f"🎯 *TP2 atteint* — {symbol}\n"
                                    f"Prix : `{format_telegram_price(symbol, current_price)}`"
                                )

                            if not tp3_hit and current_price >= tp3:
                                trade["tp3_hit"] = True
                                record_trade_event(
                                    trade_id,
                                    "TP3_HIT",
                                    current_price
                                )
                                close_trade_in_history(
                                    trade_id,
                                    "TP3",
                                    current_price
                                )
                                send_telegram_message(
                                    f"🏆 *TP3 atteint* — {symbol}\n"
                                    f"Prix : `{current_price:.8f}`.\n"
                                    f"Trade terminé."
                                )
                                trades_to_delete.append(trade_id)

                    else:
                        if current_price >= current_sl:
                            send_telegram_message(
                                f"❌ *SL Touché* sur {symbol} "
                                f"à `{current_price:.8f}`.\n"
                                f"Trade clos."
                            )

                            close_trade_in_history(
                                trade_id,
                                "SL",
                                current_price
                            )
                            trades_to_delete.append(
                                trade_id
                            )

                        else:
                            if not tp1_hit and current_price <= tp1:
                                trade["tp1_hit"] = True
                                record_trade_event(
                                    trade_id,
                                    "TP1_HIT",
                                    current_price
                                )
                                send_telegram_message(
                                    f"🎯 *TP1 atteint* — {symbol}\n"
                                    f"Prix : `{format_telegram_price(symbol, current_price)}`"
                                )

                            if not tp2_hit and current_price <= tp2:
                                trade["tp2_hit"] = True
                                record_trade_event(
                                    trade_id,
                                    "TP2_HIT",
                                    current_price
                                )
                                send_telegram_message(
                                    f"🎯 *TP2 atteint* — {symbol}\n"
                                    f"Prix : `{format_telegram_price(symbol, current_price)}`"
                                )

                            if not tp3_hit and current_price <= tp3:
                                trade["tp3_hit"] = True
                                record_trade_event(
                                    trade_id,
                                    "TP3_HIT",
                                    current_price
                                )
                                close_trade_in_history(
                                    trade_id,
                                    "TP3",
                                    current_price
                                )
                                send_telegram_message(
                                    f"🏆 *TP3 atteint* — {symbol}\n"
                                    f"Prix : `{current_price:.8f}`.\n"
                                    f"Trade terminé."
                                )
                                trades_to_delete.append(trade_id)

                except Exception as e:
                    logging.error(
                        f"Erreur traitement trade "
                        f"{trade_id}: {e}"
                    )

            if trades_to_delete:
                with JSON_LOCK:
                    active_trades = load_json(
                        TRADES_FILE
                    )
                    for trade_id in trades_to_delete:
                        active_trades.pop(
                            trade_id,
                            None
                        )
                    save_json(
                        TRADES_FILE,
                        active_trades
                    )
            else:
                save_json(
                    TRADES_FILE,
                    active_trades
                )

        except Exception as e:
            logging.exception(
                f"Erreur dans le suivi temps réel : {e}"
            )

        time.sleep(10)


# ==========================================
# PLANIFICATEUR MULTI-TIMEFRAME
# ==========================================
# FLASK
# ==========================================

app = Flask(
    __name__
)


# ==========================================
# DÉMARRAGE DES THREADS
# ==========================================

def start_trading_threads():

    global _threads_started

    with _threads_lock:

        if _threads_started:

            logging.info(
                "Threads de trading "
                "déjà démarrés."
            )

            return

        _threads_started = True

        logging.info(
            "DÉMARRAGE DES THREADS "
            "DE TRADING EN ARRIÈRE-PLAN..."
        )

        t_track = threading.Thread(
            target=track_active_trades,
            name="trade-tracker",
            daemon=True
        )

        t_sched = threading.Thread(
            target=main_scheduler,
            name="market-scheduler",
            daemon=True
        )

        t_telegram = threading.Thread(
            target=telegram_polling_loop,
            name="telegram-controller",
            daemon=True
        )

        t_track.start()
        t_sched.start()
        t_telegram.start()

        logging.info(
            "Threads de trading et "
            "Telegram démarrés."
        )


# ==========================================
# ROUTES WEB
# ==========================================

@app.route("/")
@app.route("/health")
def health_check():

    return {
        "status": "healthy",
        "provider": "biquote",
        "symbols": SYMBOLS,
        "telegram_configured": (
            telegram_is_configured()
        ),
        "indicators": [
            "EMA20",
            "EMA50",
            "RSI14",
            "ATR14",
            "ADX14"
        ],
        "filter": "contextual_contributive",
        "timestamp": (
            datetime.utcnow()
            .isoformat()
        )
    }, 200


# ==========================================
# DÉMARRAGE AUTOMATIQUE AVEC GUNICORN
# ==========================================

start_trading_threads()


# ==========================================
# LANCEMENT LOCAL
# ==========================================

if __name__ == "__main__":

    port = int(
        os.environ.get(
            "PORT",
            "8080"
        )
    )

    app.run(
        host="0.0.0.0",
        port=port
    )