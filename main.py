import os
import asyncio
import io
import struct
import zlib
import time
import json
import logging
import math
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import requests
from flask import Flask, Response, request


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
SL_GUARD_FILE = "sl_guard.json"

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
                    "text": "📈 Graphiques marché",
                    "callback_data": "market_charts"
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
    text,
    reply_markup=None
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
        "reply_markup": (
            reply_markup
            if reply_markup is not None
            else telegram_menu_keyboard()
        )
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
        "Cartographie macro : M15 (300 bougies)",
        "Liquidité / Stop Hunt : M5",
        "Déclencheur CHoCH + BOS : M1 (chaque minute)",
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
            _register_pair_sl_result(trade.get("symbol", ""), result)

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

        elif data == "market_charts":

            text = "📈 *GRAPHIQUES NOVA*\n\nChoisissez la paire à visualiser :"
            chart_keyboard = {
                "inline_keyboard": [
                    [
                        {"text": "₿ BTCUSD", "callback_data": "chart_pair_BTCUSD"},
                        {"text": "🪙 XAUUSD", "callback_data": "chart_pair_XAUUSD"}
                    ],
                    [
                        {"text": "💶 EURUSD", "callback_data": "chart_pair_EURUSD"},
                        {"text": "💷 GBPUSD", "callback_data": "chart_pair_GBPUSD"}
                    ],
                    [
                        {"text": "⬅️ Retour", "callback_data": "refresh"}
                    ]
                ]
            }
            if message_id is not None:
                telegram_edit_message(chat_id, message_id, text, reply_markup=chart_keyboard)
            return

        elif data.startswith("chart_pair_"):

            symbol = data.replace("chart_pair_", "", 1).upper()
            if symbol not in SYMBOLS:
                return
            text = f"📈 *{symbol}*\n\nChoisissez le timeframe :"
            tf_keyboard = {
                "inline_keyboard": [
                    [
                        {"text": "M15", "callback_data": f"chart_tf_{symbol}_15m"},
                        {"text": "M5", "callback_data": f"chart_tf_{symbol}_5m"},
                        {"text": "M1", "callback_data": f"chart_tf_{symbol}_1m"}
                    ],
                    [{"text": "⬅️ Retour", "callback_data": "market_charts"}]
                ]
            }
            if message_id is not None:
                telegram_edit_message(chat_id, message_id, text, reply_markup=tf_keyboard)
            return

        elif data.startswith("chart_tf_"):

            parts = data.split("_")
            if len(parts) != 4:
                return
            symbol = parts[2].upper()
            timeframe = parts[3]
            if symbol not in SYMBOLS or timeframe not in {"15m", "5m", "1m"}:
                return
            if message_id is not None:
                telegram_edit_message(
                    chat_id,
                    message_id,
                    (
                        f"⏳ Génération du graphique *{symbol} "
                        f"{timeframe.upper()}*...\n\n"
                        "Données marché + zones SMC détectées par NOVA."
                    ),
                    reply_markup=telegram_menu_keyboard()
                )

            chart_png = _build_owner_chart_png(symbol, timeframe)

            if not chart_png:
                send_telegram_owner_message(
                    (
                        f"⚠️ *Graphique indisponible — {symbol} "
                        f"{timeframe.upper()}*\n\n"
                        "NOVA n'a pas obtenu de données de marché exploitables "
                        "pour ce graphique."
                    ),
                    reply_markup=telegram_menu_keyboard()
                )
                return

            caption = (
                f"📈 *NOVA — {symbol} {timeframe.upper()}*\n"
                f"Bias M15 : {(_owner_chart_data(symbol, '15m') or {}).get('overlays', {}).get('bias') or 'N/A'}\n"
                "Graphique généré directement par NOVA.\n"
                "Les zones affichées correspondent aux éléments SMC détectés."
            )

            if not send_telegram_owner_photo(chart_png, caption):
                send_telegram_owner_message(
                    "⚠️ Impossible d'envoyer le graphique directement sur Telegram.",
                    reply_markup=telegram_menu_keyboard()
                )
            return

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


def is_trading_session(now=None):
    return True


def is_pair_locked_24h(symbol):
    symbol=str(symbol).upper()
    try:
        now=datetime.now(timezone.utc)
        with JSON_LOCK:
            state=load_json(SL_GUARD_FILE); item=state.get(symbol) if isinstance(state,dict) else None
            if not isinstance(item,dict) or not item.get("locked_until"):return False
            if datetime.fromisoformat(str(item["locked_until"]).replace("Z","+00:00"))>now:return True
            state.pop(symbol,None); save_json(SL_GUARD_FILE,state); return False
    except Exception as exc:
        logging.exception(f"[SL GUARD] Erreur lecture {symbol}: {exc}"); return False


def _register_pair_sl_result(symbol,result):
    symbol=str(symbol).upper(); result=str(result).upper()
    try:
        now=datetime.now(timezone.utc)
        with JSON_LOCK:
            state=load_json(SL_GUARD_FILE); state=state if isinstance(state,dict) else {}; item=state.get(symbol,{})
            if result=="SL":
                n=int(item.get("consecutive_sl",0))+1; state[symbol]={"consecutive_sl":n,"last_result":"SL","last_sl_at":now.isoformat(),"locked_until":(now+timedelta(hours=24)).isoformat() if n>=2 else None}
                if n>=2:logging.warning(f"[SL GUARD] {symbol}: 2 SL successifs -> blocage 24h.")
            else:state.pop(symbol,None)
            save_json(SL_GUARD_FILE,state)
    except Exception as exc:logging.exception(f"[SL GUARD] Erreur mise à jour {symbol}: {exc}")



def pip_size(symbol):
    symbol = str(symbol).upper()
    if symbol in ("EURUSD", "GBPUSD"):
        return 0.0001
    if symbol == "XAUUSD":
        return 0.01
    return 0.01


def calculate_pip_metrics(symbol, entry, stop_loss, tp3):
    try:
        size = pip_size(symbol)
        risk_pips = abs(float(entry) - float(stop_loss)) / size
        reward_pips = abs(float(tp3) - float(entry)) / size
        rr = reward_pips / risk_pips if risk_pips > 0 else 0.0
        return float(risk_pips), float(reward_pips), float(rr)
    except Exception as exc:
        logging.exception(f"[PIPS] Erreur calcul pips {symbol}: {exc}")
        return 0.0, 0.0, 0.0

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
# MATRICE DE SECOURS MULTI-SOURCE
# ==========================================

DATA_SOURCE_LOCK = threading.RLock()
MARKET_DATA_CACHE_LOCK = threading.RLock()
MARKET_DATA_CACHE = {}
TIINGO_API_TOKEN = (os.environ.get("TIINGO_API_TOKEN", "") or os.environ.get("TIINGO_TOKEN", "")).strip()
_TIMEFRAME_SECONDS = {"1m":60,"5m":300,"15m":900}
_YAHOO_SYMBOLS = {"BTCUSD":"BTC-USD","XAUUSD":"GC=F","EURUSD":"EURUSD=X","GBPUSD":"GBPUSD=X"}
_TIIINGO_SYMBOLS = {"XAUUSD":"XAUUSD","EURUSD":"EURUSD","GBPUSD":"GBPUSD"}

def _empty_market_dataframe():
    return pd.DataFrame(columns=["open","high","low","close","volume"],index=pd.DatetimeIndex([],tz="UTC",name="timestamp"))

def _normalize_market_dataframe(rows):
    if rows is None:return None
    try:
        df=rows.copy() if isinstance(rows,pd.DataFrame) else pd.DataFrame(rows)
        if df.empty:return None
        df.columns=[str(c).strip().lower() for c in df.columns]
        df=df.rename(columns={"datetime":"timestamp","date":"timestamp","time":"timestamp","open_time":"timestamp","opentime":"timestamp","adj close":"close"})
        if "timestamp" not in df.columns:
            if isinstance(df.index,pd.DatetimeIndex):df["timestamp"]=df.index
            else:return None
        if not all(c in df.columns for c in ("open","high","low","close")):return None
        if "volume" not in df.columns:df["volume"]=0.0
        df["timestamp"]=pd.to_datetime(df["timestamp"],errors="coerce",utc=True)
        for c in ("open","high","low","close","volume"):df[c]=pd.to_numeric(df[c],errors="coerce")
        df=df.replace([np.inf,-np.inf],np.nan).dropna(subset=["timestamp","open","high","low","close"]).sort_values("timestamp").drop_duplicates("timestamp",keep="last")
        if df.empty:return None
        valid=(df["high"]>=df[["open","close"]].max(axis=1))&(df["low"]<=df[["open","close"]].min(axis=1))&(df["high"]>=df["low"])
        df=df.loc[valid]
        if df.empty:return None
        out=df.set_index("timestamp")[["open","high","low","close","volume"]].astype(float); out.index.name="timestamp"; return out
    except Exception as exc:
        logging.warning(f"[DATA NORMALIZE] Données corrompues: {exc}"); return None

def _fetch_biquote_safe(symbol,timeframe,limit):
    r=requests.get(f"{BIQUOTE_BASE_URL}/{symbol}/ohlc",params={"interval":timeframe,"limit":int(limit)},timeout=5)
    if r.status_code!=200:raise RuntimeError(f"BiQuote HTTP {r.status_code}")
    payload=r.json(); bars=payload.get("bars") if isinstance(payload,dict) else None
    if not isinstance(bars,list) or not bars:raise ValueError("Réponse BiQuote sans bars")
    rows=[{"timestamp":b.get("openTime",b.get("timestamp")),"open":b.get("open"),"high":b.get("high"),"low":b.get("low"),"close":b.get("close"),"volume":b.get("volume",0.0)} for b in bars if isinstance(b,dict)]
    df=_normalize_market_dataframe(rows)
    if df is None or df.empty:raise ValueError("Données BiQuote invalides")
    return df.tail(limit)

def _fetch_kraken_safe(symbol,timeframe,limit):
    if symbol!="BTCUSD":raise ValueError("Kraken réservé à BTCUSD")
    r=requests.get("https://api.kraken.com/0/public/OHLC",params={"pair":"XBTUSD","interval":{"1m":1,"5m":5,"15m":15}[timeframe]},timeout=5)
    if r.status_code!=200:raise RuntimeError(f"Kraken HTTP {r.status_code}")
    payload=r.json()
    if payload.get("error"):raise RuntimeError(f"Kraken API: {payload['error']}")
    result=payload.get("result",{}); key=next((k for k in result if k!="last"),None); raw=result.get(key) if key else None
    if not isinstance(raw,list) or not raw:raise ValueError("Réponse Kraken vide")
    rows=[]
    for x in raw:
        if isinstance(x,(list,tuple)) and len(x)>=7:rows.append({"timestamp":pd.to_datetime(float(x[0]),unit="s",utc=True),"open":x[1],"high":x[2],"low":x[3],"close":x[4],"volume":x[6]})
    df=_normalize_market_dataframe(rows)
    if df is None or df.empty:raise ValueError("Données Kraken invalides")
    return df.tail(limit)

def _fetch_tiingo_safe(symbol,timeframe,limit):
    if symbol not in _TIIINGO_SYMBOLS:raise ValueError("Tiingo réservé à XAUUSD/EURUSD/GBPUSD")
    if not TIINGO_API_TOKEN:raise RuntimeError("TIINGO_API_TOKEN non configuré")
    start=datetime.now(timezone.utc)-timedelta(seconds=_TIMEFRAME_SECONDS[timeframe]*max(limit+30,60))
    r=requests.get(f"https://api.tiingo.com/tiingo/fx/{_TIIINGO_SYMBOLS[symbol]}/prices",params={"startDate":start.strftime("%Y-%m-%dT%H:%M:%SZ"),"resampleFreq":{"1m":"1min","5m":"5min","15m":"15min"}[timeframe],"token":TIINGO_API_TOKEN},timeout=5)
    if r.status_code!=200:raise RuntimeError(f"Tiingo HTTP {r.status_code}")
    payload=r.json()
    if not isinstance(payload,list) or not payload:raise ValueError("Réponse Tiingo vide")
    rows=[{"timestamp":x.get("date"),"open":x.get("open"),"high":x.get("high"),"low":x.get("low"),"close":x.get("close"),"volume":x.get("volume",0.0)} for x in payload if isinstance(x,dict)]
    df=_normalize_market_dataframe(rows)
    if df is None or df.empty:raise ValueError("Données Tiingo invalides")
    return df.tail(limit)

def _fetch_yahoo_safe(symbol,timeframe,limit):
    now=datetime.now(timezone.utc); ticker=_YAHOO_SYMBOLS[symbol]; p1=int((now-timedelta(seconds=_TIMEFRAME_SECONDS[timeframe]*max(limit+30,60))).timestamp()); p2=int(now.timestamp())
    r=requests.get(f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}",params={"period1":p1,"period2":p2,"interval":timeframe,"events":"history","includeAdjustedClose":"true"},timeout=5,headers={"User-Agent":"Mozilla/5.0"})
    if r.status_code!=200:raise RuntimeError(f"Yahoo Finance HTTP {r.status_code}")
    payload=r.json(); result=payload.get("chart",{}).get("result")
    if not result:raise ValueError(f"Yahoo Finance réponse vide: {payload.get('chart',{}).get('error')}")
    q=((result[0].get("indicators") or {}).get("quote") or [{}])[0]; ts=result[0].get("timestamp") or []; o=q.get("open") or []; h=q.get("high") or []; l=q.get("low") or []; c=q.get("close") or []; v=q.get("volume") or [0.0]*len(ts)
    rows=[{"timestamp":pd.to_datetime(stamp,unit="s",utc=True),"open":o[i] if i<len(o) else None,"high":h[i] if i<len(h) else None,"low":l[i] if i<len(l) else None,"close":c[i] if i<len(c) else None,"volume":v[i] if i<len(v) else 0.0} for i,stamp in enumerate(ts)]
    df=_normalize_market_dataframe(rows)
    if df is None or df.empty:raise ValueError("Données Yahoo invalides")
    return df.tail(limit)

def _cache_market_data(symbol, timeframe, df):
    if df is None or df.empty:
        return
    with MARKET_DATA_CACHE_LOCK:
        MARKET_DATA_CACHE[(str(symbol).upper(), str(timeframe).lower())] = df.copy()


def _get_cached_market_data(symbol, timeframe, limit=300):
    with MARKET_DATA_CACHE_LOCK:
        df = MARKET_DATA_CACHE.get((str(symbol).upper(), str(timeframe).lower()))
        if df is None:
            return None
        return df.tail(limit).copy()


def fetch_market_data_safe(symbol,timeframe,limit=300):
    symbol=str(symbol).upper().strip(); timeframe=str(timeframe).lower().strip(); limit=max(1,int(limit))
    if symbol not in SYMBOLS or timeframe not in _TIMEFRAME_SECONDS:
        logging.warning(f"[DATA] Paramètres invalides: {symbol} {timeframe}"); return None
    loaders=[("BiQuote",lambda:_fetch_biquote_safe(symbol,timeframe,limit))]
    loaders.append(("Kraken",lambda:_fetch_kraken_safe(symbol,timeframe,limit)) if symbol=="BTCUSD" else ("Tiingo",lambda:_fetch_tiingo_safe(symbol,timeframe,limit)))
    loaders.append(("Yahoo Finance",lambda:_fetch_yahoo_safe(symbol,timeframe,limit)))
    for source,loader in loaders:
        try:
            with DATA_SOURCE_LOCK:
                source_call_id = f"{symbol}:{timeframe}:{source}"
            df=loader()
            if df is None or df.empty:raise ValueError("DataFrame vide")
            if list(df.columns)!=["open","high","low","close","volume"] or not isinstance(df.index,pd.DatetimeIndex):raise ValueError("Format OHLCV invalide")
            df = df.tail(limit)
            _cache_market_data(symbol, timeframe, df)
            logging.info(f"[DATA {source}] {symbol} {timeframe} | {len(df)} bougies"); return df
        except Exception as exc:logging.warning(f"[DATA FALLBACK] {source_call_id} échoue pour {symbol} {timeframe}: {exc}")
    logging.error(f"[DATA FALLBACK] Toutes les sources ont échoué pour {symbol} {timeframe}"); return None

def fetch_biquote_ohlcv(symbol,timeframe="15m",count=100):
    df=fetch_market_data_safe(symbol,timeframe,limit=count); return df if df is not None else _empty_market_dataframe()


# ==========================================
# PRIX LIVE — MATRICE DE SECOURS
# ==========================================

def fetch_biquote_live_price(symbol):
    try:
        df=fetch_market_data_safe(symbol,"1m",limit=2)
        return float(df["close"].iloc[-1]) if df is not None and not df.empty else None
    except Exception as exc:
        logging.warning(f"[LIVE PRICE] Erreur {symbol}: {exc}")
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


def detect_structure_pivots(
    df,
    left=2,
    right=2
):
    """Détecte les pivots utilisés par la structure SMC existante."""

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
                "timestamp": str(df.index[i])
            })

        if low_value == low_window.min():
            pivots.append({
                "type": "LOW",
                "index": i,
                "price": float(low_value),
                "timestamp": str(df.index[i])
            })

    return sorted(pivots, key=lambda item: item["index"])


def detect_pivots(df, window=5):
    """
    Détecte les fractals M15 majeurs de manière vectorisée.

    Un pivot haut possède ``window`` bougies avec des plus hauts
    inférieurs de chaque côté. Même principe inverse pour un pivot bas.
    """

    if df is None or df.empty:
        empty = pd.DataFrame(columns=["index", "price"])
        return {"highs": empty.copy(), "lows": empty.copy()}

    window = int(window)
    if window < 1:
        raise ValueError("window doit être >= 1")

    data = df.copy()
    data.columns = [str(c).strip().lower() for c in data.columns]

    required = {"high", "low", "close"}
    if not required.issubset(data.columns):
        raise ValueError("Le DataFrame doit contenir high, low et close")

    high = pd.to_numeric(data["high"], errors="coerce")
    low = pd.to_numeric(data["low"], errors="coerce")

    span = 2 * window + 1
    high_max = high.rolling(span, center=True, min_periods=span).max()
    low_min = low.rolling(span, center=True, min_periods=span).min()

    pivot_high = high.eq(high_max)
    pivot_low = low.eq(low_min)

    # Évite les doubles pivots consécutifs sur des plateaux de prix.
    pivot_high &= high.gt(high.shift(1)) & high.gt(high.shift(-1))
    pivot_low &= low.lt(low.shift(1)) & low.lt(low.shift(-1))

    high_idx = np.flatnonzero(pivot_high.fillna(False).to_numpy())
    low_idx = np.flatnonzero(pivot_low.fillna(False).to_numpy())

    return {
        "highs": pd.DataFrame({
            "index": high_idx,
            "price": high.iloc[high_idx].to_numpy(dtype=float)
        }),
        "lows": pd.DataFrame({
            "index": low_idx,
            "price": low.iloc[low_idx].to_numpy(dtype=float)
        })
    }


# ==========================================
# PHASE 1 — ZONES MAJEURES M15
# ==========================================

M15_ZONE_LOCK = threading.RLock()
M15_ZONES = {symbol: [] for symbol in SYMBOLS}
M15_ZONE_STATE = {
    symbol: {"bias": None, "bos": None, "bos_level": None}
    for symbol in SYMBOLS
}

M15_ZONE_BUFFER_PCT = {
    "EURUSD": 0.0005,
    "GBPUSD": 0.0005,
    "XAUUSD": 0.0010,
    "BTCUSD": 0.0030
}
M15_ZONE_ATR_PERIOD = 14
M15_ZONE_ATR_MULTIPLIER = 0.50
M15_ZONE_MIN_TOUCHES = 2


def _m15_zone_atr(df, period=14):
    previous_close = df["close"].shift(1)
    tr = pd.concat([
        df["high"] - df["low"],
        (df["high"] - previous_close).abs(),
        (df["low"] - previous_close).abs()
    ], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / period, adjust=False, min_periods=1).mean()


def _m15_zone_width(price, atr_value, symbol):
    pct_width = abs(float(price)) * M15_ZONE_BUFFER_PCT[symbol]
    atr_width = (float(atr_value) * M15_ZONE_ATR_MULTIPLIER
                 if np.isfinite(atr_value) and atr_value > 0 else 0.0)
    return float(max(pct_width, atr_width))


def _m15_zone_touch_count(df, level, width, level_type):
    high = df["high"].to_numpy(dtype=float)
    low = df["low"].to_numpy(dtype=float)
    close = df["close"].to_numpy(dtype=float)

    zone_min = level - width
    zone_max = level + width
    intersects = (high >= zone_min) & (low <= zone_max)

    if level_type == "SUPPORT":
        reaction = close >= level
    else:
        reaction = close <= level

    return int(np.count_nonzero(intersects & reaction))


def _cluster_m15_pivots(levels, types, atr_value, symbol):
    if not levels:
        return []

    order = np.argsort(np.asarray(levels, dtype=float))
    sorted_levels = np.asarray(levels, dtype=float)[order]
    sorted_types = np.asarray(types, dtype=object)[order]

    clusters = []
    current_levels = [float(sorted_levels[0])]
    current_types = [str(sorted_types[0])]

    for level, level_type in zip(sorted_levels[1:], sorted_types[1:]):
        center = float(np.mean(current_levels))
        width = _m15_zone_width(center, atr_value, symbol)
        if abs(float(level) - center) <= width * 2.0:
            current_levels.append(float(level))
            current_types.append(str(level_type))
        else:
            clusters.append((
                float(np.mean(current_levels)),
                max(set(current_types), key=current_types.count)
            ))
            current_levels = [float(level)]
            current_types = [str(level_type)]

    clusters.append((
        float(np.mean(current_levels)),
        max(set(current_types), key=current_types.count)
    ))
    return clusters


def _detect_m15_order_blocks(df,lookback=120):
    data=df.tail(lookback); bull=[]; bear=[]; atr=_m15_zone_atr(data,14).replace(0,np.nan); body=(data["close"]-data["open"]).abs(); disp=body>=(atr*0.8)
    for i in range(1,len(data)-1):
        if not bool(disp.iloc[i+1]):continue
        p,m=data.iloc[i],data.iloc[i+1]
        if float(m["close"])>float(m["open"]) and float(p["close"])<float(p["open"]):bull.append({"type":"BULLISH_OB","index":i,"timestamp":str(data.index[i]),"low":float(p["low"]),"high":float(p["high"])})
        elif float(m["close"])<float(m["open"]) and float(p["close"])>float(p["open"]):bear.append({"type":"BEARISH_OB","index":i,"timestamp":str(data.index[i]),"low":float(p["low"]),"high":float(p["high"])})
    return bull[-12:],bear[-12:]

def _detect_m15_fvgs(df,lookback=150):
    data=df.tail(lookback); bull=[];bear=[]
    for i in range(2,len(data)):
        a,c=data.iloc[i-2],data.iloc[i]
        if float(c["low"])>float(a["high"]):bull.append({"type":"BULLISH_FVG","index":i,"timestamp":str(data.index[i]),"lower":float(a["high"]),"upper":float(c["low"])})
        if float(c["high"])<float(a["low"]):bear.append({"type":"BEARISH_FVG","index":i,"timestamp":str(data.index[i]),"lower":float(c["high"]),"upper":float(a["low"])})
    return bull[-20:],bear[-20:]

def build_m15_major_zones(df,symbol,window=5):
    symbol=str(symbol).upper()
    if symbol not in M15_ZONE_BUFFER_PCT:raise ValueError(f"Symbole non supporté: {symbol}")
    if df is None or df.empty:return {"zones":[],"bias":None,"bos":None,"bos_level":None,"order_blocks":[],"fvgs":[],"macro_high":None,"macro_low":None}
    data=df.copy();data.columns=[str(c).strip().lower() for c in data.columns]
    for col in ("open","high","low","close"):data[col]=pd.to_numeric(data[col],errors="coerce")
    data=data.dropna(subset=["open","high","low","close"])
    atr=float(_m15_zone_atr(data,M15_ZONE_ATR_PERIOD).iloc[-1]);piv=detect_pivots(data,window);hi=piv["highs"]["price"].tolist();lo=piv["lows"]["price"].tolist();levels=hi+lo;types=["RESISTANCE"]*len(hi)+["SUPPORT"]*len(lo);zones=[]
    for level,typ in _cluster_m15_pivots(levels,types,atr,symbol):
        w=_m15_zone_width(level,atr,symbol);touch=_m15_zone_touch_count(data,level,w,typ)
        if touch>=M15_ZONE_MIN_TOUCHES:zones.append({"level_type":typ,"zone_min":float(level-w),"zone_max":float(level+w),"touches":touch,"is_active":True})
    closed=data.iloc[:-1] if len(data)>1 else data;ema20=closed["close"].ewm(span=20,adjust=False).mean().iloc[-1];ema50=closed["close"].ewm(span=50,adjust=False).mean().iloc[-1];bias="BULLISH" if ema20>ema50 else "BEARISH" if ema20<ema50 else None;bos=None;bos_level=None;last=data.iloc[-2]
    for z in zones:
        if z["level_type"]=="RESISTANCE" and float(last["close"])>z["zone_max"] and float(last["close"])>=float(last["open"]):z["is_active"]=False;bos,bos_level=("BULLISH_BOS",float(z["zone_max"])) if bos is None or z["zone_max"]>bos_level else (bos,bos_level)
        elif z["level_type"]=="SUPPORT" and float(last["close"])<z["zone_min"] and float(last["close"])<=float(last["open"]):z["is_active"]=False;bos,bos_level=("BEARISH_BOS",float(z["zone_min"])) if bos is None or bos_level is None or z["zone_min"]<bos_level else (bos,bos_level)
    ob1,ob2=_detect_m15_order_blocks(data);fv1,fv2=_detect_m15_fvgs(data)
    return {"zones":zones,"bias":bias,"bos":bos,"bos_level":bos_level,"order_blocks":ob1+ob2,"fvgs":fv1+fv2,"macro_high":float(data["high"].max()),"macro_low":float(data["low"].min())}

def update_m15_major_zones(df, symbol, window=5):
    state = build_m15_major_zones(df, symbol, window)
    symbol = str(symbol).upper()
    with M15_ZONE_LOCK:
        M15_ZONES[symbol] = [dict(zone) for zone in state["zones"]]
        M15_ZONE_STATE[symbol] = {
            "bias": state["bias"],
            "bos": state["bos"],
            "bos_level": state["bos_level"],
            "order_blocks": [dict(x) for x in state.get("order_blocks",[])],
            "fvgs": [dict(x) for x in state.get("fvgs",[])],
            "macro_high": state.get("macro_high"),
            "macro_low": state.get("macro_low"),
            "updated_at": datetime.now(timezone.utc).isoformat()
        }
    return state


def check_m15_zones(current_price, symbol):
    symbol = str(symbol).upper()
    price = float(current_price)
    if symbol not in SYMBOLS or not np.isfinite(price):
        return None

    with M15_ZONE_LOCK:
        zones = [dict(zone) for zone in M15_ZONES.get(symbol, [])]
        state = dict(M15_ZONE_STATE.get(symbol, {}))

    bias = state.get("bias")
    for zone in zones:
        if not zone.get("is_active") or int(zone.get("touches", 0)) < M15_ZONE_MIN_TOUCHES:
            continue
        if not (float(zone["zone_min"]) <= price <= float(zone["zone_max"])):
            continue

        if zone["level_type"] == "SUPPORT" and bias == "BULLISH":
            direction = "HAUSSIER"
        elif zone["level_type"] == "RESISTANCE" and bias == "BEARISH":
            direction = "BAISSIER"
        else:
            continue

        trigger = (float(zone["zone_min"]) + float(zone["zone_max"])) / 2.0
        structure = {
            "direction": direction,
            "bos_price": trigger,
            "bos_timestamp": strategy_timestamp(),
            "trigger_price": trigger,
            "trigger_type": zone["level_type"],
            "zone_min": float(zone["zone_min"]),
            "zone_max": float(zone["zone_max"]),
            "trigger_timestamp": strategy_timestamp(),
            "closed_timestamp": strategy_timestamp()
        }
        return create_pending_opportunity(symbol, direction, trigger, structure)

    return None


def get_m15_major_zones(symbol):
    symbol = str(symbol).upper()
    with M15_ZONE_LOCK:
        return [dict(zone) for zone in M15_ZONES.get(symbol, [])]


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

    pivots = detect_structure_pivots(
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
                closed_candle.name
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
                closed_candle.name
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
        "m15_zone_min": float(m15_structure.get("zone_min",m15_structure["trigger_price"])),
        "m15_zone_max": float(m15_structure.get("zone_max",m15_structure["trigger_price"])),
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
    """Cartographie macro M15 sur 300 bougies: OB, FVG et S/R."""
    try:
        if not is_trading_session() or is_pair_locked_24h(symbol):
            return
        df_m15 = fetch_biquote_ohlcv(
            symbol,
            timeframe="15m",
            count=300
        )

        if df_m15.empty or len(df_m15) < 300:
            logging.warning(
                f"[M15 ZONES] Données insuffisantes pour {symbol}."
            )
            return

        state = update_m15_major_zones(
            df_m15,
            symbol,
            window=5
        )

        current_price = float(df_m15["close"].iloc[-2])
        opportunity = check_m15_zones(
            current_price,
            symbol
        )

        logging.info(
            f"[M15 MACRO] {symbol} | 300 bougies | SR={len(state['zones'])} | OB={len(state.get('order_blocks',[]))} | FVG={len(state.get('fvgs',[]))} | bias={state['bias']} | BOS={state['bos']} | price={current_price:.8f}"
        )

        if opportunity:
            logging.info(
                f"[M15 ZONES] {symbol} | opportunité créée | "
                f"{opportunity}"
            )

    except Exception as e:
        logging.exception(
            f"[M15 ZONES] Erreur scan {symbol}: {e}"
        )

def _last_closed_timestamp(
    df
):
    if df is None or len(df) < 2:
        return None

    return str(
        df.index[-2]
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
        closed_df.index.astype(str)
        != str(last_timestamp)
    ]

    if matches.empty:
        return None

    return matches.iloc[-1]


def check_m5_liquidity(df,opp):
    if df is None or len(df)<5:return False,None
    direction=str(opp["direction"]).upper();stamp=str(df.index[-2]);last=df.iloc[-2]
    if opp.get("last_processed_m5_timestamp")==stamp:return False,None
    zmin=float(opp.get("m15_zone_min",opp["trigger_price"]));zmax=float(opp.get("m15_zone_max",opp["trigger_price"]));high=float(last["high"]);low=float(last["low"]);close=float(last["close"])
    return ((low<zmin and zmin<=close<=zmax) if direction=="HAUSSIER" else (high>zmax and zmin<=close<=zmax)),stamp


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


def check_m1_choch(df, opp):
    """Détecte le CHoCH M1 et calcule le SL structurel absolu entre la liquidité M5 et le CHoCH."""
    try:
        if df is None or len(df) < 10:
            return None
        pivots = detect_structure_pivots(df, M1_PIVOT_LEFT, M1_PIVOT_RIGHT)
        if not pivots:
            return None
        closed_index = len(df) - 2
        candle = df.iloc[closed_index]
        close = float(candle["close"])
        direction = str(opp["direction"]).upper()
        m5_timestamp = opp.get("m5_liquidity_timestamp")
        eligible = [p for p in pivots if not m5_timestamp or str(p["timestamp"]) >= str(m5_timestamp)]
        if direction == "HAUSSIER":
            candidates = [p for p in eligible if p["type"] == "HIGH" and p["index"] < closed_index]
            if not candidates:
                return None
            broken = candidates[-1]
            if close <= broken["price"]:
                return None
        else:
            candidates = [p for p in eligible if p["type"] == "LOW" and p["index"] < closed_index]
            if not candidates:
                return None
            broken = candidates[-1]
            if close >= broken["price"]:
                return None

        start_idx = 0
        if m5_timestamp:
            ts = df.index.astype(str)
            matches = [i for i, value in enumerate(ts) if value >= str(m5_timestamp)]
            if matches:
                start_idx = matches[0]
        end_idx = closed_index
        if start_idx > end_idx:
            return None
        segment = df.iloc[start_idx:end_idx + 1]
        if segment.empty:
            return None
        if direction == "HAUSSIER":
            manipulation_index = segment["low"].idxmin()
            structural_sl = float(segment.loc[manipulation_index, "low"])
        else:
            manipulation_index = segment["high"].idxmax()
            structural_sl = float(segment.loc[manipulation_index, "high"])
        manipulation_timestamp = str(manipulation_index)
        return {
            "timestamp": str(candle.name),
            "broken_level": float(broken["price"]),
            "sl": structural_sl,
            "sl_timestamp": manipulation_timestamp,
            "pivot_index": int(broken["index"]),
            "m5_liquidity_index": int(start_idx),
            "m1_choch_index": int(end_idx),
        }
    except Exception as exc:
        logging.exception(f"[M1-CHOCH] Erreur: {exc}")
        return None


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
                        candle.name
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
                        candle.name
                    ),
                    "open": open_price,
                    "high": float(candle["high"]),
                    "low": float(candle["low"]),
                    "close": close_price,
                    "type": "ORDER_BLOCK_BAISSIER"
                }

    return None


def _find_recent_fvg(df, bos_index, direction, lookback=4):
    try:
        first = max(2, bos_index - lookback)
        last = bos_index - 1
        for i in range(last, first - 1, -1):
            a = df.iloc[i - 2]
            c = df.iloc[i]
            if direction == "HAUSSIER" and float(c["low"]) > float(a["high"]):
                return {"index": i, "type": "FVG_HAUSSIER", "lower": float(a["high"]), "upper": float(c["low"]), "entry": float(c["low"]), "timestamp": str(c.name)}
            if direction == "BAISSIER" and float(c["high"]) < float(a["low"]):
                return {"index": i, "type": "FVG_BAISSIER", "lower": float(c["high"]), "upper": float(a["low"]), "entry": float(c["high"]), "timestamp": str(c.name)}
    except Exception as exc:
        logging.exception(f"[FVG] Erreur détection: {exc}")
    return None


def check_m1_bos(df, opp):
    try:
        if df is None or len(df) < 12:
            return None
        pivots = detect_structure_pivots(df, M1_PIVOT_LEFT, M1_PIVOT_RIGHT)
        if not pivots:
            return None
        choch_timestamp = str(opp.get("m1_choch_timestamp"))
        closed_df = df.iloc[:-1].copy()
        positions = [i for i, value in enumerate(closed_df.index.astype(str)) if value == choch_timestamp]
        if not positions:
            return None
        choch_index = positions[-1]
        closed_index = len(df) - 2
        direction = str(opp["direction"]).upper()
        if direction == "HAUSSIER":
            candidates = [p for p in pivots if p["type"] == "HIGH" and choch_index < p["index"] < closed_index]
            if not candidates or float(df["close"].iloc[closed_index]) <= candidates[-1]["price"]:
                return None
        else:
            candidates = [p for p in pivots if p["type"] == "LOW" and choch_index < p["index"] < closed_index]
            if not candidates or float(df["close"].iloc[closed_index]) >= candidates[-1]["price"]:
                return None
        target = candidates[-1]
        fvg = _find_recent_fvg(df, target["index"], direction, 4)
        order_block = _find_order_block(df, target["index"], direction)
        if fvg is not None:
            entry = float(fvg["entry"])
            entry_source = "FVG"
        elif order_block is not None:
            entry = float(order_block["open"])
            entry_source = "ORDER_BLOCK"
        else:
            return None
        sl = float(opp["m1_sl"])
        if (direction == "HAUSSIER" and sl >= entry) or (direction == "BAISSIER" and sl <= entry):
            return None
        return {
            "timestamp": str(df.index[closed_index]),
            "bos_level": float(target["price"]),
            "bos_pivot_timestamp": str(target["timestamp"]),
            "order_block": order_block,
            "fvg": fvg,
            "entry_price": entry,
            "entry_source": entry_source,
            "sl": sl,
        }
    except Exception as exc:
        logging.exception(f"[M1-BOS] Erreur: {exc}")
        return None


def _last_m5_structural_target(df_m5, direction, minimum_target):
    pivots=detect_structure_pivots(df_m5,M5_PIVOT_LEFT,M5_PIVOT_RIGHT)
    if str(direction).upper()=="HAUSSIER":
        c=[p for p in pivots if p["type"]=="HIGH" and p["price"]>minimum_target]
        fallback=float(df_m5["high"].max())
        return float(c[-1]["price"] if c else fallback)
    c=[p for p in pivots if p["type"]=="LOW" and p["price"]<minimum_target]
    fallback=float(df_m5["low"].min())
    return float(c[-1]["price"] if c else fallback)


def calculate_take_profits(direction,entry_price,stop_loss,df_m5,df_m15):
    try:
        direction=str(direction).upper();entry_price=float(entry_price);stop_loss=float(stop_loss)
        if direction not in ("HAUSSIER","BAISSIER") or df_m5 is None or df_m15 is None or len(df_m5)<10 or len(df_m15)<50:return None
        if direction=="HAUSSIER":
            if stop_loss>=entry_price:return None
            risk=entry_price-stop_loss;tp1=entry_price+risk;tp2=_last_m5_structural_target(df_m5,direction,tp1);tp3=float(df_m15["high"].max())*0.9998
            if tp2<=entry_price:tp2=tp1
            if tp3<=tp2:return None
            reward=tp3-entry_price
        else:
            if stop_loss<=entry_price:return None
            risk=stop_loss-entry_price;tp1=entry_price-risk;tp2=_last_m5_structural_target(df_m5,direction,tp1);tp3=float(df_m15["low"].min())*1.0002
            if tp2>=entry_price:tp2=tp1
            if tp3>=tp2:return None
            reward=entry_price-tp3
        if risk<=0 or reward<=0:return None
        rr=reward/risk
        if rr<3.0:return None
        return {"tp1":float(tp1),"tp2":float(tp2),"tp3":float(tp3),"rr_tp3":float(rr),"tp2_source":"M5_STRUCTURAL_PIVOT","tp3_source":"M15_300_MACRO_EXTREME"}
    except Exception as exc:logging.exception(f"[TP] Erreur calcul TP: {exc}");return None


def _build_active_trade(
    candidate_id,
    opp,
    execution
):
    symbol = opp["symbol"]
    direction = opp["direction"]

    entry = float(execution["entry_price"])
    sl = float(execution["sl"])

    df_m15=fetch_biquote_ohlcv(symbol,timeframe="15m",count=300)
    df_m5=fetch_biquote_ohlcv(symbol,timeframe="5m",count=300)
    if df_m15.empty or len(df_m15)<50 or df_m5.empty or len(df_m5)<10:
        logging.info(
            f"[M1] {symbol} {direction}: données M15 insuffisantes pour TP."
        )
        return None

    tp_levels = calculate_take_profits(
        direction=direction,
        entry_price=entry,
        stop_loss=sl,
        df_m5=df_m5,
        df_m15=df_m15
    )
    if tp_levels is None:
        logging.info(f"[M1] {symbol} {direction}: trade invalidé — RR TP3 < 1:3 ou niveaux invalides.")
        return None

    if direction == "HAUSSIER":
        trade_direction = "BUY"
    else:
        trade_direction = "SELL"

    trade_id = (
        f"TRADE_{trade_direction}_{symbol}_"
        f"{int(time.time() * 1000)}"
    )

    order_block = execution.get("order_block")
    risk_pips, reward_pips, rr_pips = calculate_pip_metrics(symbol, entry, sl, tp_levels["tp3"])

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
        "risk_pips": risk_pips,
        "reward_pips": reward_pips,
        "rr_pips": rr_pips,
        "entry_source": execution.get("entry_source"),
        "fvg": execution.get("fvg"),
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


def execute_m1_order(candidate_id, opp, execution):
    try:
        symbol = str(opp.get("symbol", "")).upper()
        if symbol not in SYMBOLS or not is_trading_session() or is_pair_locked_24h(symbol):
            return False
        result = _build_active_trade(candidate_id, opp, execution)
        if result is None:
            _delete_opportunity(candidate_id)
            return False
        trade_id, trade = result
        with MARKET_EXECUTION_LOCK:
            with JSON_LOCK:
                active_trades = load_json(TRADES_FILE)
                if any(existing.get("candidate_id") == candidate_id for existing in active_trades.values()):
                    return False
                active_trades[trade_id] = trade
                save_json(TRADES_FILE, active_trades)
            ensure_trade_history_record(trade_id, trade)
        direction_text = "ACHAT" if trade["direction"] == "BUY" else "VENTE"
        message = (f"{'🟢' if trade['direction']=='BUY' else '🔴'} {direction_text} — {symbol}\n\n"
                   f"Entrée : {format_telegram_price(symbol, trade['entry_price'])}\n"
                   f"SL absolu : {format_telegram_price(symbol, trade['initial_sl'])}\n"
                   f"TP1 : {format_telegram_price(symbol, trade['tp1'])}\n"
                   f"TP2 : {format_telegram_price(symbol, trade['tp2'])}\n"
                   f"TP3 : {format_telegram_price(symbol, trade['tp3'])}\n"
                   f"RR théorique : 1:{trade['rr_pips']:.2f}\n"
                   f"Risque : {trade['risk_pips']:.1f} pips | Gain TP3 : {trade['reward_pips']:.1f} pips")
        if not send_telegram_message(message):
            logging.error(f"[EXECUTION] {trade_id}: Telegram non confirmé; paire non verrouillée.")
            return False
        _delete_opportunity(candidate_id)
        logging.info(f"[EXECUTION] {trade_id} envoyé pour {symbol}.")
        return True
    except Exception as exc:
        logging.exception(f"[EXECUTION] Erreur {candidate_id}: {exc}")
        return False


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
        if not is_trading_session() or is_pair_locked_24h(symbol):
            return
        if status == "WAITING_M5_LIQUIDITY":
            df_m5 = fetch_biquote_ohlcv(
                symbol,
                timeframe="5m",
                count=300
            )

            if df_m5.empty:
                return
            latest_m5_closed=str(df_m5.index[-2]) if len(df_m5)>=2 else None
            if latest_m5_closed==opportunity.get("last_processed_m5_timestamp"):
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
            df_m1 = _get_cached_market_data(symbol, "1m", 300)
            if df_m1 is None or df_m1.empty:
                df_m1 = fetch_biquote_ohlcv(symbol, timeframe="1m", count=300)

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
            df_m1 = _get_cached_market_data(symbol, "1m", 300)
            if df_m1 is None or df_m1.empty:
                df_m1 = fetch_biquote_ohlcv(symbol, timeframe="1m", count=300)

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


def scan_all_symbols_m1():
    """Scan M1 global de tous les actifs chaque minute, puis déclenche les étapes M5/M1 des opportunités."""
    with ThreadPoolExecutor(max_workers=SCAN_WORKERS, thread_name_prefix="m1-market") as executor:
        futures={
            executor.submit(fetch_biquote_ohlcv,symbol,"1m",300): symbol
            for symbol in SYMBOLS
            if not is_pair_locked_24h(symbol)
        }
        for future in as_completed(futures):
            symbol=futures[future]
            try:
                df=future.result()
                if df is not None and not df.empty:
                    logging.info(f"[M1 SCAN] {symbol} | 300 bougies | close={float(df['close'].iloc[-2]):.8f}")
            except Exception as exc:
                logging.exception(f"[M1 SCAN] {symbol} erreur: {exc}")

    with JSON_LOCK:
        opportunities=load_json(OPPORTUNITIES_FILE)
    if not opportunities:
        return
    snapshot=[(candidate_id,opportunity.copy()) for candidate_id,opportunity in opportunities.items()]
    workers=max(1,min(SCAN_WORKERS,len(snapshot)))
    with ThreadPoolExecutor(max_workers=workers,thread_name_prefix="smc-pending") as executor:
        futures=[executor.submit(_process_pending_opportunity,candidate_id,opportunity) for candidate_id,opportunity in snapshot]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                logging.exception(f"[SMC] Erreur worker pending: {exc}")


def scan_pending_opportunities_m1():
    return scan_all_symbols_m1()

def scan_pending_opportunities_m5():
    return scan_pending_opportunities_m1()

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

async def _async_market_scheduler():
    last_m15_slot=None;last_m1_slot=None
    while True:
        try:
            now=datetime.now(timezone.utc)
            if is_trading_session(now):
                m15_slot=now.replace(minute=(now.minute//15)*15,second=0,microsecond=0)
                if now.second>=5 and m15_slot!=last_m15_slot:
                    await asyncio.to_thread(scan_all_symbols_m15);last_m15_slot=m15_slot
                m1_slot=now.replace(second=0,microsecond=0)
                if now.second>=5 and m1_slot!=last_m1_slot:
                    await asyncio.to_thread(scan_all_symbols_m1);last_m1_slot=m1_slot
            await asyncio.sleep(1)
        except asyncio.CancelledError:raise
        except Exception as exc:logging.exception(f"[SCHEDULER] Erreur: {exc}");await asyncio.sleep(5)

def main_scheduler():
    logging.info("Scheduler asynchrone SMC démarré — M15 macro / M5 liquidité / M1 déclencheur, 24h/24, 7j/7.")
    try:asyncio.run(_async_market_scheduler())
    except Exception as exc:logging.exception(f"[SCHEDULER] Arrêt inattendu: {exc}")


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
# VISUALISATION GRAPHIQUE PROPRIETAIRE
# ==========================================

OWNER_CHART_TOKEN_SECRET = os.environ.get(
    "OWNER_CHART_TOKEN_SECRET",
    TELEGRAM_TOKEN or TELEGRAM_OWNER_ID
).strip()


def _owner_chart_token(symbol, timeframe):
    import hashlib
    payload = f"{symbol.upper()}:{timeframe}:{OWNER_CHART_TOKEN_SECRET}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32]


def _owner_chart_authorized(symbol, timeframe, token):
    if not telegram_owner_is_configured():
        return False
    return bool(token) and token == _owner_chart_token(symbol, timeframe)


OWNER_CHART_DISCOVERED_BASE_URL = ""
OWNER_CHART_BASE_URL_LOCK = threading.RLock()


def _remember_owner_chart_public_base_url_from_request():
    global OWNER_CHART_DISCOVERED_BASE_URL
    try:
        host = (request.headers.get("X-Forwarded-Host", "") or request.host or "").strip()
        if not host or host.startswith(("127.", "localhost", "0.0.0.0", "10.", "172.", "192.168.")):
            return
        forwarded_proto = (request.headers.get("X-Forwarded-Proto", "") or "").split(",")[0].strip()
        scheme = forwarded_proto if forwarded_proto in {"http", "https"} else request.scheme
        if scheme not in {"http", "https"}:
            scheme = "https"
        discovered = f"{scheme}://{host}".rstrip("/")
        with OWNER_CHART_BASE_URL_LOCK:
            OWNER_CHART_DISCOVERED_BASE_URL = discovered
    except Exception:
        pass


def _owner_chart_public_base_url():
    base = (os.environ.get("PUBLIC_BASE_URL", "") or "").strip().rstrip("/")
    if base:
        return base
    railway_domain = (os.environ.get("RAILWAY_PUBLIC_DOMAIN", "") or "").strip()
    if railway_domain:
        return railway_domain if railway_domain.startswith("http") else f"https://{railway_domain}"
    railway_static_url = (os.environ.get("RAILWAY_STATIC_URL", "") or "").strip().rstrip("/")
    if railway_static_url:
        return railway_static_url
    render_url = (os.environ.get("RENDER_EXTERNAL_URL", "") or "").strip().rstrip("/")
    if render_url:
        return render_url
    render_hostname = (os.environ.get("RENDER_EXTERNAL_HOSTNAME", "") or "").strip()
    if render_hostname:
        return render_hostname if render_hostname.startswith("http") else f"https://{render_hostname}"
    with OWNER_CHART_BASE_URL_LOCK:
        return OWNER_CHART_DISCOVERED_BASE_URL


def build_owner_chart_url(symbol, timeframe, owner_id):
    if not is_telegram_owner(owner_id):
        return None
    base = _owner_chart_public_base_url()
    if not base:
        return None
    token = _owner_chart_token(symbol, timeframe)
    return f"{base}/owner/chart?symbol={symbol.upper()}&timeframe={timeframe}&token={token}"



def _png_chunk(chunk_type, data):
    return (
        struct.pack(">I", len(data))
        + chunk_type
        + data
        + struct.pack(">I", zlib.crc32(chunk_type + data) & 0xffffffff)
    )


def _build_owner_chart_png(symbol, timeframe):
    """
    Génère directement en mémoire un PNG du marché.
    Aucun serveur web, URL publique ou navigateur n'est nécessaire.
    """
    chart = _owner_chart_data(symbol, timeframe)
    if not chart or not chart.get("candles"):
        return None

    candles = chart["candles"][-180:]
    overlays = chart.get("overlays", {})

    width, height = 1400, 820
    left, right, top, bottom = 70, 35, 55, 65
    plot_w = width - left - right
    plot_h = height - top - bottom

    values = []
    for c in candles:
        values.extend([c["high"], c["low"]])

    for key in ("macro_high", "macro_low", "bos_level"):
        value = overlays.get(key)
        if value is not None:
            try:
                values.append(float(value))
            except (TypeError, ValueError):
                pass

    for collection in (
        overlays.get("zones", []),
        overlays.get("order_blocks", []),
        overlays.get("fvgs", []),
    ):
        for zone in collection:
            for key in ("zone_min", "zone_max", "low", "high", "lower", "upper"):
                value = zone.get(key)
                if value is not None:
                    try:
                        values.append(float(value))
                    except (TypeError, ValueError):
                        pass

    if not values:
        return None

    price_min = min(values)
    price_max = max(values)
    span = max(price_max - price_min, abs(price_max) * 0.001, 1e-9)
    pad = span * 0.08
    price_min -= pad
    price_max += pad

    raw = bytearray(width * height * 3)

    def fill(bg):
        r, g, b = bg
        for y in range(height):
            row = y * width * 3
            for x in range(width):
                i = row + x * 3
                raw[i:i + 3] = bytes((r, g, b))

    fill((11, 14, 19))

    def pixel(x, y, color):
        if 0 <= x < width and 0 <= y < height:
            i = (y * width + x) * 3
            raw[i:i + 3] = bytes(color)

    def rect(x1, y1, x2, y2, color):
        x1, x2 = sorted((max(0, int(x1)), min(width - 1, int(x2))))
        y1, y2 = sorted((max(0, int(y1)), min(height - 1, int(y2))))
        if x2 < x1 or y2 < y1:
            return
        for y in range(y1, y2 + 1):
            for x in range(x1, x2 + 1):
                pixel(x, y, color)

    def line(x1, y1, x2, y2, color, thickness=1):
        x1, y1, x2, y2 = map(int, (x1, y1, x2, y2))
        dx = abs(x2 - x1)
        dy = -abs(y2 - y1)
        sx = 1 if x1 < x2 else -1
        sy = 1 if y1 < y2 else -1
        err = dx + dy
        while True:
            for ox in range(-thickness + 1, thickness):
                for oy in range(-thickness + 1, thickness):
                    pixel(x1 + ox, y1 + oy, color)
            if x1 == x2 and y1 == y2:
                break
            e2 = 2 * err
            if e2 >= dy:
                err += dy
                x1 += sx
            if e2 <= dx:
                err += dx
                y1 += sy

    def py(value):
        return top + int((price_max - float(value)) / (price_max - price_min) * plot_h)

    def px(index):
        if len(candles) <= 1:
            return left + plot_w // 2
        return left + int(index * plot_w / (len(candles) - 1))

    # Grille
    for n in range(1, 6):
        y = top + int(n * plot_h / 6)
        line(left, y, width - right, y, (31, 37, 47), 1)

    for n in range(1, 10):
        x = left + int(n * plot_w / 10)
        line(x, top, x, height - bottom, (25, 30, 39), 1)

    # Zones M15 / OB / FVG
    def draw_zone(z, color):
        lows = []
        highs = []
        for key in ("zone_min", "low", "lower"):
            if z.get(key) is not None:
                try:
                    lows.append(float(z[key]))
                except (TypeError, ValueError):
                    pass
        for key in ("zone_max", "high", "upper"):
            if z.get(key) is not None:
                try:
                    highs.append(float(z[key]))
                except (TypeError, ValueError):
                    pass
        if not lows or not highs:
            return
        y1, y2 = py(max(highs)), py(min(lows))
        rect(left, y1, width - right, y2, color)

    for z in overlays.get("zones", []):
        draw_zone(
            z,
            (18, 55, 105) if str(z.get("level_type", "")).upper() == "SUPPORT"
            else (105, 28, 38)
        )

    for z in overlays.get("order_blocks", []):
        draw_zone(z, (78, 34, 110))

    for z in overlays.get("fvgs", []):
        draw_zone(z, (110, 88, 20))

    # Prix clés
    key_lines = (
        ("macro_high", (239, 68, 68)),
        ("macro_low", (59, 130, 246)),
        ("bos_level", (245, 158, 11)),
    )
    for key, color in key_lines:
        value = overlays.get(key)
        if value is not None:
            try:
                y = py(float(value))
                line(left, y, width - right, y, color, 2)
            except (TypeError, ValueError):
                pass

    # Chandeliers
    candle_w = max(2, int(plot_w / max(len(candles), 1) * 0.58))
    for i, c in enumerate(candles):
        x = px(i)
        y_high = py(c["high"])
        y_low = py(c["low"])
        y_open = py(c["open"])
        y_close = py(c["close"])
        bullish = c["close"] >= c["open"]
        color = (38, 166, 154) if bullish else (239, 83, 80)

        line(x, y_high, x, y_low, color, 1)
        rect(
            x - candle_w // 2,
            min(y_open, y_close),
            x + candle_w // 2,
            max(y_open, y_close),
            color
        )

    # Cadre
    line(left, top, width - right, top, (48, 54, 65))
    line(left, height - bottom, width - right, height - bottom, (48, 54, 65))
    line(left, top, left, height - bottom, (48, 54, 65))
    line(width - right, top, width - right, height - bottom, (48, 54, 65))

    # Encode PNG RGB sans dépendance externe.
    rows = []
    stride = width * 3
    for y in range(height):
        rows.append(b"\x00" + bytes(raw[y * stride:(y + 1) * stride]))
    compressed = zlib.compress(b"".join(rows), 6)

    png = bytearray(b"\x89PNG\r\n\x1a\n")
    png += _png_chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0))
    png += _png_chunk(b"IDAT", compressed)
    png += _png_chunk(b"IEND", b"")
    return bytes(png)


def send_telegram_owner_photo(photo_bytes, caption):
    """
    Envoie une image directement au propriétaire.
    Aucun lien public n'est utilisé.
    """
    if not telegram_owner_is_configured() or not photo_bytes:
        return False

    try:
        response = requests.post(
            telegram_api_url("sendPhoto"),
            data={
                "chat_id": TELEGRAM_OWNER_ID,
                "caption": str(caption),
            },
            files={
                "photo": (
                    "nova_chart.png",
                    io.BytesIO(photo_bytes),
                    "image/png",
                )
            },
            timeout=30,
        )

        if response.status_code != 200:
            logging.error(
                f"Erreur Telegram sendPhoto HTTP {response.status_code}: "
                f"{response.text}"
            )
            return False

        try:
            data = response.json()
        except ValueError:
            logging.error(
                f"Réponse Telegram sendPhoto invalide : {response.text}"
            )
            return False

        if not data.get("ok", False):
            logging.error(f"Telegram a refusé sendPhoto : {data}")
            return False

        return True

    except Exception as e:
        logging.exception(
            f"Erreur envoi graphique Telegram : {e}"
        )
        return False


def _owner_chart_data(symbol, timeframe):
    df = fetch_market_data_safe(symbol, timeframe, limit=300)
    if df is None or df.empty:
        return None

    candles = []
    for ts, row in df.tail(300).iterrows():
        try:
            epoch = int(pd.Timestamp(ts).timestamp())
            candles.append({
                "time": epoch,
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"])
            })
        except Exception:
            continue

    with M15_ZONE_LOCK:
        state = dict(M15_ZONE_STATE.get(symbol, {}))
        zones = [dict(x) for x in M15_ZONES.get(symbol, [])]

    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "candles": candles,
        "overlays": {
            "zones": zones,
            "order_blocks": [dict(x) for x in state.get("order_blocks", [])],
            "fvgs": [dict(x) for x in state.get("fvgs", [])],
            "bias": state.get("bias"),
            "bos": state.get("bos"),
            "bos_level": state.get("bos_level"),
            "macro_high": state.get("macro_high"),
            "macro_low": state.get("macro_low"),
            "updated_at": state.get("updated_at")
        },
        "generated_at": datetime.now(timezone.utc).isoformat()
    }


OWNER_CHART_HTML = """<!doctype html>
<html lang=\"fr\">
<head>
<meta charset=\"utf-8\">
<meta name=\"viewport\" content=\"width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no\">
<title>NOVA — Graphique marché</title>
<script src=\"https://unpkg.com/lightweight-charts@4.2.3/dist/lightweight-charts.standalone.production.js\"></script>
<style>
html,body{margin:0;width:100%;height:100%;background:#0b0e13;color:#d8dee9;font-family:Inter,Arial,sans-serif;overflow:hidden}
#top{height:54px;display:flex;align-items:center;gap:10px;padding:0 14px;background:#11151c;border-bottom:1px solid #252b35;box-sizing:border-box}
#title{font-weight:700;font-size:16px;margin-right:auto}.badge{font-size:12px;padding:5px 8px;border:1px solid #343b48;border-radius:6px;background:#171c24}.bias{font-weight:700}
#chart{position:absolute;left:0;right:0;top:54px;bottom:0}.legend{position:absolute;z-index:10;top:66px;left:12px;background:rgba(13,17,23,.88);border:1px solid #2a303a;border-radius:7px;padding:8px 10px;font-size:11px;line-height:1.65;pointer-events:none}.sw{display:inline-block;width:9px;height:9px;margin-right:5px;border-radius:2px}
</style>
</head>
<body>
<div id=\"top\"><div id=\"title\">NOVA — Graphique</div><div class=\"badge\" id=\"tf\"></div><div class=\"badge bias\" id=\"bias\"></div></div>
<div class=\"legend\"><span><i class=\"sw\" style=\"background:#3b82f6\"></i>Support</span> &nbsp; <span><i class=\"sw\" style=\"background:#ef4444\"></i>Résistance</span><br><span><i class=\"sw\" style=\"background:#a855f7\"></i>Order Block</span> &nbsp; <span><i class=\"sw\" style=\"background:#eab308\"></i>FVG</span></div>
<div id=\"chart\"></div>
<script>
const payload = __PAYLOAD__;
const root=document.getElementById('chart');
const chart=LightweightCharts.createChart(root,{layout:{background:{color:'#0b0e13'},textColor:'#b8c0cc'},grid:{vertLines:{color:'#171c24'},horzLines:{color:'#171c24'}},crosshair:{mode:LightweightCharts.CrosshairMode.Normal},rightPriceScale:{borderColor:'#303641'},timeScale:{borderColor:'#303641',timeVisible:true,secondsVisible:false},handleScroll:{mouseWheel:true,pressedMouseMove:true},handleScale:{mouseWheel:true,pinch:true,axisPressedMouseMove:true}});
const candles=chart.addCandlestickSeries({upColor:'#26a69a',downColor:'#ef5350',borderUpColor:'#26a69a',borderDownColor:'#ef5350',wickUpColor:'#26a69a',wickDownColor:'#ef5350'});
candles.setData(payload.candles);
function line(price,color,title){if(price===null||price===undefined)return;const s=chart.addLineSeries({color,lineWidth:1,lineStyle:LightweightCharts.LineStyle.Dashed,lastValueVisible:true,priceLineVisible:false,title});s.setData(payload.candles.map(c=>({time:c.time,value:price})));}
const o=payload.overlays||{};
line(o.macro_high,'#ef4444','M15 High');line(o.macro_low,'#3b82f6','M15 Low');line(o.bos_level,'#f59e0b','BOS');
const zoneLayer=document.createElement('div');zoneLayer.style.cssText='position:absolute;inset:0;pointer-events:none;z-index:5;';root.appendChild(zoneLayer);
const zoneDefs=[];
function zone(z,color,title){if(!z||z.zone_min===undefined||z.zone_max===undefined)return;zoneDefs.push({min:Number(z.zone_min),max:Number(z.zone_max),color,title});}
(o.zones||[]).forEach(z=>zone(z,z.level_type==='SUPPORT'?'rgba(59,130,246,.16)':'rgba(239,68,68,.16)',z.level_type));
(o.order_blocks||[]).forEach(z=>zone({zone_min:z.low,zone_max:z.high},'rgba(168,85,247,.18)',z.type));
(o.fvgs||[]).forEach(z=>zone({zone_min:z.lower,zone_max:z.upper},'rgba(234,179,8,.16)',z.type));
function drawZones(){zoneLayer.innerHTML='';zoneDefs.forEach(z=>{const a=candles.priceToCoordinate(z.max),b=candles.priceToCoordinate(z.min);if(a===null||b===null)return;const el=document.createElement('div');el.style.cssText=`position:absolute;left:0;right:0;top:${Math.min(a,b)}px;height:${Math.max(1,Math.abs(b-a))}px;background:${z.color};border-top:1px solid ${z.color.replace('.16', '.55').replace('.18','.55')};border-bottom:1px solid ${z.color.replace('.16', '.55').replace('.18','.55')};`;el.title=z.title;zoneLayer.appendChild(el);});}
chart.timeScale().subscribeVisibleTimeRangeChange(drawZones);
document.getElementById('title').textContent=`NOVA — ${payload.symbol}`;document.getElementById('tf').textContent=payload.timeframe.toUpperCase();document.getElementById('bias').textContent=o.bias?`Biais ${o.bias}`:'';
window.addEventListener('resize',()=>{chart.applyOptions({width:root.clientWidth,height:root.clientHeight});drawZones();});
chart.timeScale().fitContent();drawZones();
</script>
</body>
</html>"""


@app.route("/owner/chart")
def owner_chart():
    _remember_owner_chart_public_base_url_from_request()
    symbol = (request.args.get("symbol", "") or "").upper().strip()
    timeframe = (request.args.get("timeframe", "") or "").lower().strip()
    token = (request.args.get("token", "") or "").strip()

    if symbol not in SYMBOLS or timeframe not in {"1m", "5m", "15m"}:
        return "Accès graphique invalide.", 400
    if not _owner_chart_authorized(symbol, timeframe, token):
        return "Accès propriétaire refusé.", 403

    data = _owner_chart_data(symbol, timeframe)
    if data is None:
        return "Données de marché indisponibles pour ce graphique.", 503

    import json as _json
    html = OWNER_CHART_HTML.replace("__PAYLOAD__", _json.dumps(data, separators=(",", ":")))
    return Response(html, mimetype="text/html")


# ==========================================
# ROUTES WEB
# ==========================================

@app.route("/")
@app.route("/health")
def health_check():

    _remember_owner_chart_public_base_url_from_request()

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