import os
import time
import json
import logging
import threading
from datetime import datetime, timedelta

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

TELEGRAM_OWNER_ID = os.environ.get(
    "TELEGRAM_OWNER_ID",
    "5459538739"
).strip()

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

def ensure_trade_history_record(
    trade_id,
    trade
):
    """
    Enregistre un nouveau signal dans l'historique.

    L'historique est conservé même lorsque le trade
    est ensuite retiré de active_trades.json.
    """

    history = load_json(
        TRADE_HISTORY_FILE
    )

    if trade_id in history:
        return

    history[trade_id] = {
        "trade_id": trade_id,
        "symbol": trade.get(
            "symbol",
            "INCONNU"
        ),
        "direction": trade.get(
            "direction",
            "INCONNUE"
        ),
        "entry_price": trade.get(
            "entry_price"
        ),
        "initial_sl": trade.get(
            "initial_sl"
        ),
        "current_sl": trade.get(
            "current_sl"
        ),
        "tp1": trade.get(
            "tp1"
        ),
        "tp2": trade.get(
            "tp2"
        ),
        "tp3": trade.get(
            "tp3"
        ),
        "pattern": trade.get(
            "pattern"
        ),
        "filter_score": trade.get(
            "filter_score"
        ),
        "indicators": trade.get(
            "indicators",
            {}
        ),
        "created_at": trade.get(
            "created_at",
            utc_now_iso()
        ),
        "status": "ACTIVE",
        "result": None,
        "closed_at": None,
        "close_price": None,
        "events": []
    }

    save_json(
        TRADE_HISTORY_FILE,
        history
    )

def record_trade_event(
    trade_id,
    event,
    price=None
):
    """
    Enregistre un événement du cycle de vie du signal.
    """

    history = load_json(
        TRADE_HISTORY_FILE
    )

    if trade_id not in history:
        return

    event_data = {
        "event": event,
        "timestamp": utc_now_iso()
    }

    if price is not None:
        event_data["price"] = price

    history[trade_id].setdefault(
        "events",
        []
    )

    history[trade_id]["events"].append(
        event_data
    )

    save_json(
        TRADE_HISTORY_FILE,
        history
    )

def close_trade_in_history(
    trade_id,
    result,
    close_price
):
    """
    Enregistre définitivement le résultat d'un trade.
    """

    history = load_json(
        TRADE_HISTORY_FILE
    )

    if trade_id not in history:
        logging.warning(
            f"Historique absent pour "
            f"le trade {trade_id}."
        )
        return

    history[trade_id]["status"] = "CLOSED"
    history[trade_id]["result"] = result
    history[trade_id]["close_price"] = close_price
    history[trade_id]["closed_at"] = utc_now_iso()

    history[trade_id].setdefault(
        "events",
        []
    )

    history[trade_id]["events"].append({
        "event": result,
        "timestamp": utc_now_iso(),
        "price": close_price
    })

    save_json(
        TRADE_HISTORY_FILE,
        history
    )

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

    history = load_json(
        TRADE_HISTORY_FILE
    )

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

    for trade_id, trade in history.items():

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

                if direction == "BUY":

                    statistics[
                        "buy_signals"
                    ] += 1

                elif direction == "SELL":

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
# PERSISTANCE JSON
# ==========================================

def load_json(filename):

    if not os.path.exists(filename):

        return {}

    try:

        with open(
            filename,
            "r",
            encoding="utf-8"
        ) as f:

            data = json.load(f)

        if isinstance(
            data,
            dict
        ):

            return data

        logging.warning(
            f"Le fichier {filename} ne contient "
            f"pas un objet JSON valide."
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

    try:

        with open(
            filename,
            "w",
            encoding="utf-8"
        ) as f:

            json.dump(
                data,
                f,
                indent=4,
                ensure_ascii=False
            )

    except Exception as e:

        logging.error(
            f"Impossible de sauvegarder "
            f"{filename}: {e}"
        )

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
# CRÉATION D'UNE OPPORTUNITÉ M15
# ==========================================

def create_pending_opportunity(
    symbol,
    direction,
    pattern,
    entry,
    sl,
    tp1,
    tp2,
    tp3,
    filter_score,
    indicators,
    zone,
    candle_id
):
    """
    Enregistre l'opportunité M15 pour qu'elle soit
    ensuite surveillée par M5.

    Cette étape remplace uniquement la création
    immédiate du trade qui existait auparavant.
    """

    opportunities = load_json(
        OPPORTUNITIES_FILE
    )

    candidate_id = (
        f"OPP_{direction}_"
        f"{symbol}_"
        f"{int(time.time())}"
    )

    opportunities[candidate_id] = {
        "candidate_id": candidate_id,
        "symbol": symbol,
        "direction": direction,
        "pattern": pattern,
        "entry_price": entry,
        "initial_sl": sl,
        "current_sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "filter_score": filter_score,
        "indicators": indicators,
        "zone": zone,
        "m15_candle_id": candle_id,
        "created_at": datetime.utcnow().isoformat(),
        "status": "M5_SURVEILLANCE"
    }

    save_json(
        OPPORTUNITIES_FILE,
        opportunities
    )

    logging.info(
        f"Opportunité {direction} créée pour "
        f"{symbol} : {candidate_id}. "
        f"Surveillance M5 activée."
    )

    return candidate_id

# ==========================================
# ANALYSE M15
# ==========================================

def scan_market_m15(
    symbol
):

    logging.info(
        f"Vérification des conditions "
        f"de marché M15 pour {symbol}..."
    )

    try:

        supports, resistances = (
            get_h1_zones(symbol)
        )

        df_m15 = fetch_biquote_ohlcv(
            symbol,
            timeframe="15m",
            count=100
        )

        if df_m15.empty:

            logging.warning(
                f"Aucune donnée M15 "
                f"disponible pour {symbol}."
            )

            return

        if len(df_m15) < 50:

            logging.warning(
                f"Pas assez de données M15 "
                f"pour les indicateurs de {symbol}."
            )

            return

        df_m15 = calculate_indicators(
            df_m15
        )

        market_filter = (
            evaluate_market_filter(
                df_m15
            )
        )

        if market_filter is None:

            logging.warning(
                f"Contexte indicateurs incomplet "
                f"pour {symbol}."
            )

            return

        last_candle = (
            df_m15.iloc[-2]
        )

        candle_timestamp = (
            last_candle["timestamp"]
        )

        candle_id = (
            f"{symbol}_M15_"
            f"{candle_timestamp}"
        )

        processed_signals = load_json(
            SIGNALS_FILE
        )

        if candle_id in processed_signals:

            logging.info(
                f"Bougie {candle_id} "
                f"déjà traitée."
            )

            return

        pattern = detect_patterns(
            df_m15
        )

        if not pattern:

            logging.info(
                f"Aucun pattern M15 "
                f"exploitable pour {symbol}."
            )

            return

        # ======================================
        # BUY
        # ======================================

        if pattern in [
            "AVALEMENT_HAUSSIER",
            "MARTEAU"
        ]:

            if market_filter[
                "buy_score"
            ] < 2:

                logging.info(
                    f"BUY écarté par le filtre "
                    f"contextuel {symbol}: "
                    f"{market_filter['buy_score']}/4."
                )

                return

            for sup in supports:

                if (
                    float(
                        last_candle["low"]
                    )
                    <= sup["high_band"]
                    and
                    float(
                        last_candle["close"]
                    )
                    >= sup["low_band"]
                ):

                    entry = float(
                        last_candle["close"]
                    )

                    sl = (
                        sup["low_band"]
                        - (
                            entry
                            * 0.001
                        )
                    )

                    risk = (
                        entry
                        - sl
                    )

                    if risk <= 0:
                        continue

                    tp1 = (
                        entry
                        + risk
                    )

                    tp2 = (
                        entry
                        + (
                            risk
                            * 2
                        )
                    )

                    tp3 = (
                        entry
                        + (
                            risk
                            * 3
                        )
                    )

                    candidate_id = (
                        create_pending_opportunity(
                            symbol=symbol,
                            direction="BUY",
                            pattern=pattern,
                            entry=entry,
                            sl=sl,
                            tp1=tp1,
                            tp2=tp2,
                            tp3=tp3,
                            filter_score=(
                                market_filter[
                                    "buy_score"
                                ]
                            ),
                            indicators={
                                "ema20": (
                                    market_filter[
                                        "ema20"
                                    ]
                                ),
                                "ema50": (
                                    market_filter[
                                        "ema50"
                                    ]
                                ),
                                "rsi14": (
                                    market_filter[
                                        "rsi14"
                                    ]
                                ),
                                "atr14": (
                                    market_filter[
                                        "atr14"
                                    ]
                                ),
                                "adx14": (
                                    market_filter[
                                        "adx14"
                                    ]
                                )
                            },
                            zone=sup,
                            candle_id=candle_id
                        )
                    )

                    processed_signals[
                        candle_id
                    ] = True

                    save_json(
                        SIGNALS_FILE,
                        processed_signals
                    )

                    logging.info(
                        f"Opportunité BUY M15 créée : "
                        f"{candidate_id}. "
                        f"Le signal n'est pas encore envoyé."
                    )

                    break

        # ======================================
        # SELL
        # ======================================

        elif pattern in [
            "AVALEMENT_BAISSIER",
            "ETOILE_FILANTE"
        ]:

            if market_filter[
                "sell_score"
            ] < 2:

                logging.info(
                    f"SELL écarté par le filtre "
                    f"contextuel {symbol}: "
                    f"{market_filter['sell_score']}/4."
                )

                return

            for res in resistances:

                if (
                    float(
                        last_candle["high"]
                    )
                    >= res["low_band"]
                    and
                    float(
                        last_candle["close"]
                    )
                    <= res["high_band"]
                ):

                    entry = float(
                        last_candle["close"]
                    )

                    sl = (
                        res["high_band"]
                        + (
                            entry
                            * 0.001
                        )
                    )

                    risk = (
                        sl
                        - entry
                    )

                    if risk <= 0:
                        continue

                    tp1 = (
                        entry
                        - risk
                    )

                    tp2 = (
                        entry
                        - (
                            risk
                            * 2
                        )
                    )

                    tp3 = (
                        entry
                        - (
                            risk
                            * 3
                        )
                    )

                    candidate_id = (
                        create_pending_opportunity(
                            symbol=symbol,
                            direction="SELL",
                            pattern=pattern,
                            entry=entry,
                            sl=sl,
                            tp1=tp1,
                            tp2=tp2,
                            tp3=tp3,
                            filter_score=(
                                market_filter[
                                    "sell_score"
                                ]
                            ),
                            indicators={
                                "ema20": (
                                    market_filter[
                                        "ema20"
                                    ]
                                ),
                                "ema50": (
                                    market_filter[
                                        "ema50"
                                    ]
                                ),
                                "rsi14": (
                                    market_filter[
                                        "rsi14"
                                    ]
                                ),
                                "atr14": (
                                    market_filter[
                                        "atr14"
                                    ]
                                ),
                                "adx14": (
                                    market_filter[
                                        "adx14"
                                    ]
                                )
                            },
                            zone=res,
                            candle_id=candle_id
                        )
                    )

                    processed_signals[
                        candle_id
                    ] = True

                    save_json(
                        SIGNALS_FILE,
                        processed_signals
                    )

                    logging.info(
                        f"Opportunité SELL M15 créée : "
                        f"{candidate_id}. "
                        f"Le signal n'est pas encore envoyé."
                    )

                    break

    except Exception as e:

        logging.exception(
            f"Erreur scan_market_m15 "
            f"{symbol}: {e}"
        )

# ==========================================
# CONFIRMATION M5
# ==========================================

def evaluate_m5_opportunity(
    opportunity
):
    """
    Surveille une opportunité M15 avec M5.

    M5 ne recalcule pas la stratégie M15 :
    il sert uniquement à surveiller le moment où
    l'opportunité devient exploitable.

    Une bougie M5 clôturée dans le même sens que
    l'opportunité constitue la confirmation M5.
    """

    symbol = opportunity.get(
        "symbol"
    )

    direction = opportunity.get(
        "direction"
    )

    entry = float(
        opportunity.get(
            "entry_price"
        )
    )

    initial_sl = float(
        opportunity.get(
            "initial_sl"
        )
    )

    df_m5 = fetch_biquote_ohlcv(
        symbol,
        timeframe="5m",
        count=100
    )

    if df_m5.empty or len(df_m5) < 4:

        logging.warning(
            f"Données M5 insuffisantes "
            f"pour {symbol}."
        )

        return "WAIT", None

    last_m5 = df_m5.iloc[-2]

    m5_open = float(
        last_m5["open"]
    )

    m5_close = float(
        last_m5["close"]
    )

    m5_high = float(
        last_m5["high"]
    )

    m5_low = float(
        last_m5["low"]
    )

    # ======================================
    # INVALIDATION PAR LE SL
    # ======================================

    if direction == "BUY":

        if m5_low <= initial_sl:

            return "INVALIDATED", None

        m5_direction_ok = (
            m5_close > m5_open
            and
            m5_close >= entry
        )

    else:

        if m5_high >= initial_sl:

            return "INVALIDATED", None

        m5_direction_ok = (
            m5_close < m5_open
            and
            m5_close <= entry
        )

    if not m5_direction_ok:

        return "WAIT", None

    m5_pattern = detect_patterns(
        df_m5
    )

    if direction == "BUY":

        pattern_ok = m5_pattern in [
            "AVALEMENT_HAUSSIER",
            "MARTEAU"
        ]

    else:

        pattern_ok = m5_pattern in [
            "AVALEMENT_BAISSIER",
            "ETOILE_FILANTE"
        ]

    # ======================================
    # M5 CONFIRMÉ
    # ======================================

    confirmation = {
        "m5_pattern": m5_pattern,
        "m5_open": m5_open,
        "m5_close": m5_close,
        "m5_high": m5_high,
        "m5_low": m5_low,
        "pattern_ok": pattern_ok
    }

    return "CONFIRMED", confirmation

# ==========================================
# VÉRIFICATION M1 SI NÉCESSAIRE
# ==========================================

def evaluate_m1_if_necessary(
    opportunity,
    m5_confirmation
):
    """
    M1 est utilisé uniquement lorsque le mouvement
    M5 est déjà confirmé mais que le prix s'est éloigné
    de manière importante du prix de référence M15.

    Si le M5 est proche de l'entrée, M1 n'est pas
    nécessaire.

    Lorsque M1 est utilisé, une confirmation dans le
    même sens permet de finaliser le signal.
    """

    symbol = opportunity.get(
        "symbol"
    )

    direction = opportunity.get(
        "direction"
    )

    entry = float(
        opportunity.get(
            "entry_price"
        )
    )

    m5_close = float(
        m5_confirmation.get(
            "m5_close"
        )
    )

    df_m5 = fetch_biquote_ohlcv(
        symbol,
        timeframe="5m",
        count=100
    )

    if df_m5.empty or len(df_m5) < 20:

        return True, False

    df_m5_indicators = calculate_indicators(
        df_m5
    )

    last_m5 = df_m5_indicators.iloc[-2]

    atr14 = last_m5.get(
        "atr14"
    )

    if pd.isna(atr14) or float(atr14) <= 0:

        return True, False

    atr14 = float(atr14)

    distance = abs(
        m5_close
        - entry
    )

    # M1 n'est nécessaire que si le prix s'est
    # éloigné de plus de 0.25 ATR M5.
    m1_needed = (
        distance
        > (
            atr14
            * 0.25
        )
    )

    if not m1_needed:

        return True, False

    logging.info(
        f"M1 nécessaire pour {symbol} "
        f"après confirmation M5."
    )

    df_m1 = fetch_biquote_ohlcv(
        symbol,
        timeframe="1m",
        count=100
    )

    if df_m1.empty or len(df_m1) < 4:

        logging.warning(
            f"Données M1 insuffisantes "
            f"pour {symbol}. "
            f"Surveillance maintenue."
        )

        return False, True

    m1_pattern = detect_patterns(
        df_m1
    )

    if direction == "BUY":

        m1_ok = m1_pattern in [
            "AVALEMENT_HAUSSIER",
            "MARTEAU"
        ]

    else:

        m1_ok = m1_pattern in [
            "AVALEMENT_BAISSIER",
            "ETOILE_FILANTE"
        ]

    if not m1_ok:

        logging.info(
            f"M1 ne confirme pas encore "
            f"{direction} pour {symbol}."
        )

        return False, True

    logging.info(
        f"M1 confirme {direction} pour {symbol}."
    )

    return True, True

# ==========================================
# TRANSFORMATION DE L'OPPORTUNITÉ
# EN SIGNAL FINAL
# ==========================================

def finalize_pending_opportunity(
    candidate_id,
    opportunity
):
    """
    Transforme une opportunité M15 confirmée par M5/M1
    en trade actif.

    Les valeurs Entry / SL / TP sont reprises
    exactement de l'opportunité M15.
    """

    opportunities = load_json(
        OPPORTUNITIES_FILE
    )

    active_trades = load_json(
        TRADES_FILE
    )

    if candidate_id not in opportunities:

        return False

    direction = opportunity.get(
        "direction"
    )

    symbol = opportunity.get(
        "symbol"
    )

    entry = float(
        opportunity.get(
            "entry_price"
        )
    )

    sl = float(
        opportunity.get(
            "initial_sl"
        )
    )

    tp1 = float(
        opportunity.get(
            "tp1"
        )
    )

    tp2 = float(
        opportunity.get(
            "tp2"
        )
    )

    tp3 = float(
        opportunity.get(
            "tp3"
        )
    )

    pattern = opportunity.get(
        "pattern"
    )

    filter_score = opportunity.get(
        "filter_score"
    )

    indicators = opportunity.get(
        "indicators",
        {}
    )

    trade_id = (
        f"TRADE_{direction}_"
        f"{symbol}_"
        f"{int(time.time())}"
    )

    active_trades[
        trade_id
    ] = {

        "symbol": symbol,

        "direction": direction,

        "entry_price": entry,

        "initial_sl": sl,

        "current_sl": sl,

        "tp1": tp1,

        "tp2": tp2,

        "tp3": tp3,

        "status": "ACTIVE",

        "pattern": pattern,

        "filter_score": filter_score,

        "indicators": indicators,

        "created_at": (
            datetime.utcnow()
            .isoformat()
        )
    }

    save_json(
        TRADES_FILE,
        active_trades
    )

    ensure_trade_history_record(
        trade_id,
        active_trades[
            trade_id
        ]
    )

    if direction == "BUY":

        send_telegram_message(
            f"🟢 *SIGNAL ACHAT (BUY) "
            f"via BIQUOTE*\n"
            f"Actif: {symbol}\n"
            f"Motif: {pattern}\n"
            f"Confirmation: M15 → M5"
            f" → M1 si nécessaire\n"
            f"Filtre: "
            f"{filter_score}/4\n"
            f"EMA20/50: "
            f"{indicators.get('ema20', 0):.5f} / "
            f"{indicators.get('ema50', 0):.5f}\n"
            f"RSI14: "
            f"{indicators.get('rsi14', 0):.2f}\n"
            f"ATR14: "
            f"{indicators.get('atr14', 0):.5f}\n"
            f"ADX14: "
            f"{indicators.get('adx14', 0):.2f}\n"
            f"Entrée: {entry:.2f}\n"
            f"SL Initial: {sl:.2f}\n"
            f"TP1: "
            f"{tp1:.2f}\n"
            f"TP2: "
            f"{tp2:.2f}\n"
            f"TP3: "
            f"{tp3:.2f}"
        )

    else:

        send_telegram_message(
            f"🔴 *SIGNAL VENTE "
            f"(SHORT) via BIQUOTE*\n"
            f"Actif: {symbol}\n"
            f"Motif: {pattern}\n"
            f"Confirmation: M15 → M5"
            f" → M1 si nécessaire\n"
            f"Filtre: "
            f"{filter_score}/4\n"
            f"EMA20/50: "
            f"{indicators.get('ema20', 0):.5f} / "
            f"{indicators.get('ema50', 0):.5f}\n"
            f"RSI14: "
            f"{indicators.get('rsi14', 0):.2f}\n"
            f"ATR14: "
            f"{indicators.get('atr14', 0):.5f}\n"
            f"ADX14: "
            f"{indicators.get('adx14', 0):.2f}\n"
            f"Entrée: {entry:.2f}\n"
            f"SL Initial: {sl:.2f}\n"
            f"TP1: "
            f"{tp1:.2f}\n"
            f"TP2: "
            f"{tp2:.2f}\n"
            f"TP3: "
            f"{tp3:.2f}"
        )

    opportunities.pop(
        candidate_id,
        None
    )

    save_json(
        OPPORTUNITIES_FILE,
        opportunities
    )

    logging.info(
        f"{direction} final créé : "
        f"{trade_id} pour {symbol}."
    )

    return True

# ==========================================
# SURVEILLANCE DES OPPORTUNITÉS M5
# ==========================================

def scan_pending_opportunities_m5():
    """
    Parcourt les opportunités détectées par M15.

    Flux :
        M15 → opportunité
             ↓
        M5 surveillance
             ↓
        M1 si nécessaire
             ↓
        signal final
    """

    opportunities = load_json(
        OPPORTUNITIES_FILE
    )

    if not opportunities:

        return

    opportunities_to_delete = []

    for candidate_id, opportunity in list(
        opportunities.items()
    ):

        try:

            if opportunity.get(
                "status"
            ) != "M5_SURVEILLANCE":

                continue

            symbol = opportunity.get(
                "symbol"
            )

            direction = opportunity.get(
                "direction"
            )

            result, m5_confirmation = (
                evaluate_m5_opportunity(
                    opportunity
                )
            )

            if result == "INVALIDATED":

                logging.info(
                    f"Opportunité {candidate_id} "
                    f"invalide par le M5."
                )

                opportunities_to_delete.append(
                    candidate_id
                )

                continue

            if result != "CONFIRMED":

                continue

            # ==================================
            # M5 CONFIRMÉ
            # ==================================

            opportunity[
                "status"
            ] = "M5_CONFIRMED"

            opportunity[
                "m5_confirmation"
            ] = m5_confirmation

            should_finalize, m1_used = (
                evaluate_m1_if_necessary(
                    opportunity,
                    m5_confirmation
                )
            )

            if not should_finalize:

                opportunity[
                    "status"
                ] = "M5_SURVEILLANCE"

                opportunities[
                    candidate_id
                ] = opportunity

                if m1_used:

                    logging.info(
                        f"{symbol} {direction} : "
                        f"M5 confirmé, M1 en attente."
                    )

                continue

            # ==================================
            # SIGNAL FINAL
            # ==================================

            finalize_pending_opportunity(
                candidate_id,
                opportunity
            )

            opportunities_to_delete.append(
                candidate_id
            )

        except Exception as e:

            logging.exception(
                f"Erreur surveillance M5 "
                f"{candidate_id}: {e}"
            )

    for candidate_id in opportunities_to_delete:

        opportunities.pop(
            candidate_id,
            None
        )

    save_json(
        OPPORTUNITIES_FILE,
        opportunities
    )

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

            symbols_in_trades = set()

            for trade in active_trades.values():

                symbol = trade.get(
                    "symbol"
                )

                if symbol:

                    symbols_in_trades.add(
                        symbol
                    )

            for symbol in symbols_in_trades:

                prices[symbol] = (
                    fetch_biquote_live_price(
                        symbol
                    )
                )

            trades_to_delete = []

            for t_id, trade in list(
                active_trades.items()
            ):

                try:

                    symbol = trade.get(
                        "symbol"
                    )

                    if not symbol:
                        continue

                    current_price = (
                        prices.get(
                            symbol
                        )
                    )

                    if current_price is None:
                        continue

                    direction = trade[
                        "direction"
                    ]

                    entry = float(
                        trade["entry_price"]
                    )

                    current_sl = float(
                        trade["current_sl"]
                    )

                    tp1 = float(
                        trade["tp1"]
                    )

                    tp2 = float(
                        trade["tp2"]
                    )

                    tp3 = float(
                        trade["tp3"]
                    )

                    status = trade.get(
                        "status",
                        "ACTIVE"
                    )

                    if direction == "BUY":

                        if current_price <= current_sl:

                            send_telegram_message(
                                f"❌ *SL Touché* sur "
                                f"{symbol} "
                                f"à {current_price:.2f}.\n"
                                f"Trade clos."
                            )

                            close_trade_in_history(
                                t_id,
                                "SL",
                                current_price
                            )

                            trades_to_delete.append(
                                t_id
                            )

                        elif (
                            current_price >= tp1
                            and status == "ACTIVE"
                        ):

                            trade["status"] = (
                                "TP1_HIT"
                            )

                            trade["current_sl"] = (
                                entry
                            )

                            record_trade_event(
                                t_id,
                                "TP1_HIT",
                                tp1
                            )

                            record_trade_event(
                                t_id,
                                "BREAK_EVEN",
                                entry
                            )

                            send_telegram_message(
                                f"🎯 *TP1 Atteint* "
                                f"sur {symbol} "
                                f"({tp1:.2f}) !\n"
                                f"🛡️ SL déplacé au "
                                f"Break-Even."
                            )

                        elif (
                            current_price >= tp2
                            and status == "TP1_HIT"
                        ):

                            trade["status"] = (
                                "TP2_HIT"
                            )

                            trade["current_sl"] = (
                                tp1
                            )

                            record_trade_event(
                                t_id,
                                "TP2_HIT",
                                tp2
                            )

                            send_telegram_message(
                                f"🎯🎯 *TP2 Atteint* "
                                f"sur {symbol} "
                                f"({tp2:.2f}) !\n"
                                f"🔒 SL déplacé au "
                                f"niveau du TP1."
                            )

                        elif current_price >= tp3:

                            record_trade_event(
                                t_id,
                                "TP3_HIT",
                                tp3
                            )

                            close_trade_in_history(
                                t_id,
                                "TP3",
                                current_price
                            )

                            send_telegram_message(
                                f"🏆 *TP3 Atteint* "
                                f"sur {symbol} "
                                f"({tp3:.2f}).\n"
                                f"Trade terminé."
                            )

                            trades_to_delete.append(
                                t_id
                            )

                    elif direction == "SELL":

                        if current_price >= current_sl:

                            send_telegram_message(
                                f"❌ *SL Touché* "
                                f"(Short) sur "
                                f"{symbol} "
                                f"à {current_price:.2f}.\n"
                                f"Trade clos."
                            )

                            close_trade_in_history(
                                t_id,
                                "SL",
                                current_price
                            )

                            trades_to_delete.append(
                                t_id
                            )

                        elif (
                            current_price <= tp1
                            and status == "ACTIVE"
                        ):

                            trade["status"] = (
                                "TP1_HIT"
                            )

                            trade["current_sl"] = (
                                entry
                            )

                            record_trade_event(
                                t_id,
                                "TP1_HIT",
                                tp1
                            )

                            record_trade_event(
                                t_id,
                                "BREAK_EVEN",
                                entry
                            )

                            send_telegram_message(
                                f"🎯 *TP1 Atteint* "
                                f"(Short) sur "
                                f"{symbol} "
                                f"({tp1:.2f}) !\n"
                                f"🛡️ SL déplacé au "
                                f"Break-Even."
                            )

                        elif (
                            current_price <= tp2
                            and status == "TP1_HIT"
                        ):

                            trade["status"] = (
                                "TP2_HIT"
                            )

                            trade["current_sl"] = (
                                tp1
                            )

                            record_trade_event(
                                t_id,
                                "TP2_HIT",
                                tp2
                            )

                            send_telegram_message(
                                f"🎯🎯 *TP2 Atteint* "
                                f"(Short) sur "
                                f"{symbol} "
                                f"({tp2:.2f}) !\n"
                                f"🔒 SL déplacé au "
                                f"niveau du TP1."
                            )

                        elif current_price <= tp3:

                            record_trade_event(
                                t_id,
                                "TP3_HIT",
                                tp3
                            )

                            close_trade_in_history(
                                t_id,
                                "TP3",
                                current_price
                            )

                            send_telegram_message(
                                f"🏆 *TP3 Atteint* "
                                f"sur {symbol} "
                                f"({tp3:.2f}).\n"
                                f"Trade terminé."
                            )

                            trades_to_delete.append(
                                t_id
                            )

                except Exception as e:

                    logging.error(
                        f"Erreur traitement trade "
                        f"{t_id}: {e}"
                    )

            for t_id in trades_to_delete:

                active_trades.pop(
                    t_id,
                    None
                )

            save_json(
                TRADES_FILE,
                active_trades
            )

        except Exception as e:

            logging.exception(
                f"Erreur dans le suivi "
                f"temps réel : {e}"
            )

        time.sleep(10)

# ==========================================
# PLANIFICATEUR MULTI-TIMEFRAME
# ==========================================

def main_scheduler():

    logging.info(
        "Thread scheduler multi-timeframe démarré."
    )

    last_m15_slot = None
    last_m5_slot = None

    while True:

        try:

            now = datetime.now()

            # ==================================
            # CRÉNEAU M15
            # ==================================

            m15_slot = now.replace(
                minute=(now.minute // 15) * 15,
                second=0,
                microsecond=0
            )

            if (
                now.second >= 5
                and
                m15_slot != last_m15_slot
            ):

                logging.info(
                    "Nouveau créneau M15 : "
                    "analyse des opportunités."
                )

                for symbol in SYMBOLS:

                    scan_market_m15(
                        symbol
                    )

                last_m15_slot = m15_slot

                logging.info(
                    "Analyse M15 terminée."
                )

            # ==================================
            # CRÉNEAU M5
            # ==================================

            m5_slot = now.replace(
                minute=(now.minute // 5) * 5,
                second=0,
                microsecond=0
            )

            if (
                now.second >= 5
                and
                m5_slot != last_m5_slot
            ):

                logging.info(
                    "Nouveau créneau M5 : "
                    "surveillance des opportunités."
                )

                scan_pending_opportunities_m5()

                last_m5_slot = m5_slot

                logging.info(
                    "Surveillance M5 terminée."
                )

            time.sleep(1)

        except Exception as e:

            logging.exception(
                f"Erreur scheduler multi-timeframe : {e}"
            )

            time.sleep(10)

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