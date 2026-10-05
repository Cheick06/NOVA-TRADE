import os
import time
import json
import logging
import threading
import csv
import io
import re
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from flask import Flask, request, redirect, url_for, session, render_template_string
from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import generate_password_hash, check_password_hash
from sqlalchemy import func
import secrets
import smtplib
from email.message import EmailMessage
import hmac
import hashlib


# ============================================================
# CONFIGURATION
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Stratégies activables indépendamment.
STRAT_REVERSAL = True
STRAT_PULLBACK = True
STRAT_BREAKOUT = True

# Module d'analyse fondamentale des annonces USD.
TRADE_NEWS = True
FUNDAMENTAL_SYMBOLS = ["BTCUSD"]
FUNDAMENTAL_POLL_SECONDS = 1
FUNDAMENTAL_CALENDAR_REFRESH_SECONDS = 6 * 60 * 60
FUNDAMENTAL_TRIGGER_WINDOW_SECONDS = 5

# Le cahier des charges fourni spécifie BTCUSD.
SYMBOLS = ["BTCUSD", "XAUUSD", "EURUSD", "GBPUSD"]

# Telegram :
# - TELEGRAM_CHANNEL_ID = uniquement les communications de trading.
# - TELEGRAM_OWNER_ID = uniquement l'interface personnelle.
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "").strip()
TELEGRAM_CHANNEL_ID = os.environ.get("TELEGRAM_CHANNEL_ID", "").strip()
TELEGRAM_OWNER_ID = "5459538739"
TELEGRAM_BOT_USERNAME = os.environ.get("TELEGRAM_BOT_USERNAME", "").strip().lstrip("@")
TELEGRAM_VIP_LINK = os.environ.get("TELEGRAM_VIP_LINK", "").strip()

# BiQuote
BIQUOTE_BASE_URL = os.environ.get(
    "BIQUOTE_BASE_URL",
    "https://biquote.io/api",
).strip().rstrip("/")

# Persistance
TRADE_HISTORY_FILE = "trade_history.json"
WEEKLY_REPORTS_FILE = "weekly_reports.json"

# SaaS / abonnements
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = "postgresql+psycopg://" + DATABASE_URL[len("postgres://"): ]
elif DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = "postgresql+psycopg://" + DATABASE_URL[len("postgresql://"): ]
SUBSCRIPTION_PRICE_XOF = int(os.environ.get("SUBSCRIPTION_PRICE_XOF", "0"))
APP_BASE_URL = os.environ.get("APP_BASE_URL", "").strip().rstrip("/")
OWNER_GMAIL = os.environ.get("OWNER_GMAIL", "").strip()
GMAIL_APP_PASSWORD = os.environ.get("GMAIL_APP_PASSWORD", "").strip()
GMAIL_SMTP_HOST = os.environ.get("GMAIL_SMTP_HOST", "smtp.gmail.com").strip()
GMAIL_SMTP_PORT = int(os.environ.get("GMAIL_SMTP_PORT", "465"))
MT_API_KEY = os.environ.get("MT_API_KEY", "").strip()
FEDAPAY_API_KEY = os.environ.get("FEDAPAY_API_KEY", "").strip()
FEDAPAY_WEBHOOK_SECRET = os.environ.get("FEDAPAY_WEBHOOK_SECRET", "").strip()
FEDAPAY_ENV = os.environ.get("FEDAPAY_ENV", "live").strip().lower()
FEDAPAY_API_BASE = (
    "https://sandbox-api.fedapay.com/v1"
    if FEDAPAY_ENV == "sandbox"
    else "https://api.fedapay.com/v1"
)
NOWPAYMENTS_API_KEY = os.environ.get("NOWPAYMENTS_API_KEY", "").strip()
NOWPAYMENTS_IPN_SECRET = os.environ.get("NOWPAYMENTS_IPN_SECRET", "").strip()
NOWPAYMENTS_API_BASE = "https://api.nowpayments.io/v1"
NOWPAYMENTS_PAY_CURRENCY = os.environ.get("NOWPAYMENTS_PAY_CURRENCY", "usdttrc20").strip().lower()

# Paramètres Price Action
PIVOT_WINDOW = 5
ATR_PERIOD = 14
ZONE_ATR_MULTIPLIER = 0.25
SL_ATR_MULTIPLIER = 0.50
WICK_MIN_RATIO = 0.60
BREAKOUT_BODY_MIN_RATIO = 0.70
H1_RANGE_BARS = 24

# Etat des threads.
_threads_started = False
_threads_lock = threading.Lock()

_fundamental_events = {}
_fundamental_events_lock = threading.RLock()
_fundamental_last_calendar_fetch = 0.0

# Verrou d'état partagé entre scheduler / tracker / Telegram.
_state_lock = threading.RLock()

# Moteur hybride M15 -> M5 -> M1.
M15_STRUCTURE_BARS = 300
M5_SENTINEL_BARS = 120
M1_TRIGGER_BARS = 80
M1_VOLUME_AVG_PERIOD = 10
M1_VOLUME_CLIMAX_MULTIPLIER = 2.5
M1_REJECTION_WICK_MIN_RATIO = 0.50
M1_MONITOR_START_MINUTE = 10
M1_MONITOR_END_MINUTE = 13
M1_SCAN_INTERVAL_SECONDS = 10

# Etat local de la sentinelle. La décision anti-doublon reste PostgreSQL.
_monitoring_state = {
    symbol: {
        "active_monitoring": False,
        "slot_key": None,
        "expires_at": None,
        "direction": None,
        "zone": None,
        "structure": None,
        "last_m1_scan": 0.0,
    }
    for symbol in SYMBOLS
}

def utc_now():
    return datetime.now(timezone.utc)


def utc_now_iso():
    return utc_now().isoformat()


app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "").strip() or secrets.token_hex(32)
app.config["SQLALCHEMY_DATABASE_URI"] = DATABASE_URL or "sqlite:///nova_trade_local.db"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)

class User(db.Model):
    __tablename__ = "users"
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(255), unique=True, nullable=False, index=True)
    password_hash = db.Column(db.String(255), nullable=False)
    date_inscription = db.Column(db.DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
    status_abonnement = db.Column(db.String(20), nullable=False, default="TRIAL", index=True)
    date_expiration_abonnement = db.Column(db.DateTime(timezone=True), nullable=False)
    telegram_user_id = db.Column(db.BigInteger, nullable=True, unique=True)
    telegram_link_token = db.Column(db.String(128), unique=True, nullable=False, default=lambda: secrets.token_urlsafe(32))
    last_j7_notice = db.Column(db.DateTime(timezone=True), nullable=True)
    last_j1_notice = db.Column(db.DateTime(timezone=True), nullable=True)

class ActiveTrade(db.Model):
    __tablename__ = "active_trades"

    id = db.Column(db.String(128), primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=True, index=True)
    symbol = db.Column(db.String(20), nullable=False, index=True)
    direction = db.Column(db.String(10), nullable=False)
    entry_price = db.Column(db.Float, nullable=False)
    initial_sl = db.Column(db.Float, nullable=False)
    current_sl = db.Column(db.Float, nullable=False)
    tp1 = db.Column(db.Float, nullable=False)
    tp2 = db.Column(db.Float, nullable=False)
    tp3 = db.Column(db.Float, nullable=False)
    status = db.Column(db.String(20), nullable=False, default="ACTIVE", index=True)


class ProcessedSignal(db.Model):
    __tablename__ = "processed_signals"

    id = db.Column(db.Integer, primary_key=True)
    signal_key = db.Column(db.String(255), nullable=False, unique=True, index=True)
    date_detection = db.Column(db.DateTime(timezone=True), nullable=False, default=utc_now)


class Payment(db.Model):
    __tablename__ = "payments"
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False, index=True)
    provider = db.Column(db.String(30), nullable=False)
    external_id = db.Column(db.String(255), nullable=True, index=True)
    amount = db.Column(db.Numeric(18, 2), nullable=False)
    currency = db.Column(db.String(20), nullable=False)
    status = db.Column(db.String(30), nullable=False, default="pending")
    created_at = db.Column(db.DateTime(timezone=True), nullable=False, default=utc_now)
    paid_at = db.Column(db.DateTime(timezone=True), nullable=True)

with app.app_context():
    db.create_all()


# ============================================================
# UTILITAIRES TEMPS / JSON
# ============================================================

def load_json(filename):
    with _state_lock:
        try:
            if not os.path.exists(filename):
                return {}
            with open(filename, "r", encoding="utf-8") as file:
                data = json.load(file)
            return data if isinstance(data, dict) else {}
        except (OSError, json.JSONDecodeError) as exc:
            logging.error("Lecture JSON impossible %s : %s", filename, exc)
            return {}


def save_json(filename, data):
    with _state_lock:
        temp_filename = f"{filename}.tmp"
        try:
            with open(temp_filename, "w", encoding="utf-8") as file:
                json.dump(data, file, ensure_ascii=False, indent=2)
            os.replace(temp_filename, filename)
            return True
        except OSError as exc:
            logging.error("Écriture JSON impossible %s : %s", filename, exc)
            try:
                if os.path.exists(temp_filename):
                    os.remove(temp_filename)
            except OSError:
                pass
            return False


# ============================================================
# SAAS — COMPTES, ABONNEMENTS, EMAILS ET PAIEMENTS
# ============================================================

TRIAL_DAYS = 15
SUBSCRIPTION_DAYS = 30


def current_user():
    user_id = session.get("user_id")
    if not user_id:
        return None
    return db.session.get(User, int(user_id))


def refresh_subscription_status(user):
    if not user:
        return

    now = datetime.now(timezone.utc)
    expiration = user.date_expiration_abonnement

    if expiration is None:
        return

    if expiration.tzinfo is None:
        expiration = expiration.replace(tzinfo=timezone.utc)
    else:
        expiration = expiration.astimezone(timezone.utc)

    if user.status_abonnement in ("TRIAL", "ACTIVE") and expiration <= now:
        user.status_abonnement = "EXPIRED"
        db.session.commit()
        telegram_remove_user(user)


def subscription_days_left(user):
    refresh_subscription_status(user)
    if not user or user.status_abonnement == "EXPIRED":
        return 0
    delta = user.date_expiration_abonnement - utc_now()
    return max(0, delta.days + (1 if delta.seconds else 0))


def send_owner_email(subject, body, recipient=None):
    recipient = recipient or OWNER_GMAIL
    if not recipient or not GMAIL_APP_PASSWORD:
        logging.warning("Email Gmail non envoyé : configuration absente.")
        return False
    try:
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = OWNER_GMAIL
        msg["To"] = recipient
        msg.set_content(body)
        with smtplib.SMTP_SSL(GMAIL_SMTP_HOST, GMAIL_SMTP_PORT, timeout=20) as smtp:
            smtp.login(OWNER_GMAIL, GMAIL_APP_PASSWORD)
            smtp.send_message(msg)
        return True
    except Exception as exc:
        logging.error("Envoi Gmail impossible : %s", exc)
        return False


def send_user_email(user, subject, body):
    return send_owner_email(subject, body, recipient=user.email)


def telegram_ban_user(user):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHANNEL_ID or not user.telegram_user_id:
        return False
    try:
        response = requests.post(
            telegram_api_url("banChatMember"),
            json={"chat_id": TELEGRAM_CHANNEL_ID, "user_id": int(user.telegram_user_id)},
            timeout=10,
        )
        return response.status_code == 200 and response.json().get("ok", False)
    except Exception as exc:
        logging.error("Retrait Telegram impossible pour user=%s : %s", user.id, exc)
        return False


def telegram_unban_user(user):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHANNEL_ID or not user.telegram_user_id:
        return False
    try:
        response = requests.post(
            telegram_api_url("unbanChatMember"),
            json={
                "chat_id": TELEGRAM_CHANNEL_ID,
                "user_id": int(user.telegram_user_id),
                "only_if_banned": True,
            },
            timeout=10,
        )
        return response.status_code == 200 and response.json().get("ok", False)
    except Exception as exc:
        logging.error("Réactivation Telegram impossible pour user=%s : %s", user.id, exc)
        return False


def telegram_remove_user(user):
    return telegram_ban_user(user)


def telegram_create_personal_invite(user):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHANNEL_ID or not user.telegram_user_id:
        return TELEGRAM_VIP_LINK or ""
    try:
        response = requests.post(
            telegram_api_url("createChatInviteLink"),
            json={
                "chat_id": TELEGRAM_CHANNEL_ID,
                "name": f"NOVA user {user.id}",
                "member_limit": 1,
            },
            timeout=10,
        )
        data = response.json()
        if response.status_code == 200 and data.get("ok"):
            return data["result"].get("invite_link", "")
    except Exception as exc:
        logging.error("Création invitation Telegram impossible : %s", exc)
    return TELEGRAM_VIP_LINK or ""


def telegram_link_url(user):
    if not TELEGRAM_BOT_USERNAME:
        return ""
    return f"https://t.me/{TELEGRAM_BOT_USERNAME}?start=link_{user.telegram_link_token}"


def activate_subscription(user):
    now = datetime.now(timezone.utc)
    expiration = user.date_expiration_abonnement

    if expiration is not None:
        if expiration.tzinfo is None:
            expiration = expiration.replace(tzinfo=timezone.utc)
        else:
            expiration = expiration.astimezone(timezone.utc)

    base = expiration if expiration and expiration > now else now
    user.date_expiration_abonnement = base + timedelta(days=SUBSCRIPTION_DAYS)
    user.status_abonnement = "ACTIVE"
    user.last_j7_notice = None
    user.last_j1_notice = None
    db.session.commit()
    telegram_unban_user(user)
    return user.date_expiration_abonnement


def process_subscription_notifications():
    now = datetime.now(timezone.utc)
    with app.app_context():
        users = User.query.filter(User.status_abonnement.in_(["TRIAL", "ACTIVE"])).all()
        changed = False
        for user in users:
            expiration = user.date_expiration_abonnement

            if expiration is None:
                logging.warning(
                    "Date d'expiration absente pour l'utilisateur %s.",
                    getattr(user, "id", "inconnu"),
                )
                continue

            if expiration.tzinfo is None:
                expiration = expiration.replace(tzinfo=timezone.utc)
            else:
                expiration = expiration.astimezone(timezone.utc)

            if user.date_expiration_abonnement != expiration:
                user.date_expiration_abonnement = expiration
                changed = True

            if expiration <= now:
                user.status_abonnement = "EXPIRED"
                db.session.flush()
                telegram_remove_user(user)
                changed = True
                continue

            remaining = expiration - now
            days = remaining.total_seconds() / 86400
            if 6.0 < days <= 7.0 and user.last_j7_notice is None:
                send_user_email(
                    user,
                    "NOVA TRADE IA — votre abonnement expire dans 7 jours",
                    "Bonjour,\n\nVotre abonnement NOVA TRADE IA expire dans 7 jours.\n\nRenouvelez votre abonnement avant son expiration pour conserver votre accès au Telegram VIP et aux signaux de trading.\n\nConnectez-vous à votre espace pour renouveler.\n\nCordialement,\nNOVA TRADE IA",
                )
                user.last_j7_notice = now
                changed = True
            elif 0.0 < days <= 1.0 and user.last_j1_notice is None:
                send_user_email(
                    user,
                    "NOVA TRADE IA — votre abonnement expire demain",
                    "Bonjour,\n\nVotre abonnement NOVA TRADE IA expire demain.\n\nRenouvelez votre abonnement pour conserver votre accès au Telegram VIP et aux signaux de trading.\n\nCordialement,\nNOVA TRADE IA",
                )
                user.last_j1_notice = now
                changed = True
        if changed:
            db.session.commit()


def subscription_loop():
    logging.info("Thread abonnements SaaS démarré.")
    while True:
        try:
            process_subscription_notifications()
        except Exception as exc:
            logging.exception("Erreur gestion abonnements : %s", exc)
        time.sleep(60)


def render_page(title, body):
    return render_template_string("""
<!doctype html>
<html lang="fr"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{{ title }} — NOVA TRADE IA</title>
<style>body{font-family:Arial,sans-serif;background:#0b1020;color:#fff;margin:0;padding:30px}main{max-width:760px;margin:auto;background:#151c31;padding:28px;border-radius:16px}a,button{display:inline-block;padding:12px 18px;border-radius:9px;border:0;text-decoration:none;background:#2b6cff;color:#fff;margin:6px 4px;cursor:pointer}input{width:100%;box-sizing:border-box;padding:12px;margin:7px 0 14px;border-radius:8px;border:1px solid #39445f;background:#0d1426;color:#fff}.muted{color:#aeb8cf}.danger{color:#ff8f8f}.ok{color:#79e6a3}.card{background:#0d1426;padding:18px;border-radius:12px;margin:12px 0}</style></head>
<body><main><h1>NOVA TRADE IA</h1><h2>{{ title }}</h2>{{ body|safe }}</main></body></html>
""", title=title, body=body)


def login_required_view():
    user = current_user()
    if not user:
        return redirect(url_for("login"))
    refresh_subscription_status(user)
    return user


@app.route("/register", methods=["GET", "POST"])
def register():
    if request.method == "POST":
        email = request.form.get("email", "").strip().lower()
        password = request.form.get("password", "")
        if not email or len(password) < 8:
            return render_page("Inscription", "<p class='danger'>Email valide et mot de passe d'au moins 8 caractères requis.</p>" + register_form())
        if User.query.filter(func.lower(User.email) == email).first():
            return render_page("Inscription", "<p class='danger'>Cet email est déjà utilisé.</p>" + register_form())
        now = utc_now()
        user = User(
            email=email,
            password_hash=generate_password_hash(password),
            date_inscription=now,
            status_abonnement="TRIAL",
            date_expiration_abonnement=now + timedelta(days=TRIAL_DAYS),
        )
        db.session.add(user)
        db.session.commit()
        session["user_id"] = user.id
        send_user_email(
            user,
            "Bienvenue sur NOVA TRADE IA",
            "Bienvenue sur NOVA TRADE IA.\n\nVotre essai gratuit de 15 jours commence maintenant.\n\nLiez votre compte Telegram depuis votre espace utilisateur pour recevoir votre accès VIP.\n\nCordialement,\nNOVA TRADE IA",
        )
        return redirect(url_for("dashboard"))
    return render_page("Inscription", register_form())


def register_form():
    return """<form method='post'><label>Email</label><input type='email' name='email' required><label>Mot de passe</label><input type='password' name='password' minlength='8' required><button type='submit'>Créer mon compte</button></form><p><a href='/login'>J'ai déjà un compte</a></p>"""


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        email = request.form.get("email", "").strip().lower()
        password = request.form.get("password", "")
        user = User.query.filter(func.lower(User.email) == email).first()
        if not user or not check_password_hash(user.password_hash, password):
            return render_page("Connexion", "<p class='danger'>Email ou mot de passe incorrect.</p>" + login_form())
        session.clear()
        session["user_id"] = user.id
        refresh_subscription_status(user)
        return redirect(url_for("dashboard"))
    return render_page("Connexion", login_form())


def login_form():
    return """<form method='post'><label>Email</label><input type='email' name='email' required><label>Mot de passe</label><input type='password' name='password' required><button type='submit'>Se connecter</button></form><p><a href='/register'>Créer un compte</a></p>"""


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/dashboard")
def dashboard():
    user = login_required_view()
    if not isinstance(user, User):
        return user
    days = subscription_days_left(user)
    if user.status_abonnement == "EXPIRED":
        body = f"<div class='card'><h3 class='danger'>Abonnement expiré</h3><p>Votre accès premium est verrouillé.</p><a href='{url_for('checkout_mobile')}'>Mobile Money</a><a href='{url_for('checkout_crypto')}'>Crypto</a></div>"
    else:
        link = telegram_link_url(user)
        telegram = f"<a href='{link}'>Lier mon Telegram</a>" if link else "<p class='muted'>TELEGRAM_BOT_USERNAME n'est pas encore configuré.</p>"
        body = f"<div class='card'><p>Statut : <span class='ok'>{user.status_abonnement}</span></p><p>Jours restants : <strong>{days}</strong></p><p>Expiration : {user.date_expiration_abonnement.strftime('%d/%m/%Y %H:%M UTC')}</p>{telegram}</div><div class='card'><a href='{url_for('checkout_mobile')}'>Renouveler — Mobile Money</a><a href='{url_for('checkout_crypto')}'>Renouveler — Crypto</a></div><p><a href='/logout'>Déconnexion</a></p>"
    return render_page("Dashboard", body)


@app.route("/telegram/link/<token>")
def telegram_link(token):
    user = User.query.filter_by(telegram_link_token=token).first()
    if not user:
        return render_page("Telegram", "<p class='danger'>Lien Telegram invalide.</p>"), 404
    link = telegram_link_url(user)
    if not link:
        return render_page("Telegram", "<p class='danger'>Le bot Telegram n'est pas configuré.</p>"), 503
    return redirect(link)


def feda_create_transaction(user):
    if not FEDAPAY_API_KEY or SUBSCRIPTION_PRICE_XOF <= 0:
        raise RuntimeError("Configuration FedaPay/prix manquante.")
    payload = {
        "description": f"Abonnement NOVA TRADE IA - {user.email}",
        "amount": SUBSCRIPTION_PRICE_XOF,
        "currency": {"iso": "XOF"},
        "callback_url": f"{APP_BASE_URL}/dashboard",
        "customer": {"email": user.email},
    }
    response = requests.post(
        f"{FEDAPAY_API_BASE}/transactions",
        headers={"Authorization": f"Bearer {FEDAPAY_API_KEY}", "Content-Type": "application/json"},
        json=payload,
        timeout=20,
    )
    response.raise_for_status()
    data = response.json()
    transaction = data.get("v1/transaction") or data.get("transaction") or data
    external_id = transaction.get("id")
    if not external_id:
        raise RuntimeError("FedaPay n'a pas renvoyé d'identifiant de transaction.")
    payment = Payment(user_id=user.id, provider="fedapay", external_id=str(external_id), amount=SUBSCRIPTION_PRICE_XOF, currency="XOF", status="pending")
    db.session.add(payment)
    db.session.commit()
    token_response = requests.post(
        f"{FEDAPAY_API_BASE}/transactions/{external_id}/token",
        headers={"Authorization": f"Bearer {FEDAPAY_API_KEY}", "Content-Type": "application/json"},
        timeout=20,
    )
    token_response.raise_for_status()
    token_data = token_response.json()
    token = token_data.get("token") or token_data.get("v1/token") or token_data.get("url")
    if isinstance(token, dict):
        token = token.get("url") or token.get("token")
    if not token:
        raise RuntimeError("FedaPay n'a pas renvoyé de lien de paiement.")
    return str(token)


@app.route("/checkout/mobile")
def checkout_mobile():
    user = login_required_view()
    if not isinstance(user, User):
        return user
    try:
        url = feda_create_transaction(user)
        return redirect(url)
    except Exception as exc:
        logging.exception("Création paiement FedaPay impossible : %s", exc)
        return render_page("Paiement Mobile Money", "<p class='danger'>Impossible de créer le paiement pour le moment. Vérifiez la configuration FedaPay.</p>"), 503


@app.route("/checkout/crypto")
def checkout_crypto():
    user = login_required_view()
    if not isinstance(user, User):
        return user
    if not NOWPAYMENTS_API_KEY or SUBSCRIPTION_PRICE_XOF <= 0:
        return render_page("Paiement Crypto", "<p class='danger'>Configuration NOWPayments/prix manquante.</p>"), 503
    payload = {
        "price_amount": SUBSCRIPTION_PRICE_XOF,
        "price_currency": "xof",
        "pay_currency": NOWPAYMENTS_PAY_CURRENCY,
        "ipn_callback_url": f"{APP_BASE_URL}/webhook/nowpayments",
        "order_id": f"user_{user.id}_{int(time.time())}",
        "order_description": "Abonnement NOVA TRADE IA 30 jours",
    }
    try:
        response = requests.post(f"{NOWPAYMENTS_API_BASE}/payment", headers={"x-api-key": NOWPAYMENTS_API_KEY, "Content-Type": "application/json"}, json=payload, timeout=20)
        response.raise_for_status()
        data = response.json()
        payment_id = data.get("payment_id")
        pay_address = data.get("pay_address", "")
        pay_amount = data.get("pay_amount", "")
        if not payment_id:
            raise RuntimeError("NOWPayments n'a pas renvoyé payment_id.")
        payment = Payment(user_id=user.id, provider="nowpayments", external_id=str(payment_id), amount=SUBSCRIPTION_PRICE_XOF, currency="XOF", status="pending")
        db.session.add(payment)
        db.session.commit()
        return render_page("Paiement Crypto", f"<div class='card'><p>Montant à payer : <strong>{pay_amount} {NOWPAYMENTS_PAY_CURRENCY.upper()}</strong></p><p>Adresse : <code>{pay_address}</code></p><p>Le renouvellement sera activé automatiquement après confirmation du paiement.</p></div><p><a href='/dashboard'>Retour au dashboard</a></p>")
    except Exception as exc:
        logging.exception("Création paiement NOWPayments impossible : %s", exc)
        return render_page("Paiement Crypto", "<p class='danger'>Impossible de créer le paiement crypto pour le moment.</p>"), 503


def verify_fedapay_signature(raw_body, signature):
    if not FEDAPAY_WEBHOOK_SECRET or not signature:
        return False
    digest = hmac.new(FEDAPAY_WEBHOOK_SECRET.encode(), raw_body, hashlib.sha256).hexdigest()
    candidate = signature.split("=")[-1].strip()
    return hmac.compare_digest(digest, candidate)


@app.route("/webhook/fedapay", methods=["POST"])
def webhook_fedapay():
    raw = request.get_data()
    signature = request.headers.get("X-FEDAPAY-SIGNATURE", "")
    if not verify_fedapay_signature(raw, signature):
        return {"error": "invalid signature"}, 400
    event = request.get_json(silent=True) or {}
    event_name = str(event.get("name") or event.get("type") or "")
    transaction = event.get("entity") or event.get("data") or event.get("transaction") or {}
    status = str(transaction.get("status") or "").lower()
    external_id = transaction.get("id")
    if event_name.endswith("transaction.approved") or status == "approved":
        payment = Payment.query.filter_by(provider="fedapay", external_id=str(external_id)).first()
        if payment and payment.status != "approved":
            payment.status = "approved"
            payment.paid_at = utc_now()
            user = db.session.get(User, payment.user_id)
            if user:
                activate_subscription(user)
            db.session.commit()
    return {"received": True}, 200


def verify_nowpayments_signature(raw_body, signature):
    if not NOWPAYMENTS_IPN_SECRET or not signature:
        return False
    payload = request.get_json(silent=True) or {}
    ordered = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    digest = hmac.new(NOWPAYMENTS_IPN_SECRET.encode(), ordered, hashlib.sha512).hexdigest()
    return hmac.compare_digest(digest, signature)


@app.route("/webhook/nowpayments", methods=["POST"])
def webhook_nowpayments():
    raw = request.get_data()
    signature = request.headers.get("x-nowpayments-sig", "")
    if not verify_nowpayments_signature(raw, signature):
        return {"error": "invalid signature"}, 400
    event = request.get_json(silent=True) or {}
    status = str(event.get("payment_status", "")).lower()
    payment_id = event.get("payment_id")
    if status in {"confirmed", "finished"}:
        payment = Payment.query.filter_by(provider="nowpayments", external_id=str(payment_id)).first()
        if payment and payment.status not in {"confirmed", "finished"}:
            payment.status = status
            payment.paid_at = utc_now()
            user = db.session.get(User, payment.user_id)
            if user:
                activate_subscription(user)
            db.session.commit()
    return {"received": True}, 200


@app.route("/api/pending-orders")
def pending_orders():
    api_key = request.headers.get("X-API-Key", "")
    if not MT_API_KEY or not hmac.compare_digest(api_key, MT_API_KEY):
        return {"error": "unauthorized"}, 401
    trades = ActiveTrade.query.order_by(ActiveTrade.id.asc()).all()
    return [active_trade_to_dict(t) for t in trades], 200

# ============================================================
# TELEGRAM
# ============================================================

def telegram_api_url(method):
    return f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/{method}"


def telegram_is_configured():
    return bool(TELEGRAM_TOKEN and TELEGRAM_CHANNEL_ID)


def telegram_owner_is_configured():
    return bool(TELEGRAM_TOKEN and TELEGRAM_OWNER_ID)


def telegram_configuration_diagnostic():
    token_present = bool(TELEGRAM_TOKEN)
    channel_present = bool(TELEGRAM_CHANNEL_ID)

    logging.info(
        "Telegram : token=%s, canal=%s, propriétaire=%s",
        "OK" if token_present else "ABSENT",
        "OK" if channel_present else "ABSENT",
        "OK" if TELEGRAM_OWNER_ID else "ABSENT",
    )


def telegram_validate_and_prepare():
    telegram_configuration_diagnostic()

    if not TELEGRAM_TOKEN:
        logging.warning("Telegram non configuré : TELEGRAM_TOKEN absent.")
        return False

    try:
        response = requests.get(
            telegram_api_url("getMe"),
            timeout=10,
        )
        if response.status_code != 200:
            logging.error(
                "Telegram getMe HTTP %s : %s",
                response.status_code,
                response.text,
            )
            return False

        data = response.json()
        if not data.get("ok"):
            logging.error("Token Telegram refusé : %s", data)
            return False

        bot = data.get("result", {})
        logging.info(
            "Telegram connecté : @%s",
            bot.get("username", "inconnu"),
        )

        # getUpdates ne fonctionne pas correctement si un webhook reste actif.
        delete_response = requests.post(
            telegram_api_url("deleteWebhook"),
            json={"drop_pending_updates": False},
            timeout=10,
        )

        if delete_response.status_code == 200:
            logging.info("Webhook Telegram supprimé : polling prêt.")
        else:
            logging.warning(
                "Suppression webhook Telegram HTTP %s.",
                delete_response.status_code,
            )

        return True

    except (requests.RequestException, ValueError) as exc:
        logging.error("Préparation Telegram impossible : %s", exc)
        return False


def _telegram_send(chat_id, message, reply_markup=None):
    if not TELEGRAM_TOKEN or not chat_id:
        return False

    payload = {
        "chat_id": str(chat_id),
        "text": str(message),
    }
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup

    try:
        response = requests.post(
            telegram_api_url("sendMessage"),
            json=payload,
            timeout=10,
        )
        if response.status_code != 200:
            logging.error(
                "Telegram sendMessage HTTP %s : %s",
                response.status_code,
                response.text,
            )
            return False

        data = response.json()
        if not data.get("ok"):
            logging.error("Telegram a refusé le message : %s", data)
            return False

        return True

    except (requests.RequestException, ValueError) as exc:
        logging.error("Envoi Telegram impossible : %s", exc)
        return False


def send_telegram_message(message):
    """Communications de trading uniquement vers le canal/groupe."""
    if not telegram_is_configured():
        logging.warning(
            "Alerte trading non envoyée : Telegram canal non configuré."
        )
        return False
    return _telegram_send(TELEGRAM_CHANNEL_ID, message)


def send_telegram_owner_message(message, reply_markup=None):
    """Interface personnelle uniquement vers le propriétaire."""
    if not telegram_owner_is_configured():
        return False
    return _telegram_send(
        TELEGRAM_OWNER_ID,
        message,
        reply_markup=reply_markup,
    )


def is_telegram_owner(chat_id):
    return (
        telegram_owner_is_configured()
        and str(chat_id).strip() == str(TELEGRAM_OWNER_ID).strip()
    )


def telegram_menu_keyboard():
    return {
        "inline_keyboard": [
            [
                {"text": "📡 Signaux actifs", "callback_data": "active_signals"},
                {"text": "🎯 Suivi TP/SL", "callback_data": "tracking"},
            ],
            [
                {"text": "📈 Historique", "callback_data": "history"},
                {"text": "💰 Résultats", "callback_data": "results"},
            ],
            [
                {"text": "📊 Statistiques", "callback_data": "stats"},
                {"text": "💱 Marchés", "callback_data": "watched_pairs"},
            ],
            [
                {"text": "📰 Fondamental", "callback_data": "fundamental"},
                {"text": "⚙️ État du bot", "callback_data": "bot_status"},
            ],
            [
                {"text": "📅 Rapport hebdo", "callback_data": "weekly"},
                {"text": "🔄 Actualiser", "callback_data": "refresh"},
            ],
        ]
    }


def get_tracking_message():
    active_rows = ActiveTrade.query.order_by(ActiveTrade.id.asc()).all()
    if not active_rows:
        return "🎯 SUIVI TP / SL\n━━━━━━━━━━━━━━━━━━\n\nAucun trade actif."
    lines = ["🎯 SUIVI TP / SL", "━━━━━━━━━━━━━━━━━━", ""]
    for trade in active_rows:
        symbol = trade.symbol
        direction = trade.direction
        entry = trade.entry_price
        emoji = "🟢" if direction == "BUY" else "🔴"
        status = trade.status
        lines.extend([
            f"{emoji} #{trade.id}",
            f"{direction} {symbol}",
            f"Entry : {format_price(symbol, entry)}",
            f"SL initial : {format_price(symbol, trade.initial_sl)} ({format_pips(signed_pips(symbol, direction, entry, trade.initial_sl))})",
            f"SL actuel : {format_price(symbol, trade.current_sl)} ({format_pips(signed_pips(symbol, direction, entry, trade.current_sl))})",
            f"TP1 : {'✅' if status in ('TP1_HIT','TP2_HIT') else '⏳'} {format_price(symbol, trade.tp1)} ({format_pips(signed_pips(symbol, direction, entry, trade.tp1))})",
            f"TP2 : {'✅' if status == 'TP2_HIT' else '⏳'} {format_price(symbol, trade.tp2)} ({format_pips(signed_pips(symbol, direction, entry, trade.tp2))})",
            f"TP3 : ⏳ {format_price(symbol, trade.tp3)} ({format_pips(signed_pips(symbol, direction, entry, trade.tp3))})",
            f"État : {status}",
            "",
        ])
    return "\n".join(lines)

def get_results_message():
    history = load_json(TRADE_HISTORY_FILE)
    closed = [t for t in history.values() if t.get("result") in {"TP3", "SL"}]
    tp3 = sum(t.get("result") == "TP3" for t in closed)
    sl = sum(t.get("result") == "SL" for t in closed)
    total_pips = 0.0
    for trade in closed:
        try:
            total_pips += signed_pips(
                trade.get("symbol", ""),
                trade.get("direction", "BUY"),
                trade.get("entry_price"),
                trade.get("close_price"),
            )
        except Exception:
            pass
    win_rate = (tp3 / len(closed) * 100) if closed else 0
    return ("💰 RÉSULTATS\n━━━━━━━━━━━━━━━━━━\n\n"
            f"Trades clôturés : {len(closed)}\n"
            f"🏆 TP3 : {tp3}\n"
            f"🛑 SL : {sl}\n"
            f"Taux TP3 : {win_rate:.1f}%\n"
            f"Distance cumulée : {format_pips(total_pips)}")


def get_weekly_owner_message():
    history = load_json(TRADE_HISTORY_FILE)
    start, end = _current_week_bounds()
    trades = [t for t in history.values() if (d:=_parse_trade_datetime(t.get("created_at"))) and start <= d < end]
    return ("📅 RAPPORT HEBDOMADAIRE\n━━━━━━━━━━━━━━━━━━\n\n"
            f"Semaine : {start.strftime('%d/%m/%Y')} → {(end-timedelta(seconds=1)).strftime('%d/%m/%Y')}\n"
            f"Trades : {len(trades)}\n"
            f"TP3 : {sum(t.get('result') == 'TP3' for t in trades)}\n"
            f"SL : {sum(t.get('result') == 'SL' for t in trades)}\n"
            f"Fermetures marché : {sum(t.get('result') == 'MARKET_CLOSED' for t in trades)}")

def send_telegram_menu():
    return send_telegram_owner_message(
        "🤖 NOVA TRADE AI — CONSOLE PROPRIÉTAIRE\n\n"
        "Supervision détaillée du bot, des marchés, des signaux, "
        "des trades et du module fondamental.\n\n"
        "Choisissez une rubrique :",
        reply_markup=telegram_menu_keyboard(),
    )


def telegram_answer_callback(callback_query_id):
    if not TELEGRAM_TOKEN or not callback_query_id:
        return False

    try:
        response = requests.post(
            telegram_api_url("answerCallbackQuery"),
            json={"callback_query_id": callback_query_id},
            timeout=10,
        )
        return response.status_code == 200 and response.json().get("ok", False)
    except (requests.RequestException, ValueError):
        return False


def _telegram_text_chunks(text, limit=3900):
    """Découpe un long rapport pour rester sous la limite Telegram."""
    text = str(text or "")
    if len(text) <= limit:
        return [text]

    chunks = []
    current = ""
    for block in text.split("\n\n"):
        candidate = block if not current else current + "\n\n" + block
        if len(candidate) <= limit:
            current = candidate
        else:
            if current:
                chunks.append(current)
            if len(block) <= limit:
                current = block
            else:
                for i in range(0, len(block), limit):
                    chunks.append(block[i:i + limit])
                current = ""
    if current:
        chunks.append(current)
    return chunks or [""]


def telegram_edit_message(message_id, text):
    if not telegram_owner_is_configured():
        return False

    chunks = _telegram_text_chunks(text)
    payload = {
        "chat_id": TELEGRAM_OWNER_ID,
        "message_id": message_id,
        "text": chunks[0],
        "reply_markup": telegram_menu_keyboard(),
    }

    try:
        response = requests.post(
            telegram_api_url("editMessageText"),
            json=payload,
            timeout=10,
        )
        ok = response.status_code == 200 and response.json().get("ok", False)
        if not ok:
            return False

        for chunk in chunks[1:]:
            send_telegram_owner_message(
                chunk,
                reply_markup=telegram_menu_keyboard(),
            )
        return True
    except (requests.RequestException, ValueError):
        return False


def _history_events_text(trade, symbol):
    events = trade.get("events") or []
    if not events:
        return "Aucun événement de suivi enregistré."

    lines = []
    for event in events:
        name = event.get("event", "INCONNU")
        timestamp = event.get("timestamp", "")
        price = event.get("price")
        if price is not None:
            lines.append(
                f"• {name} — {format_price(symbol, price)} — {timestamp}"
            )
        else:
            lines.append(f"• {name} — {timestamp}")
    return "\n".join(lines)


def get_watched_pairs_message():
    now = utc_now()
    lines = [
        "💱 MARCHÉS SURVEILLÉS",
        "━━━━━━━━━━━━━━━━━━",
        f"Heure serveur : {now.strftime('%Y-%m-%d %H:%M:%S UTC')}",
        "",
    ]

    for symbol in SYMBOLS:
        market_open = is_market_open(symbol, now)
        status = "🟢 OUVERT" if market_open else "🔴 FERMÉ"
        lines.append(f"• {symbol} : {status}")

    lines.extend([
        "",
        "📊 Chaîne d'analyse : M15 → M5 → M1",
        "⏱️ Suivi : toutes les 10 secondes",
    ])
    return "\n".join(lines)


def get_active_signals_message():
    active_rows = ActiveTrade.query.order_by(ActiveTrade.id.asc()).all()
    history = load_json(TRADE_HISTORY_FILE)

    if not active_rows:
        return (
            "📊 SIGNAUX ACTIFS\n"
            "━━━━━━━━━━━━━━━━━━\n\n"
            "Aucun signal actif actuellement."
        )

    lines = [
        "📊 SIGNAUX ACTIFS",
        "━━━━━━━━━━━━━━━━━━",
        f"Nombre : {len(active_rows)}",
        "",
    ]

    for index, row in enumerate(active_rows, 1):
        trade_id = row.id
        trade = history.get(trade_id, {})
        symbol = row.symbol
        direction = row.direction
        emoji = "🟢" if direction == "BUY" else "🔴"
        current_sl = row.current_sl
        lines.extend([
            f"{index}️⃣ {emoji} {direction} {symbol}",
            f"ID : {trade_id}",
            f"Stratégie : {trade.get('strategy', 'N/D')}",
            f"Pattern : {trade.get('pattern', 'N/D')}",
            f"Raison : {trade.get('reason', 'N/D')}",
            f"Tendance H1 : {trade.get('trend_h1', 'N/D')}",
            f"Entrée : {format_price(symbol, row.entry_price)}",
            f"SL initial : {format_price(symbol, row.initial_sl)} ({format_pips(signed_pips(symbol, direction, row.entry_price, row.initial_sl))})",
            f"SL actuel : {format_price(symbol, current_sl)} ({format_pips(signed_pips(symbol, direction, row.entry_price, current_sl))})",
            f"TP1 : {format_price(symbol, row.tp1)} ({format_pips(signed_pips(symbol, direction, row.entry_price, row.tp1))})",
            f"TP2 : {format_price(symbol, row.tp2)} ({format_pips(signed_pips(symbol, direction, row.entry_price, row.tp2))})",
            f"TP3 : {format_price(symbol, row.tp3)} ({format_pips(signed_pips(symbol, direction, row.entry_price, row.tp3))})",
            f"État : {row.status}",
            "",
        ])

        fundamental = trade.get("fundamental_event")
        if fundamental:
            lines.extend([
                "📰 Événement fondamental :",
                f"  Annonce : {fundamental.get('name', 'N/D')}",
                f"  Réel : {fundamental.get('actual', 'N/D')}",
                f"  Attendu : {fundamental.get('consensus', 'N/D')}",
                f"  Précédent : {fundamental.get('previous', 'N/D')}",
                f"  Impact USD : {fundamental.get('usd_impact', 'N/D')}",
            ])

        events = trade.get("events") or []
        if events:
            lines.append("Événements :")
            lines.extend(
                f"• {event.get('event', 'N/D')} — {event.get('timestamp', 'N/D')}"
                for event in events
            )
        lines.append("")

    return "\n".join(lines)

def get_history_message():
    history = load_json(TRADE_HISTORY_FILE)
    if not history:
        return "📋 HISTORIQUE\n━━━━━━━━━━━━━━━━━━\n\nAucun trade enregistré."

    now = utc_now()
    week_start = now - timedelta(days=7)
    trades = []
    for trade in history.values():
        created = _parse_trade_datetime(trade.get("created_at"))
        if created and created >= week_start:
            trades.append(trade)

    trades.sort(
        key=lambda item: _parse_trade_datetime(item.get("created_at"))
        or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )

    lines = [
        "📋 HISTORIQUE — 7 DERNIERS JOURS",
        "━━━━━━━━━━━━━━━━━━",
        f"Trades : {len(trades)}",
        "",
    ]

    for trade in trades[:20]:
        symbol = trade.get("symbol", "INCONNU")
        result = trade.get("result") or "OUVERT"
        emoji = "🟢" if result == "TP3" else "🔴" if result == "SL" else "🟡"
        lines.extend([
            f"{emoji} {symbol} {trade.get('direction', 'N/D')} — {result}",
            f"  Stratégie : {trade.get('strategy', 'N/D')} / {trade.get('pattern', 'N/D')}",
            f"  Entrée : {format_price(symbol, trade.get('entry_price', 0))}",
            f"  Clôture : {format_price(symbol, trade.get('close_price', 0)) if trade.get('close_price') is not None else '—'}",
            f"  Ouverture : {trade.get('created_at', 'N/D')}",
            f"  Fin : {trade.get('closed_at', '—')}",
            "",
        ])

    if len(trades) > 20:
        lines.append(f"… {len(trades) - 20} trade(s) supplémentaire(s).")
    return "\n".join(lines)


def get_fundamental_message():
    now = utc_now()
    with _fundamental_events_lock:
        events = list(_fundamental_events.values())

    events.sort(key=lambda event: event.get("release_at") or now)
    upcoming = [event for event in events if event.get("release_at") and event["release_at"] >= now]
    recent = [event for event in events if event.get("release_at") and event["release_at"] < now]
    recent.sort(key=lambda event: event["release_at"], reverse=True)

    lines = [
        "📰 ANALYSE FONDAMENTALE",
        "━━━━━━━━━━━━━━━━━━",
        f"Module : {'🟢 ACTIVÉ' if TRADE_NEWS else '🔴 DÉSACTIVÉ'}",
        "Devise surveillée : USD",
        "Impact : HIGH uniquement",
        "Symbole de trading : BTCUSD",
        "",
        "📅 PROCHAINES ANNONCES",
    ]

    if upcoming:
        for event in upcoming[:8]:
            release = event["release_at"].strftime("%d/%m %H:%M UTC")
            lines.extend([
                f"• {release} — {event.get('name', 'N/D')}",
                f"  Réel : {event.get('actual') or 'N/D'} | Attendu : {event.get('consensus') or 'N/D'} | Précédent : {event.get('previous') or 'N/D'}",
            ])
    else:
        lines.append("Aucune annonce à venir chargée.")

    lines.extend(["", "📌 DERNIÈRES ANNONCES"])
    if recent:
        for event in recent[:5]:
            impact = classify_usd_event(event) or "NON DÉTERMINÉ"
            lines.extend([
                f"• {event.get('name', 'N/D')} — {event['release_at'].strftime('%d/%m %H:%M UTC')}",
                f"  Réel : {event.get('actual') or 'N/D'} | Attendu : {event.get('consensus') or 'N/D'} | Précédent : {event.get('previous') or 'N/D'}",
                f"  Impact USD calculé : {impact}",
            ])
    else:
        lines.append("Aucune annonce passée chargée.")

    return "\n".join(lines)


def get_all_time_statistics_message():
    history = load_json(TRADE_HISTORY_FILE)
    active = ActiveTrade.query.count()
    closed = [trade for trade in history.values() if trade.get("result") is not None]

    wins = sum(1 for trade in closed if trade.get("result") == "TP3")
    losses = sum(1 for trade in closed if trade.get("result") == "SL")
    weekend = sum(1 for trade in closed if trade.get("result") == "MARKET_CLOSED")
    breakeven = sum(
        1 for trade in closed
        if any(event.get("event") == "BREAK_EVEN" for event in trade.get("events", []))
    )

    by_strategy = {}
    by_symbol = {}
    for trade in history.values():
        strategy = trade.get("strategy", "INCONNUE")
        symbol = trade.get("symbol", "INCONNU")
        by_strategy[strategy] = by_strategy.get(strategy, 0) + 1
        by_symbol[symbol] = by_symbol.get(symbol, 0) + 1

    lines = [
        "📊 STATISTIQUES DU BOT",
        "━━━━━━━━━━━━━━━━━━",
        f"Trades enregistrés : {len(history)}",
        f"Trades clôturés : {len(closed)}",
        f"🟢 TP3 : {wins}",
        f"🔴 SL : {losses}",
        f"🔒 Break-Even : {breakeven}",
        f"⏰ Fermetures marché : {weekend}",
        f"🟡 Positions actives : {active}",
        "",
        "📐 PAR STRATÉGIE",
    ]
    lines.extend(f"• {name} : {count}" for name, count in sorted(by_strategy.items()))
    lines.extend(["", "📈 PAR ACTIF"])
    lines.extend(f"• {name} : {count}" for name, count in sorted(by_symbol.items()))
    return "\n".join(lines)


def get_bot_status_message():
    active = ActiveTrade.query.count()
    strategies = []
    if STRAT_REVERSAL:
        strategies.append("Reversal")
    if STRAT_PULLBACK:
        strategies.append("Pullback")
    if STRAT_BREAKOUT:
        strategies.append("Breakout")

    return (
        "🤖 ÉTAT DÉTAILLÉ DU BOT\n"
        "━━━━━━━━━━━━━━━━━━\n\n"
        "🟢 Application : opérationnelle\n"
        f"🟢 BiQuote : {BIQUOTE_BASE_URL}\n"
        f"📨 Telegram canal : {'🟢 CONNECTÉ' if telegram_is_configured() else '🔴 NON CONFIGURÉ'}\n"
        f"👤 Interface propriétaire : {'🟢 CONNECTÉE' if telegram_owner_is_configured() else '🔴 NON CONFIGURÉE'}\n"
        f"📊 Positions actives : {active}\n"
        "📐 Moteur : SMC M15 → Sentinelle M5 → Déclencheur M1\n"
        "📐 Score minimum : 3/4\n"
        f"📰 Fondamental : {'🟢 ACTIF' if TRADE_NEWS else '🔴 INACTIF'}\n"
        "📡 Données : M15 → M5 → M1\n"
        "⏱️ Suivi positions : 10 secondes\n"
        "🕐 Serveur : UTC\n"
        f"🔢 Actifs surveillés : {', '.join(SYMBOLS)}\n"
    )


def handle_telegram_callback(callback_query):
    try:
        callback_id = callback_query.get("id")
        message = callback_query.get("message", {})
        chat_id = message.get("chat", {}).get("id")
        message_id = message.get("message_id")
        data = callback_query.get("data", "")

        telegram_answer_callback(callback_id)

        if not is_telegram_owner(chat_id):
            logging.warning("Bouton Telegram refusé : chat_id=%s", chat_id)
            return

        if data == "active_signals":
            text = get_active_signals_message()
        elif data == "watched_pairs":
            text = get_watched_pairs_message()
        elif data == "history":
            text = get_history_message()
        elif data == "tracking":
            text = get_tracking_message()
        elif data == "results":
            text = get_results_message()
        elif data == "weekly":
            text = get_weekly_owner_message()
        elif data == "fundamental":
            text = get_fundamental_message()
        elif data == "stats":
            text = get_all_time_statistics_message()
        elif data == "bot_status":
            text = get_bot_status_message()
        elif data == "refresh":
            text = (
                get_bot_status_message()
                + "\n"
                + get_active_signals_message()
            )
        else:
            return

        if message_id is not None:
            telegram_edit_message(message_id, text)

    except Exception as exc:
        logging.exception("Erreur callback Telegram : %s", exc)


def telegram_polling_loop():
    if not telegram_owner_is_configured():
        logging.warning("Polling Telegram arrêté : token absent.")
        return

    if not telegram_validate_and_prepare():
        logging.error("Polling Telegram arrêté : validation impossible.")
        return

    offset = None
    logging.info("Thread Telegram propriétaire démarré.")

    while True:
        try:
            params = {"timeout": 20}
            if offset is not None:
                params["offset"] = offset

            response = requests.get(
                telegram_api_url("getUpdates"),
                params=params,
                timeout=30,
            )

            if response.status_code != 200:
                logging.error(
                    "Telegram getUpdates HTTP %s.",
                    response.status_code,
                )
                time.sleep(5)
                continue

            data = response.json()
            if not data.get("ok"):
                logging.error("getUpdates refusé : %s", data)
                time.sleep(5)
                continue

            for update in data.get("result", []):
                update_id = update.get("update_id")
                if update_id is not None:
                    offset = update_id + 1

                callback = update.get("callback_query")
                if callback:
                    with app.app_context():
                        handle_telegram_callback(callback)
                    continue

                message = update.get("message")
                if not message:
                    continue

                chat_id = message.get("chat", {}).get("id")
                text = message.get("text", "").strip()

                if text.startswith("/start"):
                    parts = text.split(maxsplit=1)
                    token = parts[1].strip() if len(parts) == 2 else ""
                    if token.startswith("link_"):
                        link_token = token[5:]
                        with app.app_context():
                            linked_user = User.query.filter_by(telegram_link_token=link_token).first()
                            if linked_user:
                                linked_user.telegram_user_id = int(chat_id)
                                db.session.commit()
                                if linked_user.status_abonnement in ("TRIAL", "ACTIVE") and linked_user.date_expiration_abonnement > utc_now():
                                    telegram_unban_user(linked_user)
                                    invite = telegram_create_personal_invite(linked_user)
                                    message_text = "✅ Votre compte Telegram est lié."
                                    if invite:
                                        message_text += f"\n\n🎟️ Accès VIP : {invite}"
                                    _telegram_send(chat_id, message_text)
                                else:
                                    _telegram_send(chat_id, "❌ Votre abonnement est expiré. Renouvelez depuis votre espace NOVA TRADE IA.")
                            else:
                                _telegram_send(chat_id, "❌ Lien de liaison Telegram invalide ou expiré.")
                        continue

                if not is_telegram_owner(chat_id):
                    continue

                if text in ("/start", "/menu"):
                    with app.app_context():
                        send_telegram_menu()
                elif text == "/stats":
                    with app.app_context():
                        send_telegram_owner_message(
                            get_all_time_statistics_message(),
                            reply_markup=telegram_menu_keyboard(),
                        )

        except requests.RequestException as exc:
            logging.error("Erreur polling Telegram : %s", exc)
            time.sleep(5)
        except Exception as exc:
            logging.exception("Erreur boucle Telegram : %s", exc)
            time.sleep(5)


# ============================================================
# MODULE FONDAMENTAL — ANNONCES USD
# ============================================================

# Finance Calendar : API JSON publique sans clé API.
# Documentation : https://www.financecalendar.com/api/
# Les réponses sont mises en cache côté fournisseur ; le thread continue
# néanmoins à sonder la fenêtre de publication pour récupérer l'actual dès
# qu'il est disponible.
FINANCE_CALENDAR_BASE_URL = "https://www.financecalendar.com/wp-json/fc/v1"
FINANCE_CALENDAR_URL = f"{FINANCE_CALENDAR_BASE_URL}/calendar"
FINANCE_CALENDAR_SOURCE_URL = "https://www.financecalendar.com"

UNEMPLOYMENT_KEYWORDS = (
    "unemployment",
    "jobless claims",
    "initial jobless claims",
    "continuing jobless claims",
    "unemployment rate",
)

# Événements américains à caractère macroéconomique. Le filtre USD reste
# prioritaire lorsqu'un champ currency/country est fourni par l'API ; ces
# mots-clés servent de secours lorsque la réponse ne fournit pas ce champ.
US_MACRO_KEYWORDS = (
    "us ",
    "u.s.",
    "united states",
    "fomc",
    "fed rate",
    "federal funds",
    "nonfarm payroll",
    "non-farm payroll",
    "payrolls",
    "nfp",
    "consumer price index",
    "cpi",
    "producer price index",
    "ppi",
    "personal consumption expenditures",
    "pce",
    "gross domestic product",
    "gdp",
    "retail sales",
    "jobless claims",
    "unemployment",
    "ism",
    "jolts",
)


def _normalise_event_text(value):
    return re.sub(r"\s+", " ", str(value or "").strip()).lower()


def _parse_numeric_macro(value):
    """Convertit des valeurs macro comme 4.2%, 250K, 1.2M en float."""
    if value is None:
        return None

    text = str(value).strip()
    if not text or text.lower() in {"-", "—", "n/a", "na", "null", "pending", "not yet published"}:
        return None

    text = text.replace(",", "").replace("%", "")
    text = text.replace("−", "-").replace("–", "-")

    multiplier = 1.0
    suffix = text[-1:].upper()
    if suffix == "K":
        multiplier = 1_000.0
        text = text[:-1]
    elif suffix == "M":
        multiplier = 1_000_000.0
        text = text[:-1]
    elif suffix == "B":
        multiplier = 1_000_000_000.0
        text = text[:-1]

    match = re.search(r"[-+]?\d+(?:\.\d+)?", text)
    if not match:
        return None

    try:
        return float(match.group(0)) * multiplier
    except ValueError:
        return None


def _parse_finance_calendar_datetime(value):
    """Parse time_utc de Finance Calendar en datetime UTC."""
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except ValueError:
        pass

    # Fallbacks si le fournisseur renvoie un format sans offset.
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue

    return None


def _finance_event_currency(row):
    """Retourne le code devise/pays lorsqu'il est présent dans la réponse."""
    for key in (
        "currency",
        "currency_code",
        "country_code",
        "country",
        "country_iso",
        "iso",
    ):
        value = row.get(key)
        if value is None:
            continue
        normalized = str(value).strip().upper()
        if normalized:
            return normalized
    return ""


def _is_usd_finance_event(row, event_name):
    """Filtre USD sans supposer un champ précis dans le JSON fournisseur."""
    currency = _finance_event_currency(row)

    if currency:
        if currency in {"USD", "US", "USA", "UNITED STATES", "UNITED STATES OF AMERICA"}:
            return True
        # Une devise/pays explicitement différent de USD ne doit pas passer.
        if len(currency) <= 4 or currency in {"EUR", "GBP", "JPY", "CHF", "CAD", "AUD", "NZD"}:
            return False

    text_parts = [
        str(event_name or ""),
        str(row.get("title") or ""),
        str(row.get("category") or ""),
        str(row.get("series") or ""),
    ]
    haystack = " ".join(text_parts).lower()
    return any(keyword in haystack for keyword in US_MACRO_KEYWORDS)


def _finance_calendar_row_to_event(row):
    """Normalise un événement JSON Finance Calendar."""
    if not isinstance(row, dict):
        return None

    event_name = (
        row.get("name")
        or row.get("title")
        or row.get("event")
        or row.get("series")
        or ""
    )
    event_name = str(event_name).strip()
    if not event_name:
        return None

    impact = str(row.get("impact") or "").strip().lower()
    if impact != "high":
        return None

    if not _is_usd_finance_event(row, event_name):
        return None

    release_at = _parse_finance_calendar_datetime(
        row.get("time_utc")
        or row.get("release_at")
        or row.get("datetime")
        or row.get("date_time")
    )
    if release_at is None:
        return None

    currency = _finance_event_currency(row) or "USD"
    consensus = row.get("consensus")
    if consensus in (None, ""):
        consensus = row.get("forecast")
    previous = row.get("prior")
    if previous in (None, ""):
        previous = row.get("previous")

    event_id = (
        row.get("id")
        or row.get("event_id")
        or row.get("slug")
        or f"{release_at.isoformat()}_{currency}_{event_name}"
    )

    return {
        "event_id": str(event_id),
        "name": event_name,
        "currency": "USD",
        "impact": "HIGH",
        "release_at": release_at,
        "actual": row.get("actual"),
        "consensus": consensus,
        "previous": previous,
        "actual_num": _parse_numeric_macro(row.get("actual")),
        "consensus_num": _parse_numeric_macro(consensus),
        "previous_num": _parse_numeric_macro(previous),
        "source": FINANCE_CALENDAR_SOURCE_URL,
        "source_url": row.get("url") or FINANCE_CALENDAR_SOURCE_URL,
        "category": row.get("category"),
        "series": row.get("series"),
    }


def _extract_finance_calendar_events(payload):
    """Accepte les variantes de structure JSON possibles du fournisseur."""
    if isinstance(payload, list):
        return payload

    if not isinstance(payload, dict):
        return []

    for key in ("events", "data", "results", "calendar"):
        value = payload.get(key)
        if isinstance(value, list):
            return value

    # Une réponse unique peut aussi être normalisée comme un événement.
    if payload.get("time_utc") or payload.get("release_at"):
        return [payload]

    return []


def fetch_usd_high_impact_calendar(reference_time=None):
    """Récupère les annonces USD High Impact du calendrier Finance Calendar."""
    now = reference_time or utc_now()
    date_text = now.date().isoformat()

    try:
        response = requests.get(
            FINANCE_CALENDAR_URL,
            params={
                "from": date_text,
                "to": date_text,
                "impact": "high",
                "limit": 500,
            },
            headers={"User-Agent": "NOVA-TRADE-AI/1.0"},
            timeout=12,
        )
        response.raise_for_status()
        payload = response.json()

        events = []
        for row in _extract_finance_calendar_events(payload):
            event = _finance_calendar_row_to_event(row)
            if event is None:
                continue
            if event["release_at"].date() != now.date():
                continue
            events.append(event)

        events.sort(key=lambda item: item["release_at"])
        logging.info(
            "Calendrier fondamental Finance Calendar : %s annonce(s) USD High Impact pour %s.",
            len(events),
            now.date(),
        )
        return events

    except requests.RequestException as exc:
        logging.error("Calendrier Finance Calendar inaccessible : %s", exc)
        return []
    except (ValueError, json.JSONDecodeError) as exc:
        logging.error("Réponse Finance Calendar non JSON : %s", exc)
        return []
    except Exception as exc:
        logging.exception("Erreur calendrier Finance Calendar : %s", exc)
        return []


def _event_is_unemployment(event):
    name = _normalise_event_text(event.get("name"))
    return any(keyword in name for keyword in UNEMPLOYMENT_KEYWORDS)


def classify_usd_event(event):
    """Retourne HAUSSIER/BAISSIER selon actual/consensus/previous."""
    actual = event.get("actual_num")
    consensus = event.get("consensus_num")
    previous = event.get("previous_num")

    if actual is None or consensus is None:
        return None

    if _event_is_unemployment(event):
        if actual > consensus:
            return "BAISSIER"
        if actual < consensus:
            return "HAUSSIER"
        return None

    if previous is None:
        return None

    if actual > consensus and actual > previous:
        return "HAUSSIER"
    if actual < consensus and actual < previous:
        return "BAISSIER"
    return None


def _fundamental_signal_id(event, impact):
    release_at = event["release_at"].isoformat()
    return f"NEWS_{event['event_id']}_{release_at}_{impact}"


def calculate_fundamental_trade_levels(direction, entry, atr_h1):
    """SL = 2*ATR H1 ; TP1 = 2R.

    TP2/TP3 restent présents pour conserver la state machine commune du bot.
    """
    entry = float(entry)
    atr_h1 = float(atr_h1)
    if entry <= 0 or atr_h1 <= 0:
        return None

    risk = 2.0 * atr_h1
    if direction == "BUY":
        sl = entry - risk
        tp1 = entry + 2.0 * risk
        tp2 = entry + 3.0 * risk
        tp3 = entry + 4.0 * risk
    else:
        sl = entry + risk
        tp1 = entry - 2.0 * risk
        tp2 = entry - 3.0 * risk
        tp3 = entry - 4.0 * risk

    return {
        "entry_price": entry,
        "initial_sl": sl,
        "current_sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "risk": risk,
    }


def _fundamental_news_alert(event, direction, usd_impact, symbol):
    return (
        f"[ALERTE FONDAMENTALE] Annonce: {event['name']} | "
        f"Réel: {event.get('actual', 'N/D')} "
        f"(Attendu: {event.get('consensus', 'N/D')}). "
        f"Impact USD: {usd_impact}. "
        f"Prise de position {direction} sur {symbol}.\n"
        f"Source: {FINANCE_CALENDAR_SOURCE_URL}"
    )


def create_fundamental_trade(event, symbol, usd_impact):
    """Crée un signal fondamental après publication confirmée."""
    direction = "SELL" if usd_impact == "HAUSSIER" else "BUY"
    signal_id = _fundamental_signal_id(event, usd_impact)

    if signal_already_processed(signal_id):
        return None

    for trade in ActiveTrade.query.all():
        trade_history = load_json(TRADE_HISTORY_FILE).get(trade.id, {})
        if (
            trade_history.get("strategy") == "FUNDAMENTAL"
            and trade.symbol == symbol
            and trade_history.get("candle_timestamp") == event["release_at"].isoformat()
        ):
            return None

    entry = fetch_biquote_live_price(symbol)
    if entry is None:
        logging.warning(
            "Prix live indisponible pour le signal fondamental %s.",
            symbol,
        )
        return None

    structure = get_h1_market_structure(symbol)
    if structure is None or structure.get("atr14") is None:
        logging.warning(
            "ATR H1 indisponible pour le signal fondamental %s.",
            symbol,
        )
        return None

    levels = calculate_fundamental_trade_levels(
        direction,
        entry,
        structure["atr14"],
    )
    if levels is None:
        return None

    created_at = utc_now_iso()
    signal = {
        "signal_id": signal_id,
        "symbol": symbol,
        "direction": direction,
        "strategy": "FUNDAMENTAL",
        "pattern": event["name"],
        "reason": (
            f"USD {usd_impact}: réel={event.get('actual')} "
            f"consensus={event.get('consensus')} previous={event.get('previous')}"
        ),
        "trend_h1": structure.get("trend"),
        "candle_timestamp": event["release_at"].isoformat(),
        "created_at": created_at,
        "atr_h1": structure["atr14"],
        "zone": None,
        "fundamental_event": {
            "name": event["name"],
            "actual": event.get("actual"),
            "consensus": event.get("consensus"),
            "previous": event.get("previous"),
            "impact": usd_impact,
            "release_at": event["release_at"].isoformat(),
            "source": event.get("source", FINANCE_CALENDAR_SOURCE_URL),
            "source_url": event.get("source_url", FINANCE_CALENDAR_SOURCE_URL),
        },
        **levels,
    }

    active_trade_id = create_active_trade(signal, send_standard_alert=False)
    if active_trade_id is None:
        return None

    send_telegram_message(
        _fundamental_news_alert(event, direction, usd_impact, symbol)
    )
    logging.info(
        "Trade fondamental créé : %s %s %s après %s.",
        direction,
        symbol,
        usd_impact,
        event["name"],
    )
    return active_trade_id


def fundamental_event_already_processed(event_id):
    marker = f"FUNDAMENTAL_EVENT_{event_id}"
    return db.session.query(ProcessedSignal.id).filter(ProcessedSignal.signal_key == marker).first() is not None


def mark_fundamental_event_processed(event_id, usd_impact):
    marker = f"FUNDAMENTAL_EVENT_{event_id}"
    try:
        if fundamental_event_already_processed(event_id):
            return True
        db.session.add(ProcessedSignal(
            signal_key=marker,
            date_detection=utc_now(),
        ))
        db.session.commit()
        return True
    except Exception as exc:
        db.session.rollback()
        logging.error("Impossible d'enregistrer l'événement fondamental %s : %s", event_id, exc)
        return False


def _store_fundamental_event(event):
    with _fundamental_events_lock:
        _fundamental_events[event["event_id"]] = event


def _refresh_fundamental_calendar(now):
    global _fundamental_last_calendar_fetch
    current = time.time()
    if current - _fundamental_last_calendar_fetch < FUNDAMENTAL_CALENDAR_REFRESH_SECONDS:
        return

    events = fetch_usd_high_impact_calendar(now)
    with _fundamental_events_lock:
        for event in events:
            _fundamental_events[event["event_id"]] = event
    _fundamental_last_calendar_fetch = current


def _refresh_event_actuals(now):
    """Actual/consensus/previous sont relus au voisinage de la publication."""
    events = fetch_usd_high_impact_calendar(now)
    with _fundamental_events_lock:
        for event in events:
            _fundamental_events[event["event_id"]] = event


def fundamental_news_loop():
    logging.info(
        "Thread fondamental t_fundamental démarré (TRADE_NEWS=%s, source=Finance Calendar).",
        TRADE_NEWS,
    )
    if not TRADE_NEWS:
        return

    last_actual_refresh = 0.0

    while True:
        try:
            now = utc_now()
            _refresh_fundamental_calendar(now)

            with _fundamental_events_lock:
                pending = list(_fundamental_events.values())

            for event in pending:
                seconds_from_release = (now - event["release_at"]).total_seconds()
                if 0 <= seconds_from_release <= FUNDAMENTAL_TRIGGER_WINDOW_SECONDS:
                    if time.time() - last_actual_refresh >= 1:
                        _refresh_event_actuals(now)
                        last_actual_refresh = time.time()

                    with _fundamental_events_lock:
                        event = _fundamental_events.get(event["event_id"], event)

                    with app.app_context():
                        if fundamental_event_already_processed(event["event_id"]):
                            continue

                    usd_impact = classify_usd_event(event)
                    if usd_impact is None:
                        continue

                    with app.app_context():
                        created_ids = []
                        for symbol in FUNDAMENTAL_SYMBOLS:
                            trade_id = create_fundamental_trade(
                                event,
                                symbol,
                                usd_impact,
                            )
                            if trade_id:
                                created_ids.append(trade_id)

                        if created_ids:
                            mark_fundamental_event_processed(event["event_id"], usd_impact)

            time.sleep(FUNDAMENTAL_POLL_SECONDS)

        except Exception as exc:
            logging.exception("Erreur thread fondamental : %s", exc)
            time.sleep(5)


# BIQUOTE
# ============================================================

def fetch_biquote_live_price(symbol):
    """Récupère le dernier prix via la cascade de données à 4 sources."""
    try:
        df = fetch_biquote_ohlcv(symbol, timeframe="1m", count=2)
        if df.empty:
            logging.warning("Prix live indisponible pour %s après la cascade des 4 sources.", symbol)
            return None

        price = pd.to_numeric(df.iloc[-1]["close"], errors="coerce")
        if pd.isna(price):
            logging.warning("Prix live invalide pour %s après normalisation.", symbol)
            return None

        return float(price)
    except Exception as exc:
        logging.exception("Erreur inattendue lors de la récupération du prix live %s : %s", symbol, exc)
        return None


def fetch_biquote_ohlcv(symbol, timeframe="15m", count=200):
    """Récupère les bougies via une cascade stricte de 4 sources publiques."""
    symbol = str(symbol).upper().strip()
    timeframe = str(timeframe).lower().strip()
    count = max(1, int(count))

    columns = ["timestamp", "open", "high", "low", "close", "v"]
    timeframe_map = {
        "1m": (1, 60),
        "5m": (5, 300),
        "15m": (15, 900),
        "30m": (30, 1800),
        "1h": (60, 3600),
        "4h": (240, 14400),
        "1d": (1440, 86400),
    }

    if timeframe not in timeframe_map:
        logging.warning("Timeframe non supporté pour %s : %s.", symbol, timeframe)
        return pd.DataFrame(columns=columns)

    interval_minutes, interval_seconds = timeframe_map[timeframe]

    kraken_pairs = {
        "BTCUSD": "XXBTZUSD",
        "EURUSD": "XEURZUSD",
        "GBPUSD": "XGBPZUSD",
        "XAUUSD": "XAUUSD",
    }

    yahoo_tickers = {
        "BTCUSD": "BTC-USD",
        "EURUSD": "EURUSD=X",
        "GBPUSD": "GBPUSD=X",
        "XAUUSD": "GC=F",
    }

    def normalize_dataframe(raw_df):
        if raw_df is None or raw_df.empty:
            raise ValueError("aucune donnée reçue")

        df = raw_df.copy()

        if "timestamp" not in df.columns:
            raise ValueError("colonne timestamp absente")
        for column in ["open", "high", "low", "close"]:
            if column not in df.columns:
                raise ValueError(f"colonne {column} absente")

        if "v" not in df.columns:
            if "volume" in df.columns:
                df["v"] = df["volume"]
            else:
                df["v"] = 0.0

        for column in ["open", "high", "low", "close", "v"]:
            df[column] = pd.to_numeric(df[column], errors="coerce")

        numeric_timestamp = pd.to_numeric(df["timestamp"], errors="coerce")
        if numeric_timestamp.notna().any():
            timestamp_values = numeric_timestamp.copy()
            seconds_mask = timestamp_values.notna() & (timestamp_values.abs() < 100_000_000_000)
            timestamp_values.loc[seconds_mask] = timestamp_values.loc[seconds_mask] * 1000
            df["timestamp"] = timestamp_values
        else:
            parsed_timestamp = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
            df["timestamp"] = (parsed_timestamp.astype("int64") // 1_000_000).where(parsed_timestamp.notna())

        df = df.dropna(subset=["timestamp", "open", "high", "low", "close"])
        if df.empty:
            raise ValueError("aucune bougie valide après normalisation")

        df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce")
        df = df.dropna(subset=["timestamp"])
        df["timestamp"] = df["timestamp"].astype("int64")
        df["v"] = pd.to_numeric(df["v"], errors="coerce").fillna(0.0)

        df = (
            df[columns]
            .sort_values("timestamp")
            .drop_duplicates("timestamp")
            .tail(count)
            .reset_index(drop=True)
        )

        if df.empty:
            raise ValueError("aucune bougie valide après normalisation")

        return df

    def log_success(source_number, source_name, df, started_at):
        elapsed_ms = (time.perf_counter() - started_at) * 1000
        logging.info(
            "📊 [DATA OK] %s — Source %s (%s) — %s — %d bougies récupérées — Temps: %dms",
            symbol,
            source_number,
            source_name,
            timeframe,
            len(df),
            round(elapsed_ms),
        )
        return df

    def log_fallback(source_number, source_name, exc, next_source):
        logging.warning(
            "🔌 [FALLBACK TRIGGERED] %s — Source %s (%s) indisponible (%s) — Basculement vers Source %s...",
            symbol,
            source_number,
            source_name,
            str(exc),
            next_source,
        )

    # SOURCE 1 — BiQuote API
    started_at = time.perf_counter()
    try:
        biquote_base = BIQUOTE_BASE_URL.rstrip("/")
        if biquote_base.endswith("/ohlcv"):
            biquote_base = biquote_base[:-6].rstrip("/")
        if biquote_base.endswith("/api"):
            biquote_url = f"{biquote_base}/{symbol}/ohlc"
        else:
            biquote_url = f"{biquote_base}/api/{symbol}/ohlc"

        response = requests.get(
            biquote_url,
            params={"interval": timeframe, "limit": min(count, 1000)},
            headers={
                "Accept": "application/json",
                "User-Agent": "NOVA-TRADE-IA/1.0",
            },
            timeout=8,
        )

        # Si une ancienne variable Railway pointe encore vers /api/ohlcv,
        # on retente une seule fois sur l'URL canonique publique BiQuote.
        if response.status_code == 404 and biquote_url != f"https://biquote.io/api/{symbol}/ohlc":
            response = requests.get(
                f"https://biquote.io/api/{symbol}/ohlc",
                params={"interval": timeframe, "limit": min(count, 1000)},
                headers={
                    "Accept": "application/json",
                    "User-Agent": "NOVA-TRADE-IA/1.0",
                },
                timeout=8,
            )

        response.raise_for_status()
        data = response.json()
        bars = data.get("bars") if isinstance(data, dict) else None
        if not isinstance(bars, list) or not bars:
            raise ValueError("réponse BiQuote vide ou format invalide")

        raw_df = pd.DataFrame(bars)
        if "openTime" in raw_df.columns and "timestamp" not in raw_df.columns:
            raw_df["timestamp"] = raw_df["openTime"]
        if "volume" in raw_df.columns and "v" not in raw_df.columns:
            raw_df["v"] = raw_df["volume"]

        return log_success(1, "BiQuote API", normalize_dataframe(raw_df), started_at)
    except Exception as exc:
        log_fallback(1, "BiQuote API", exc, 2)

    # SOURCE 2 — Kraken Public API
    started_at = time.perf_counter()
    try:
        pair = kraken_pairs.get(symbol)
        if not pair:
            raise ValueError(f"symbole {symbol} non mappé pour Kraken")

        response = requests.get(
            "https://api.kraken.com/0/public/OHLC",
            params={"pair": pair, "interval": interval_minutes},
            timeout=5,
        )
        response.raise_for_status()
        data = response.json()
        errors = data.get("error", [])
        if errors:
            raise ValueError("; ".join(str(error) for error in errors))

        result = data.get("result")
        if not isinstance(result, dict):
            raise ValueError("réponse Kraken invalide")

        pair_key = next(
            (key for key, value in result.items() if key != "last" and isinstance(value, list)),
            None,
        )
        if not pair_key or not result[pair_key]:
            raise ValueError("aucune bougie Kraken disponible")

        raw_df = pd.DataFrame(
            result[pair_key],
            columns=["timestamp", "open", "high", "low", "close", "vwap", "v", "count"],
        )
        return log_success(2, "Kraken Public API", normalize_dataframe(raw_df), started_at)
    except Exception as exc:
        log_fallback(2, "Kraken Public API", exc, 3)

    # SOURCE 3 — Coinbase Public API, BTCUSD uniquement
    started_at = time.perf_counter()
    try:
        if symbol != "BTCUSD":
            raise ValueError("Coinbase est réservé exclusivement au BTCUSD")

        granularity = {
            60: 60,
            300: 300,
            900: 900,
            1800: 1800,
            3600: 3600,
            86400: 86400,
        }.get(interval_seconds)
        if granularity is None:
            raise ValueError(f"intervalle {timeframe} non supporté par Coinbase")

        response = requests.get(
            "https://api.exchange.coinbase.com/products/BTC-USD/candles",
            params={"granularity": granularity},
            headers={
                "Accept": "application/json",
                "User-Agent": "NOVA-TRADE-AI/1.0",
            },
            timeout=5,
        )
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, list) or not data:
            raise ValueError("aucune bougie Coinbase disponible")

        raw_df = pd.DataFrame(
            data,
            columns=["timestamp", "low", "high", "open", "close", "v"],
        )
        return log_success(3, "Coinbase Public API", normalize_dataframe(raw_df), started_at)
    except Exception as exc:
        log_fallback(3, "Coinbase Public API", exc, 4)

    # SOURCE 4 — Yahoo Finance URL Downloader
    started_at = time.perf_counter()
    try:
        ticker = yahoo_tickers.get(symbol)
        if not ticker:
            raise ValueError(f"ticker Yahoo absent pour {symbol}")

        now_epoch = int(time.time())
        period1 = now_epoch - (interval_seconds * max(count + 10, count * 2))

        yahoo_headers = {
            "Accept": "application/json,text/plain,*/*",
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0 Safari/537.36"
            ),
            "Connection": "keep-alive",
        }
        response = requests.get(
            f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}",
            params={
                "period1": period1,
                "period2": now_epoch,
                "interval": timeframe,
                "events": "history",
                "includeAdjustedClose": "true",
            },
            headers=yahoo_headers,
            timeout=8,
        )
        if response.status_code == 429:
            time.sleep(2.0)
            response = requests.get(
                f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}",
                params={
                    "period1": period1,
                    "period2": now_epoch,
                    "interval": timeframe,
                    "events": "history",
                    "includeAdjustedClose": "true",
                },
                headers=yahoo_headers,
                timeout=8,
            )
        response.raise_for_status()
        data = response.json()
        chart = data.get("chart", {})
        if chart.get("error"):
            raise ValueError(str(chart["error"]))

        results = chart.get("result")
        if not results:
            raise ValueError("aucune donnée Yahoo disponible")

        result = results[0]
        timestamps = result.get("timestamp")
        quote_list = result.get("indicators", {}).get("quote", [])
        if not timestamps or not quote_list:
            raise ValueError("structure Yahoo invalide")

        quote = quote_list[0]
        raw_df = pd.DataFrame(
            {
                "timestamp": timestamps,
                "open": quote.get("open", []),
                "high": quote.get("high", []),
                "low": quote.get("low", []),
                "close": quote.get("close", []),
                "v": quote.get("volume", []),
            }
        )
        return log_success(4, "Yahoo Finance", normalize_dataframe(raw_df), started_at)
    except Exception as exc:
        log_fallback(4, "Yahoo Finance", exc, "aucune")
        logging.error(
            "🚨 [CRITICAL OUTAGE] %s — Source 4 échouée. Aucune donnée disponible pour ce bloc.",
            symbol,
        )
        return pd.DataFrame(columns=columns)


# ============================================================
# INDICATEURS PRICE ACTION + SMC
# ============================================================

def calculate_atr(df, period=ATR_PERIOD):
    if df.empty:
        return pd.Series(dtype=float)
    previous_close = df["close"].shift(1)
    tr = pd.concat([
        df["high"] - df["low"],
        (df["high"] - previous_close).abs(),
        (df["low"] - previous_close).abs(),
    ], axis=1).max(axis=1)
    return tr.rolling(period, min_periods=period).mean()


def calculate_ema(df, period):
    return df["close"].ewm(span=period, adjust=False, min_periods=period).mean()


def candle_range(candle):
    return float(candle["high"]) - float(candle["low"])


def candle_body(candle):
    return abs(float(candle["close"]) - float(candle["open"]))


def wick_ratio(candle, direction):
    total = candle_range(candle)
    if total <= 0:
        return 0.0
    if direction == "BUY":
        wick = min(float(candle["open"]), float(candle["close"])) - float(candle["low"])
    else:
        wick = float(candle["high"]) - max(float(candle["open"]), float(candle["close"]))
    return max(0.0, wick) / total


def bullish_engulfing(df):
    if len(df) < 4:
        return False
    previous = df.iloc[-3]
    current = df.iloc[-2]
    return (
        float(previous["close"]) < float(previous["open"])
        and float(current["close"]) > float(current["open"])
        and float(current["open"]) <= float(previous["close"])
        and float(current["close"]) >= float(previous["open"])
    )


def bearish_engulfing(df):
    if len(df) < 4:
        return False
    previous = df.iloc[-3]
    current = df.iloc[-2]
    return (
        float(previous["close"]) > float(previous["open"])
        and float(current["close"]) < float(current["open"])
        and float(current["open"]) >= float(previous["close"])
        and float(current["close"]) <= float(previous["open"])
    )


def hammer_rejection(df):
    if len(df) < 3:
        return False
    candle = df.iloc[-2]
    return float(candle["close"]) >= float(candle["open"]) and wick_ratio(candle, "BUY") >= WICK_MIN_RATIO


def shooting_star_rejection(df):
    if len(df) < 3:
        return False
    candle = df.iloc[-2]
    return float(candle["close"]) <= float(candle["open"]) and wick_ratio(candle, "SELL") >= WICK_MIN_RATIO


def detect_reversal_pattern(df, direction=None):
    patterns = []
    if bullish_engulfing(df):
        patterns.append("AVALEMENT_HAUSSIER")
    if hammer_rejection(df):
        patterns.append("MARTEAU")
    if bearish_engulfing(df):
        patterns.append("AVALEMENT_BAISSIER")
    if shooting_star_rejection(df):
        patterns.append("ETOILE_FILANTE")
    if direction == "BUY":
        return next((p for p in patterns if p in {"AVALEMENT_HAUSSIER", "MARTEAU"}), None)
    if direction == "SELL":
        return next((p for p in patterns if p in {"AVALEMENT_BAISSIER", "ETOILE_FILANTE"}), None)
    return patterns[0] if patterns else None


def find_pivots(df, window=PIVOT_WINDOW):
    if df.empty or len(df) < window:
        return [], []
    radius = window // 2
    highs, lows = [], []
    for index in range(radius, len(df) - radius):
        high_slice = df["high"].iloc[index-radius:index+radius+1]
        low_slice = df["low"].iloc[index-radius:index+radius+1]
        high = float(df["high"].iloc[index])
        low = float(df["low"].iloc[index])
        if high == float(high_slice.max()):
            highs.append({"index": index, "timestamp": df["timestamp"].iloc[index], "level": high})
        if low == float(low_slice.min()):
            lows.append({"index": index, "timestamp": df["timestamp"].iloc[index], "level": low})
    return highs, lows


def determine_dow_trend(pivot_highs, pivot_lows):
    if len(pivot_highs) < 3 or len(pivot_lows) < 3:
        return "RANGE"
    hs = [x["level"] for x in pivot_highs[-3:]]
    ls = [x["level"] for x in pivot_lows[-3:]]
    if hs[0] < hs[1] < hs[2] and ls[0] < ls[1] < ls[2]:
        return "HAUSSIERE"
    if hs[0] > hs[1] > hs[2] and ls[0] > ls[1] > ls[2]:
        return "BAISSIERE"
    return "RANGE"


def _zone_dict(kind, low, high, origin_index, timestamp, source="SMC"):
    low, high = sorted((float(low), float(high)))
    return {
        "kind": kind,
        "low_band": low,
        "high_band": high,
        "level": (low + high) / 2.0,
        "origin_index": int(origin_index),
        "timestamp": str(timestamp),
        "source": source,
    }


def detect_bos_choch(df, pivot_highs, pivot_lows):
    """Détecte les cassures confirmées des derniers pivots sur H1."""
    if len(df) < 5:
        return []
    events = []
    last_closed_index = len(df) - 1
    for pivot in pivot_highs[-8:]:
        idx = int(pivot["index"])
        future = df.iloc[idx + 1:last_closed_index + 1]
        if future.empty:
            continue
        closes = pd.to_numeric(future["close"], errors="coerce")
        hits = future[closes > float(pivot["level"])]
        if not hits.empty:
            events.append({
                "type": "BOS_BULLISH",
                "level": float(pivot["level"]),
                "pivot_index": idx,
                "break_index": int(hits.index[-1]),
                "timestamp": str(hits.iloc[-1]["timestamp"]),
            })
            break
    for pivot in pivot_lows[-8:]:
        idx = int(pivot["index"])
        future = df.iloc[idx + 1:last_closed_index + 1]
        if future.empty:
            continue
        closes = pd.to_numeric(future["close"], errors="coerce")
        hits = future[closes < float(pivot["level"])]
        if not hits.empty:
            events.append({
                "type": "BOS_BEARISH",
                "level": float(pivot["level"]),
                "pivot_index": idx,
                "break_index": int(hits.index[-1]),
                "timestamp": str(hits.iloc[-1]["timestamp"]),
            })
            break

    # Le dernier événement est classé CHoCH si sa direction est opposée à la tendance Dow.
    trend = determine_dow_trend(pivot_highs, pivot_lows)
    events.sort(key=lambda e: e["break_index"])
    if events:
        last = events[-1]
        if (trend == "BAISSIERE" and last["type"] == "BOS_BULLISH") or (trend == "HAUSSIERE" and last["type"] == "BOS_BEARISH"):
            last["type"] = last["type"].replace("BOS", "CHoCH")
    return events


def detect_order_blocks(df, structure_events, lookback=12):
    """Dernière bougie opposée avant le mouvement ayant cassé une structure."""
    bullish, bearish = [], []
    for event in structure_events[-8:]:
        break_index = int(event["break_index"])
        direction = "BUY" if event["type"] in {"BOS_BULLISH", "CHoCH_BULLISH"} else "SELL"
        start = max(0, break_index - lookback)
        segment = df.iloc[start:break_index]
        if segment.empty:
            continue
        if direction == "BUY":
            candidates = segment[segment["close"] < segment["open"]]
            if not candidates.empty:
                idx = candidates.index[-1]
                c = df.loc[idx]
                zone = _zone_dict("ORDER_BLOCK_BUY", c["low"], c["high"], idx, c["timestamp"], "OB")
                zone.update({"direction": "BUY", "break_type": event["type"]})
                bullish.append(zone)
        else:
            candidates = segment[segment["close"] > segment["open"]]
            if not candidates.empty:
                idx = candidates.index[-1]
                c = df.loc[idx]
                zone = _zone_dict("ORDER_BLOCK_SELL", c["low"], c["high"], idx, c["timestamp"], "OB")
                zone.update({"direction": "SELL", "break_type": event["type"]})
                bearish.append(zone)
    return bullish[-5:], bearish[-5:]


def detect_fvg(df):
    """FVG classique 3 bougies : bullish si High[N-2] < Low[N], bearish inverse."""
    bullish, bearish = [], []
    if len(df) < 3:
        return bullish, bearish
    for i in range(2, len(df)):
        a, b, c = df.iloc[i-2], df.iloc[i-1], df.iloc[i]
        if float(a["high"]) < float(c["low"]):
            bullish.append(_zone_dict("FVG_BUY", a["high"], c["low"], i, c["timestamp"], "FVG"))
        if float(a["low"]) > float(c["high"]):
            bearish.append(_zone_dict("FVG_SELL", c["high"], a["low"], i, c["timestamp"], "FVG"))
    return bullish[-12:], bearish[-12:]


def get_h1_market_structure(symbol):
    df = fetch_biquote_ohlcv(symbol, timeframe="1h", count=300)
    if df.empty or len(df) < 80:
        return None
    closed = df.iloc[:-1].copy()
    closed["atr14"] = calculate_atr(closed)
    closed["ema20"] = calculate_ema(closed, 20)
    closed["ema50"] = calculate_ema(closed, 50)
    atr = closed["atr14"].iloc[-1]
    if pd.isna(atr) or float(atr) <= 0:
        return None
    pivot_highs, pivot_lows = find_pivots(closed, PIVOT_WINDOW)
    trend = determine_dow_trend(pivot_highs, pivot_lows)
    events = detect_bos_choch(closed, pivot_highs, pivot_lows)
    obs_buy, obs_sell = detect_order_blocks(closed, events)
    fvg_buy, fvg_sell = detect_fvg(closed)
    return {
        "df": closed,
        "atr14": float(atr),
        "pivot_highs": pivot_highs,
        "pivot_lows": pivot_lows,
        "trend": trend,
        "ema20": float(closed["ema20"].iloc[-1]) if not pd.isna(closed["ema20"].iloc[-1]) else None,
        "ema50": float(closed["ema50"].iloc[-1]) if not pd.isna(closed["ema50"].iloc[-1]) else None,
        "structure_events": events,
        "order_blocks_buy": obs_buy,
        "order_blocks_sell": obs_sell,
        "fvg_buy": fvg_buy,
        "fvg_sell": fvg_sell,
    }


def price_in_zone(price, zone):
    return float(zone["low_band"]) <= float(price) <= float(zone["high_band"])


def candle_rejects_zone(candle, zone, direction):
    if not (float(candle["low"]) <= zone["high_band"] and float(candle["high"]) >= zone["low_band"]):
        return False
    if direction == "BUY":
        return float(candle["close"]) >= float(zone["low_band"]) and wick_ratio(candle, "BUY") >= WICK_MIN_RATIO
    return float(candle["close"]) <= float(zone["high_band"]) and wick_ratio(candle, "SELL") >= WICK_MIN_RATIO


def polarity_continuation(df_h1, direction):
    if len(df_h1) < 8:
        return False
    highs, lows = find_pivots(df_h1, PIVOT_WINDOW)
    current = float(df_h1["close"].iloc[-1])
    candidates = highs if direction == "BUY" else lows
    for pivot in reversed(candidates[-10:]):
        level = float(pivot["level"])
        after = df_h1.iloc[int(pivot["index"]) + 1:]
        if direction == "BUY":
            broke = (after["close"] > level).any()
            if broke and current >= level:
                return True
        else:
            broke = (after["close"] < level).any()
            if broke and current <= level:
                return True
    return False


def score_smc_price_action(df_m15, structure, direction):
    """Score strict 3/4 : zone OB, rejet, pattern, polarité."""
    trigger = df_m15.iloc[-2]
    price = float(trigger["close"])
    ob_zones = structure["order_blocks_buy"] if direction == "BUY" else structure["order_blocks_sell"]
    fvg_zones = structure["fvg_buy"] if direction == "BUY" else structure["fvg_sell"]
    zone = next((z for z in reversed(ob_zones) if price_in_zone(price, z) or candle_rejects_zone(trigger, z, direction)), None)
    fvg = next((z for z in reversed(fvg_zones) if price_in_zone(price, z) or candle_rejects_zone(trigger, z, direction)), None)
    score = 0
    reasons = []
    if zone:
        score += 1
        reasons.append("ZONE_OB")
    elif fvg:
        reasons.append("ZONE_FVG")
    else:
        return None
    selected_zone = zone or fvg
    if candle_rejects_zone(trigger, selected_zone, direction):
        score += 1
        reasons.append("RETEST_REJET")
    pattern = detect_reversal_pattern(df_m15, direction)
    if pattern:
        score += 1
        reasons.append(pattern)
    if polarity_continuation(structure["df"], direction):
        score += 1
        reasons.append("POLARITE")
    if score < 3:
        return None
    return {
        "strategy": "SMC_PRICE_ACTION",
        "direction": direction,
        "pattern": pattern or "CONFIRMATION_SANS_PATTERN",
        "zone": selected_zone,
        "structure_price": selected_zone["low_band"] if direction == "BUY" else selected_zone["high_band"],
        "score": score,
        "score_max": 4,
        "reason": " + ".join(reasons),
    }


def detect_smc_signal(df_m15, structure):
    if df_m15.empty or structure is None:
        return None
    h1_close = float(structure["df"]["close"].iloc[-1])
    ema20, ema50 = structure.get("ema20"), structure.get("ema50")
    directions = []
    if structure["trend"] == "HAUSSIERE" and ema20 and ema50 and h1_close >= ema20 >= ema50:
        directions.append("BUY")
    if structure["trend"] == "BAISSIERE" and ema20 and ema50 and h1_close <= ema20 <= ema50:
        directions.append("SELL")
    # Un CHoCH récent peut fournir le contexte de transition.
    if not directions and structure.get("structure_events"):
        last = structure["structure_events"][-1]
        if last["type"] == "CHoCH_BULLISH":
            directions.append("BUY")
        elif last["type"] == "CHoCH_BEARISH":
            directions.append("SELL")
    for direction in directions:
        candidate = score_smc_price_action(df_m15, structure, direction)
        if candidate:
            candidate["reason"] = f"Score {candidate['score']}/4 — {candidate['reason']}"
            return candidate
    return None


# ============================================================
# MONEY MANAGEMENT
# ============================================================
# ============================================================

def calculate_trade_levels(direction, entry, structure_price, atr):
    entry = float(entry)
    structure_price = float(structure_price)
    atr = float(atr)

    if entry <= 0 or atr <= 0:
        return None

    sl_distance = SL_ATR_MULTIPLIER * atr

    if direction == "BUY":
        sl = structure_price - sl_distance
        # Sécurité : le SL doit rester sous l'entrée.
        if sl >= entry:
            sl = entry - sl_distance
        risk = entry - sl
        if risk <= 0:
            return None

        tp1 = entry + risk
        tp2 = entry + 2 * risk
        tp3 = entry + 3 * risk

    else:
        sl = structure_price + sl_distance
        # Sécurité : le SL doit rester au-dessus de l'entrée.
        if sl <= entry:
            sl = entry + sl_distance
        risk = sl - entry
        if risk <= 0:
            return None

        tp1 = entry - risk
        tp2 = entry - 2 * risk
        tp3 = entry - 3 * risk

    return {
        "entry_price": entry,
        "initial_sl": sl,
        "current_sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "risk": risk,
    }


def build_signal(symbol, m15, structure, candidate):
    candle = m15.iloc[-2]
    entry = float(candle["close"])

    levels = calculate_trade_levels(
        candidate["direction"],
        entry,
        candidate["structure_price"],
        structure["atr14"],
    )

    if levels is None:
        return None

    candle_timestamp = candle["timestamp"]
    candle_id = (
        f"{symbol}_M15_{candle_timestamp}_"
        f"{candidate['strategy']}_{candidate['direction']}"
    )

    return {
        "signal_id": candle_id,
        "symbol": symbol,
        "direction": candidate["direction"],
        "strategy": candidate["strategy"],
        "pattern": candidate["pattern"],
        "reason": candidate["reason"],
        "trend_h1": structure["trend"],
        "candle_timestamp": str(candle_timestamp),
        "created_at": utc_now_iso(),
        "atr_h1": structure["atr14"],
        "zone": candidate["zone"],
        "score": candidate.get("score"),
        "score_max": candidate.get("score_max", 4),
        "structure_events": structure.get("structure_events", [])[-3:],
        "order_block": candidate.get("zone") if candidate.get("zone", {}).get("source") == "OB" else None,
        "fvg": candidate.get("zone") if candidate.get("zone", {}).get("source") == "FVG" else None,
        **levels,
    }


def active_trade_to_dict(trade):
    return {
        "trade_id": trade.id,
        "id_signal": trade.id,
        "symbol": trade.symbol,
        "direction": trade.direction,
        "entry_price": trade.entry_price,
        "initial_sl": trade.initial_sl,
        "current_sl": trade.current_sl,
        "tp1": trade.tp1,
        "tp2": trade.tp2,
        "tp3": trade.tp3,
        "status": trade.status,
    }


def signal_already_processed(signal_id):
    return db.session.query(ProcessedSignal.id).filter(ProcessedSignal.signal_key == signal_id).first() is not None


def has_same_active_signal(signal):
    history = load_json(TRADE_HISTORY_FILE)
    for trade in ActiveTrade.query.all():
        if trade.symbol != signal["symbol"] or trade.direction != signal["direction"]:
            continue
        trade_history = history.get(trade.id, {})
        if trade_history.get("candle_timestamp") == signal.get("candle_timestamp"):
            return True
    return False


# ============================================================
# CRÉATION / HISTORIQUE DES TRADES
# ============================================================

def ensure_trade_history_record(trade_id, trade):
    history = load_json(TRADE_HISTORY_FILE)
    if trade_id in history:
        return

    history[trade_id] = {
        "trade_id": trade_id,
        "symbol": trade.get("symbol"),
        "direction": trade.get("direction"),
        "strategy": trade.get("strategy"),
        "pattern": trade.get("pattern"),
        "entry_price": trade.get("entry_price"),
        "initial_sl": trade.get("initial_sl"),
        "current_sl": trade.get("current_sl"),
        "tp1": trade.get("tp1"),
        "tp2": trade.get("tp2"),
        "tp3": trade.get("tp3"),
        "candle_timestamp": trade.get("candle_timestamp"),
        "created_at": trade.get("created_at", utc_now_iso()),
        "status": "ACTIVE",
        "result": None,
        "closed_at": None,
        "close_price": None,
        "events": [],
    }
    save_json(TRADE_HISTORY_FILE, history)


def record_trade_event(trade_id, event, price=None):
    history = load_json(TRADE_HISTORY_FILE)
    if trade_id not in history:
        return

    event_data = {
        "event": event,
        "timestamp": utc_now_iso(),
    }
    if price is not None:
        event_data["price"] = price

    history[trade_id].setdefault("events", []).append(event_data)
    save_json(TRADE_HISTORY_FILE, history)


def send_trade_financial_report(trade_id, trade, result, close_price):
    if result not in ("TP3", "SL"):
        return False

    symbol = trade.get("symbol", "INCONNU")
    direction = trade.get("direction", "INCONNUE")
    entry = trade.get("entry_price")
    initial_sl = trade.get("initial_sl", trade.get("current_sl"))
    current_sl = trade.get("current_sl", initial_sl)
    tp1 = trade.get("tp1")
    tp2 = trade.get("tp2")
    tp3 = trade.get("tp3")

    pnl_distance = None
    try:
        if direction == "BUY":
            pnl_distance = float(close_price) - float(entry)
        else:
            pnl_distance = float(entry) - float(close_price)
    except (TypeError, ValueError):
        pass

    lines = [
        "NOVA TRADE IA — RAPPORT FINANCIER",
        "",
        f"Résultat : {result}",
        f"Actif : {symbol}",
        f"Direction : {direction}",
        f"Stratégie : {trade.get('strategy', 'INCONNUE')}",
        f"Pattern : {trade.get('pattern', 'INCONNU')}",
        f"Entrée : {format_price(symbol, entry)}",
        f"SL initial : {format_price(symbol, initial_sl)}",
        f"SL au moment de la clôture : {format_price(symbol, current_sl)}",
        f"TP1 : {format_price(symbol, tp1)}",
        f"TP2 : {format_price(symbol, tp2)}",
        f"TP3 : {format_price(symbol, tp3)}",
        f"Prix de clôture : {format_price(symbol, close_price)}",
        f"Écart entrée/clôture : {pnl_distance if pnl_distance is not None else 'N/D'}",
        f"Ouverture : {trade.get('created_at', 'N/D')}",
        f"Clôture : {utc_now_iso()}",
        "",
        "Événements :",
    ]
    events = trade.get("events") or []
    if events:
        lines.extend(
            f"- {event.get('event', 'N/D')} : {event.get('timestamp', 'N/D')}"
            for event in events
        )
    else:
        lines.append("- Aucun événement supplémentaire.")

    return send_owner_email(
        f"NOVA TRADE IA — {result} {symbol} — rapport financier",
        "\n".join(lines),
    )


def close_trade_in_history(trade_id, result, close_price):
    history = load_json(TRADE_HISTORY_FILE)
    if trade_id not in history:
        return

    history[trade_id]["status"] = "CLOSED"
    history[trade_id]["result"] = result
    history[trade_id]["closed_at"] = utc_now_iso()
    history[trade_id]["close_price"] = close_price

    save_json(TRADE_HISTORY_FILE, history)

    if result in ("TP3", "SL"):
        send_trade_financial_report(
            trade_id,
            history[trade_id],
            result,
            close_price,
        )


def format_price(symbol, value):
    if value is None:
        return "N/D"
    try:
        value = float(value)
    except (TypeError, ValueError):
        return "N/D"
    if symbol in {"EURUSD", "GBPUSD"}:
        return f"{value:.5f}"
    if symbol == "XAUUSD":
        return f"{value:.2f}"
    return f"{value:.2f}"


def pip_size(symbol):
    if symbol in {"EURUSD", "GBPUSD"}:
        return 0.0001
    if symbol == "XAUUSD":
        return 0.01
    if symbol == "BTCUSD":
        return 1.0
    return 0.0001


def pips_between(symbol, start_price, end_price):
    try:
        return (float(end_price) - float(start_price)) / pip_size(symbol)
    except (TypeError, ValueError, ZeroDivisionError):
        return 0.0


def signed_pips(symbol, direction, start_price, end_price):
    raw = pips_between(symbol, start_price, end_price)
    return raw if direction == "BUY" else -raw


def format_pips(value):
    try:
        value = float(value)
        sign = "+" if value > 0 else ""
        return f"{sign}{value:.1f} pips"
    except (TypeError, ValueError):
        return "N/D"


def send_trade_alert(signal):
    direction_emoji = "🟢" if signal["direction"] == "BUY" else "🔴"
    symbol = signal["symbol"]
    entry = signal["entry_price"]
    sl = signal["initial_sl"]
    sl_pips = signed_pips(symbol, signal["direction"], entry, sl)
    tp1_pips = signed_pips(symbol, signal["direction"], entry, signal["tp1"])
    tp2_pips = signed_pips(symbol, signal["direction"], entry, signal["tp2"])
    tp3_pips = signed_pips(symbol, signal["direction"], entry, signal["tp3"])
    score = signal.get("score")
    message = (
        "[SIGNAL ALERT]\n"
        f"{direction_emoji} {signal['direction']} {symbol}\n"
        f"Signal : #{signal['signal_id']}\n\n"
        f"Entry : {format_price(symbol, entry)}\n"
        f"SL : {format_price(symbol, sl)} ({format_pips(sl_pips)})\n"
        f"TP1 : {format_price(symbol, signal['tp1'])} ({format_pips(tp1_pips)})\n"
        f"TP2 : {format_price(symbol, signal['tp2'])} ({format_pips(tp2_pips)})\n"
        f"TP3 : {format_price(symbol, signal['tp3'])} ({format_pips(tp3_pips)})\n"
        + (f"Score SMC/PA : {score}/4\n" if score is not None else "")
        + f"Stratégie : {signal['strategy']}\n"
        f"🕐 {utc_now().strftime('%Y-%m-%d %H:%M:%S UTC')}"
    )
    return send_telegram_message(message)

def create_active_trade(signal, send_standard_alert=True):
    trade_id = (
        f"TRADE_{signal['direction']}_"
        f"{signal['symbol']}_"
        f"{int(time.time() * 1000)}"
    )

    trade = {
        "id_signal": signal["signal_id"],
        "signal_display_id": f"#{signal['signal_id']}",
        "score": signal.get("score"),
        "score_max": signal.get("score_max", 4),
        "symbol": signal["symbol"],
        "direction": signal["direction"],
        "strategy": signal["strategy"],
        "pattern": signal["pattern"],
        "reason": signal["reason"],
        "entry_price": signal["entry_price"],
        "initial_sl": signal["initial_sl"],
        "current_sl": signal["current_sl"],
        "tp1": signal["tp1"],
        "tp2": signal["tp2"],
        "tp3": signal["tp3"],
        "status": "ACTIVE",
        "candle_timestamp": signal["candle_timestamp"],
        "created_at": signal["created_at"],
        "tp1_notified": False,
        "tp2_notified": False,
        "trend_h1": signal.get("trend_h1"),
        "atr_h1": signal.get("atr_h1"),
        "fundamental_event": signal.get("fundamental_event"),
    }

    try:
        # L'unicité de ProcessedSignal est garantie par PostgreSQL.
        processed = ProcessedSignal(
            signal_key=signal["signal_id"],
            date_detection=utc_now(),
        )
        db.session.add(processed)
        db.session.add(ActiveTrade(
            id=trade_id,
            user_id=None,
            symbol=signal["symbol"],
            direction=signal["direction"],
            entry_price=float(signal["entry_price"]),
            initial_sl=float(signal["initial_sl"]),
            current_sl=float(signal["current_sl"]),
            tp1=float(signal["tp1"]),
            tp2=float(signal["tp2"]),
            tp3=float(signal["tp3"]),
            status="ACTIVE",
        ))
        db.session.commit()
    except Exception:
        db.session.rollback()
        logging.exception("Création PostgreSQL du trade/signal impossible : %s", signal.get("signal_id"))
        return None

    ensure_trade_history_record(trade_id, trade)

    if send_standard_alert:
        send_trade_alert(signal)

    logging.info(
        "Signal %s créé en PostgreSQL : %s %s, stratégie=%s.",
        trade_id,
        signal["direction"],
        signal["symbol"],
        signal["strategy"],
    )
    return trade_id


# ============================================================
# MOTEUR HYBRIDE SMC / PRICE ACTION — M15 -> M5 -> M1
# ============================================================

def _next_m15_close(now):
    """Retourne la prochaine clôture M15 à partir de l'heure UTC actuelle."""
    base = now.replace(second=0, microsecond=0)
    minutes_to_next = 15 - (base.minute % 15)
    return base + timedelta(minutes=minutes_to_next)


def _m15_slot_key(symbol, now):
    next_close = _next_m15_close(now)
    return f"{symbol}_{next_close.strftime('%H%M')}"


def _set_monitoring_state(symbol, **updates):
    with _state_lock:
        state = _monitoring_state.setdefault(symbol, {})
        state.update(updates)


def _get_monitoring_state(symbol):
    with _state_lock:
        return dict(_monitoring_state.get(symbol, {}))


def _reset_monitoring(symbol, reason=None):
    previous = _get_monitoring_state(symbol)
    slot_key = previous.get("slot_key")
    _set_monitoring_state(
        symbol,
        active_monitoring=False,
        slot_key=slot_key,
        expires_at=None,
        direction=None,
        zone=None,
        structure=None,
        last_m1_scan=0.0,
    )
    if reason:
        logging.info("Fenêtre M1 fermée pour %s : %s", symbol, reason)


def _macro_bias_from_m15(structure):
    """Détermine un biais unique BUY/SELL à partir de la structure M15."""
    if not structure:
        return None

    trend = structure.get("trend")
    if trend == "HAUSSIERE":
        return "BUY"
    if trend == "BAISSIERE":
        return "SELL"

    events = structure.get("structure_events") or []
    for event in reversed(events):
        event_type = event.get("type")
        if event_type == "CHoCH_BULLISH":
            return "BUY"
        if event_type == "CHoCH_BEARISH":
            return "SELL"
        if event_type == "BOS_BULLISH":
            return "BUY"
        if event_type == "BOS_BEARISH":
            return "SELL"
    return None


def get_m15_market_structure(symbol):
    """Cartographie SMC M15 : BOS, CHoCH, Order Blocks et FVG."""
    df = fetch_biquote_ohlcv(symbol, timeframe="15m", count=M15_STRUCTURE_BARS)
    if df.empty or len(df) < 80:
        return None

    closed = df.iloc[:-1].copy()
    closed["atr14"] = calculate_atr(closed)
    atr = closed["atr14"].iloc[-1]
    if pd.isna(atr) or float(atr) <= 0:
        return None

    pivot_highs, pivot_lows = find_pivots(closed, PIVOT_WINDOW)
    trend = determine_dow_trend(pivot_highs, pivot_lows)
    events = detect_bos_choch(closed, pivot_highs, pivot_lows)
    obs_buy, obs_sell = detect_order_blocks(closed, events)
    fvg_buy, fvg_sell = detect_fvg(closed)

    structure = {
        "df": closed,
        "atr14": float(atr),
        "pivot_highs": pivot_highs,
        "pivot_lows": pivot_lows,
        "trend": trend,
        "structure_events": events,
        "order_blocks_buy": obs_buy,
        "order_blocks_sell": obs_sell,
        "fvg_buy": fvg_buy,
        "fvg_sell": fvg_sell,
    }
    structure["bias"] = _macro_bias_from_m15(structure)
    return structure


def _valid_m15_zones(structure, direction):
    if not structure or direction not in {"BUY", "SELL"}:
        return []
    zones = []
    if direction == "BUY":
        zones.extend(structure.get("order_blocks_buy", []))
        zones.extend(structure.get("fvg_buy", []))
    else:
        zones.extend(structure.get("order_blocks_sell", []))
        zones.extend(structure.get("fvg_sell", []))
    return sorted(zones, key=lambda z: int(z.get("origin_index", -1)))


def _find_active_m15_zone(structure, direction, price):
    zones = _valid_m15_zones(structure, direction)
    for zone in reversed(zones):
        if price_in_zone(price, zone):
            return zone
    return None


def _m1_volume_column(df):
    for column in ("v", "volume", "vol"):
        if column in df.columns:
            return column
    return None


def _prepare_m1_microstructure(df):
    """Normalise volume/VWAP sans dépendre d'un motif de chandelle classique."""
    if df.empty:
        return df

    result = df.copy()
    volume_column = _m1_volume_column(result)
    if volume_column is None:
        return pd.DataFrame()

    result["_volume"] = pd.to_numeric(result[volume_column], errors="coerce")
    result = result.dropna(subset=["_volume"]).copy()
    if len(result) < M1_VOLUME_AVG_PERIOD + 2:
        return pd.DataFrame()

    if "vwap" in result.columns:
        result["_vwap"] = pd.to_numeric(result["vwap"], errors="coerce")

    if "_vwap" not in result.columns or result["_vwap"].isna().all():
        typical = (result["high"] + result["low"] + result["close"]) / 3.0
        volume_sum = result["_volume"].cumsum()
        result["_vwap"] = (typical * result["_volume"]).cumsum() / volume_sum.replace(0, pd.NA)

    result["_volume_avg10"] = result["_volume"].shift(1).rolling(
        M1_VOLUME_AVG_PERIOD,
        min_periods=M1_VOLUME_AVG_PERIOD,
    ).mean()
    return result.dropna(subset=["_vwap", "_volume_avg10"]).reset_index(drop=True)


def detect_m1_clairvoyance(df_m1, direction, zone):
    """Déclencheur M1 : climax volume + rejet VWAP dans la zone M15."""
    prepared = _prepare_m1_microstructure(df_m1)
    if prepared.empty:
        return None

    candle = prepared.iloc[-2]
    previous_volume_average = float(candle["_volume_avg10"])
    volume = float(candle["_volume"])
    if previous_volume_average <= 0:
        return None

    volume_climax = volume > M1_VOLUME_CLIMAX_MULTIPLIER * previous_volume_average
    total_range = candle_range(candle)
    if total_range <= 0:
        return None

    if direction == "BUY":
        rejection_wick = min(float(candle["open"]), float(candle["close"])) - float(candle["low"])
        wick_ratio_value = max(0.0, rejection_wick) / total_range
        vwap_rejection = (
            float(candle["low"]) <= float(candle["_vwap"]) <= float(candle["high"])
            and float(candle["close"]) > float(candle["_vwap"])
        )
    else:
        rejection_wick = float(candle["high"]) - max(float(candle["open"]), float(candle["close"]))
        wick_ratio_value = max(0.0, rejection_wick) / total_range
        vwap_rejection = (
            float(candle["low"]) <= float(candle["_vwap"]) <= float(candle["high"])
            and float(candle["close"]) < float(candle["_vwap"])
        )

    wick_rejection = wick_ratio_value > M1_REJECTION_WICK_MIN_RATIO
    if not (volume_climax and vwap_rejection and wick_rejection):
        return None

    if not price_in_zone(float(candle["close"]), zone) and not (
        float(candle["low"]) <= float(zone["high_band"])
        and float(candle["high"]) >= float(zone["low_band"])
    ):
        return None

    return {
        "direction": direction,
        "entry_price": float(candle["close"]),
        "candle_timestamp": str(candle["timestamp"]),
        "volume": volume,
        "volume_avg10": previous_volume_average,
        "volume_ratio": volume / previous_volume_average,
        "vwap": float(candle["_vwap"]),
        "wick_ratio": wick_ratio_value,
        "zone": zone,
    }


def _build_anticipated_signal(symbol, structure, trigger, slot_key):
    direction = trigger["direction"]
    entry = float(trigger["entry_price"])
    zone = trigger["zone"]
    atr = float(structure["atr14"])

    if direction == "BUY":
        structure_price = float(zone["low_band"])
        sl = structure_price - (SL_ATR_MULTIPLIER * atr)
        if sl >= entry:
            sl = entry - (SL_ATR_MULTIPLIER * atr)
        risk = entry - sl
        tp1, tp2, tp3 = entry + risk, entry + 2 * risk, entry + 3 * risk
    else:
        structure_price = float(zone["high_band"])
        sl = structure_price + (SL_ATR_MULTIPLIER * atr)
        if sl <= entry:
            sl = entry + (SL_ATR_MULTIPLIER * atr)
        risk = sl - entry
        tp1, tp2, tp3 = entry - risk, entry - 2 * risk, entry - 3 * risk

    if risk <= 0:
        return None

    return {
        "signal_id": slot_key,
        "symbol": symbol,
        "direction": direction,
        "strategy": "SMC_M15_M5_M1_ANTICIPATED",
        "pattern": "VOLUME_CLIMAX_VWAP_REJECTION",
        "reason": (
            "ALERTE ANTICIPÉE — Volume Climax > 2.5x moyenne M1(10) + "
            "rejet VWAP + mèche > 50% dans zone M15"
        ),
        "trend_h1": structure.get("trend"),
        "trend_m15": structure.get("trend"),
        "bias_m15": direction,
        "candle_timestamp": trigger["candle_timestamp"],
        "created_at": utc_now_iso(),
        "atr_h1": atr,
        "atr_m15": atr,
        "zone": zone,
        "order_block": zone if zone.get("source") == "OB" else None,
        "fvg": zone if zone.get("source") == "FVG" else None,
        "score": 3,
        "score_max": 3,
        "volume": trigger["volume"],
        "volume_avg10": trigger["volume_avg10"],
        "volume_ratio": trigger["volume_ratio"],
        "vwap": trigger["vwap"],
        "wick_ratio": trigger["wick_ratio"],
        "entry_price": entry,
        "initial_sl": sl,
        "current_sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "risk": risk,
        "anticipation": True,
        "anticipation_text": "[ALERTE ANTICIPÉE - AVANCE DE 3 MIN] Entrée chirurgicale détectée",
        "m15_slot_key": slot_key,
    }


def scan_market_m15(symbol, now=None):
    """Construit le contexte macro M15 et l'expose à la sentinelle M5."""
    now = now or utc_now()
    structure = get_m15_market_structure(symbol)
    if structure is None:
        logging.warning("Structure SMC M15 indisponible pour %s.", symbol)
        return None

    direction = structure.get("bias")
    if direction not in {"BUY", "SELL"}:
        logging.info("Biais M15 neutre pour %s : aucune fenêtre de tir.", symbol)
        return None

    live_price = fetch_biquote_live_price(symbol)
    if live_price is None:
        return None

    zone = _find_active_m15_zone(structure, direction, live_price)
    if zone is None:
        return None

    return {
        "structure": structure,
        "direction": direction,
        "live_price": float(live_price),
        "zone": zone,
        "slot_key": _m15_slot_key(symbol, now),
    }


def scan_market_m5(symbol, now=None):
    """Sentinelle M5 : la fenêtre M1 ne peut s'ouvrir qu'à :10/:25/:40/:55."""
    now = now or utc_now()
    if now.minute % 15 != M1_MONITOR_START_MINUTE:
        return False

    slot_key = _m15_slot_key(symbol, now)
    with app.app_context():
        # Vérification PostgreSQL avant toute analyse du cycle.
        if signal_already_processed(slot_key):
            _reset_monitoring(symbol, "créneau déjà traité en PostgreSQL")
            return False

    state = _get_monitoring_state(symbol)
    if state.get("slot_key") == slot_key and state.get("active_monitoring"):
        return True

    context = scan_market_m15(symbol, now)
    if context is None:
        _set_monitoring_state(symbol, active_monitoring=False, slot_key=slot_key)
        logging.info("Fenêtre M1 non ouverte pour %s : prix hors OB/FVG M15 ou biais absent.", symbol)
        return False

    _set_monitoring_state(
        symbol,
        active_monitoring=True,
        slot_key=slot_key,
        expires_at=now.replace(second=0, microsecond=0) + timedelta(minutes=3),
        direction=context["direction"],
        zone=context["zone"],
        structure=context["structure"],
        last_m1_scan=0.0,
    )
    logging.info(
        "FENÊTRE DE TIR OUVERTE %s : %s %s, zone=%s, expiration=%s.",
        symbol,
        context["direction"],
        slot_key,
        context["zone"].get("kind"),
        now.replace(second=0, microsecond=0) + timedelta(minutes=3),
    )
    return True


def scan_market_m1(symbol, now=None):
    """Scan M1 toutes les 10 secondes pendant la fenêtre :10 -> :13."""
    now = now or utc_now()
    state = _get_monitoring_state(symbol)
    if not state.get("active_monitoring"):
        return None

    slot_key = state.get("slot_key")
    if not slot_key:
        _reset_monitoring(symbol, "clé de créneau absente")
        return None

    if now.minute % 15 < M1_MONITOR_START_MINUTE or now.minute % 15 >= M1_MONITOR_END_MINUTE:
        _reset_monitoring(symbol, "fin de fenêtre M1")
        return None

    if state.get("expires_at") and now >= state["expires_at"]:
        _reset_monitoring(symbol, "expiration de la fenêtre M1")
        return None

    with app.app_context():
        # Verrou absolu : aucune analyse M1 si le créneau est déjà traité.
        if signal_already_processed(slot_key):
            _reset_monitoring(symbol, "signal déjà traité en PostgreSQL")
            return None

    last_scan = float(state.get("last_m1_scan") or 0.0)
    if time.time() - last_scan < M1_SCAN_INTERVAL_SECONDS:
        return None
    _set_monitoring_state(symbol, last_m1_scan=time.time())

    df_m1 = fetch_biquote_ohlcv(symbol, timeframe="1m", count=M1_TRIGGER_BARS)
    if df_m1.empty or len(df_m1) < M1_VOLUME_AVG_PERIOD + 3:
        return None

    trigger = detect_m1_clairvoyance(df_m1, state["direction"], state["zone"])
    if trigger is None:
        return None

    signal = _build_anticipated_signal(
        symbol,
        state["structure"],
        trigger,
        slot_key,
    )
    if signal is None:
        return None

    with app.app_context():
        # Race-condition PostgreSQL : un seul worker peut gagner l'unicité.
        if signal_already_processed(slot_key):
            _reset_monitoring(symbol, "doublon PostgreSQL détecté avant émission")
            return None

        trade_id = create_active_trade(signal, send_standard_alert=False)
        if trade_id is None:
            # create_active_trade rollbacke si PostgreSQL refuse l'unicité.
            if signal_already_processed(slot_key):
                _reset_monitoring(symbol, "clé déjà consommée par un autre cycle")
            return None

        # Message spécifique : alerte anticipée uniquement après persistance réussie.
        send_trade_alert(signal)

    _reset_monitoring(symbol, "déclencheur M1 validé et signal émis")
    logging.info(
        "ALERTE ANTICIPÉE %s %s %s — volume %.2fx moyenne10, VWAP=%s, mèche=%.1f%%.",
        symbol,
        signal["direction"],
        slot_key,
        signal["volume_ratio"],
        format_price(symbol, signal["vwap"]),
        signal["wick_ratio"] * 100,
    )
    return trade_id


# ============================================================
# STATE MACHINE — SUIVI 10 SECONDES
# ============================================================

def _trade_event_message(trade, event_name, price, extra=""):
    symbol = trade.get("symbol", "INCONNU")
    direction = trade.get("direction", "INCONNUE")
    emoji = "🟢" if direction == "BUY" else "🔴"
    entry = trade.get("entry_price")
    distance = signed_pips(symbol, direction, entry, price) if entry is not None else 0
    return (
        f"{event_name}\n\n"
        f"Signal : #{trade.get('id_signal', trade.get('signal_display_id', 'N/D'))}\n"
        f"{emoji} {direction} {symbol}\n\n"
        f"Entry : {format_price(symbol, entry)}\n"
        f"Prix atteint : {format_price(symbol, price)}\n"
        f"Distance : {format_pips(distance)}\n"
        + extra
        + f"\n🕐 {utc_now().strftime('%Y-%m-%d %H:%M:%S UTC')}"
    )


def track_active_trades():
    logging.info("Thread de suivi des trades démarré avec persistance PostgreSQL.")
    while True:
        try:
            with app.app_context():
                active_trades = ActiveTrade.query.filter(ActiveTrade.status.in_(["ACTIVE", "TP1_HIT", "TP2_HIT"])).all()
                if active_trades:
                    prices = {}
                    for trade in active_trades:
                        if trade.symbol not in prices:
                            prices[trade.symbol] = fetch_biquote_live_price(trade.symbol)

                    for trade in list(active_trades):
                        try:
                            price = prices.get(trade.symbol)
                            if price is None:
                                continue

                            entry = float(trade.entry_price)
                            current_sl = float(trade.current_sl)
                            tp1, tp2, tp3 = float(trade.tp1), float(trade.tp2), float(trade.tp3)
                            status = trade.status

                            if trade.direction == "BUY":
                                sl_hit = price <= current_sl
                                tp1_hit = price >= tp1
                                tp2_hit = price >= tp2
                                tp3_hit = price >= tp3
                            else:
                                sl_hit = price >= current_sl
                                tp1_hit = price <= tp1
                                tp2_hit = price <= tp2
                                tp3_hit = price <= tp3

                            trade_view = active_trade_to_dict(trade)
                            history = load_json(TRADE_HISTORY_FILE)
                            trade_view.update(history.get(trade.id, {}))

                            if sl_hit:
                                record_trade_event(trade.id, "SL_HIT", price)
                                send_telegram_message(_trade_event_message(trade_view, "🛑 STOP LOSS ATTEINT", price, "❌ Trade clôturé en SL."))
                                close_trade_in_history(trade.id, "SL", price)
                                db.session.delete(trade)
                                db.session.commit()
                                continue

                            if tp1_hit and status == "ACTIVE":
                                trade.status = "TP1_HIT"
                                trade.current_sl = entry
                                record_trade_event(trade.id, "TP1_HIT", tp1)
                                record_trade_event(trade.id, "BREAK_EVEN", entry)
                                trade_view.update({"status": trade.status, "current_sl": trade.current_sl})
                                send_trade_financial_event_report(trade_view, "TP1_HIT", tp1)
                                send_telegram_message(_trade_event_message(trade_view, "🎯 TP1 ATTEINT", tp1, "🛡️ SL → Break-Even."))
                                db.session.commit()
                                status = "TP1_HIT"

                            if tp2_hit and status == "TP1_HIT":
                                trade.status = "TP2_HIT"
                                trade.current_sl = tp1
                                record_trade_event(trade.id, "TP2_HIT", tp2)
                                record_trade_event(trade.id, "SL_TO_TP1", tp1)
                                trade_view.update({"status": trade.status, "current_sl": trade.current_sl})
                                send_trade_financial_event_report(trade_view, "TP2_HIT", tp2)
                                send_telegram_message(_trade_event_message(trade_view, "🎯 TP2 ATTEINT", tp2, "🛡️ SL → TP1."))
                                db.session.commit()
                                status = "TP2_HIT"

                            if tp3_hit and status in ("ACTIVE", "TP1_HIT", "TP2_HIT"):
                                record_trade_event(trade.id, "TP3_HIT", tp3)
                                trade_view.update({"status": trade.status})
                                send_telegram_message(_trade_event_message(trade_view, "🏆 TP3 ATTEINT", tp3, "✅ Trade clôturé en TP3."))
                                close_trade_in_history(trade.id, "TP3", price)
                                db.session.delete(trade)
                                db.session.commit()
                        except Exception as exc:
                            db.session.rollback()
                            logging.exception("Erreur traitement trade %s : %s", getattr(trade, "id", "N/D"), exc)
        except Exception as exc:
            logging.exception("Erreur suivi PostgreSQL : %s", exc)
        time.sleep(10)


# ============================================================
# FERMETURE HEBDOMADAIRE DES MARCHÉS
# ============================================================

def close_non_btc_positions_for_weekend():
    """Ferme les positions XAU/EUR/GBP à partir de vendredi 19:00 UTC.

    BTCUSD reste volontairement ouvert car il est traité comme marché 24/7.
    """
    with app.app_context():
        active_trades = ActiveTrade.query.filter(ActiveTrade.symbol != "BTCUSD").all()
        if not active_trades:
            return 0

        closed_count = 0
        prices = {}
        for trade in active_trades:
            if trade.symbol not in prices:
                prices[trade.symbol] = fetch_biquote_live_price(trade.symbol)

        for trade in list(active_trades):
            price = prices.get(trade.symbol)
            if price is None:
                logging.warning("Prix de fermeture indisponible pour %s (%s).", trade.symbol, trade.id)
                continue
            try:
                trade_view = active_trade_to_dict(trade)
                record_trade_event(trade.id, "WEEKEND_MARKET_CLOSE", price)
                close_trade_in_history(trade.id, "MARKET_CLOSED", price)
                send_telegram_message(
                    f"🔒 Fermeture hebdomadaire sur {trade.symbol}\n"
                    f"Signal : #{trade.id}\n"
                    f"Prix : {format_price(trade.symbol, price)}\n"
                    "Position clôturée avant la fermeture du marché."
                )
                db.session.delete(trade)
                closed_count += 1
            except Exception as exc:
                db.session.rollback()
                logging.exception("Erreur fermeture hebdomadaire du trade %s : %s", trade.id, exc)
        db.session.commit()
        return closed_count


# ============================================================
# RAPPORT HEBDOMADAIRE
# ============================================================

def _parse_trade_datetime(value):
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except (TypeError, ValueError):
        return None


def _current_week_bounds(reference_time=None):
    reference_time = reference_time or utc_now()
    monday = (reference_time - timedelta(days=reference_time.weekday())).replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )
    next_monday = monday + timedelta(days=7)
    return monday, next_monday


def _format_weekly_trade_detail(trade):
    symbol = trade.get("symbol", "INCONNU")
    direction = trade.get("direction", "INCONNUE")
    result = trade.get("result") or "OUVERT"

    lines = [
        f"• {symbol} {direction}",
        f"  Stratégie : {trade.get('strategy', 'INCONNUE')}",
        f"  Pattern : {trade.get('pattern', 'INCONNU')}",
        f"  Entrée : {format_price(symbol, trade.get('entry_price', 0))}",
        f"  SL initial : {format_price(symbol, trade.get('initial_sl', 0))}",
        f"  TP1 : {format_price(symbol, trade.get('tp1', 0))}",
        f"  TP2 : {format_price(symbol, trade.get('tp2', 0))}",
        f"  TP3 : {format_price(symbol, trade.get('tp3', 0))}",
        f"  Résultat : {result}",
        f"  Créé : {trade.get('created_at', 'INCONNU')}",
        f"  Clôturé : {trade.get('closed_at', '—')}",
        f"  Prix clôture : {format_price(symbol, trade.get('close_price', 0)) if trade.get('close_price') is not None else '—'}",
    ]

    events = trade.get("events") or []
    if events:
        lines.append("  Événements :")
        for event in events:
            event_name = event.get("event", "INCONNU")
            event_time = event.get("timestamp", "")
            event_price = event.get("price")
            if event_price is not None:
                lines.append(
                    f"    - {event_name} à {format_price(symbol, event_price)} ({event_time})"
                )
            else:
                lines.append(f"    - {event_name} ({event_time})")

    return "\n".join(lines)


def generate_weekly_report(reference_time=None):
    """Envoie à 22:00 UTC l'historique détaillé des trades de la semaine."""
    now = reference_time or utc_now()
    week_key = f"{now.isocalendar().year}-W{now.isocalendar().week:02d}"

    reports = load_json(WEEKLY_REPORTS_FILE)
    if week_key in reports:
        return False

    week_start, week_end = _current_week_bounds(now)
    history = load_json(TRADE_HISTORY_FILE)

    weekly_trades = []
    for trade in history.values():
        created_at = _parse_trade_datetime(trade.get("created_at"))
        if created_at is None:
            continue
        if week_start <= created_at < week_end:
            weekly_trades.append(trade)

    weekly_trades.sort(
        key=lambda trade: _parse_trade_datetime(trade.get("created_at"))
        or datetime.min.replace(tzinfo=timezone.utc)
    )

    tp3_count = sum(1 for trade in weekly_trades if trade.get("result") == "TP3")
    sl_count = sum(1 for trade in weekly_trades if trade.get("result") == "SL")
    market_close_count = sum(
        1 for trade in weekly_trades if trade.get("result") == "MARKET_CLOSED"
    )
    open_count = sum(1 for trade in weekly_trades if trade.get("result") is None)

    reports[week_key] = {
        "created_at": utc_now_iso(),
        "week_start": week_start.isoformat(),
        "week_end": week_end.isoformat(),
        "total_trades": len(weekly_trades),
        "tp3": tp3_count,
        "sl": sl_count,
        "market_closed": market_close_count,
        "still_open": open_count,
        "trade_ids": [trade.get("trade_id") for trade in weekly_trades],
    }
    save_json(WEEKLY_REPORTS_FILE, reports)

    if not telegram_is_configured():
        logging.warning("Rapport hebdomadaire non envoyé : Telegram canal non configuré.")
        return True

    summary = (
        "📊 RAPPORT HEBDOMADAIRE — HISTORIQUE DÉTAILLÉ\n"
        f"Semaine : {week_start.strftime('%Y-%m-%d')} → "
        f"{(now).strftime('%Y-%m-%d')}\n\n"
        f"Trades pris : {len(weekly_trades)}\n"
        f"TP3 : {tp3_count}\n"
        f"SL : {sl_count}\n"
        f"Fermeture marché : {market_close_count}\n"
        f"BTC encore ouvert : {open_count}\n\n"
        "DÉTAIL DES TRADES\n"
    )

    messages = []
    current = summary
    for trade in weekly_trades:
        detail = _format_weekly_trade_detail(trade)
        block = f"\n{detail}\n"
        # Telegram limite les messages texte à environ 4096 caractères.
        if len(current) + len(block) > 3900:
            messages.append(current)
            current = block.lstrip("\n")
        else:
            current += block

    if not weekly_trades:
        current += "Aucun trade pris cette semaine."

    if current:
        messages.append(current)

    for message in messages:
        send_telegram_message(message)

    logging.info(
        "Rapport hebdomadaire %s envoyé : %s trades.",
        week_key,
        len(weekly_trades),
    )
    return True


# ============================================================
# SCHEDULER — SYNCHRONISATION M15 / M5 / M1
# ============================================================

def main_scheduler():
    logging.info("Scheduler hybride M15 -> M5 -> M1 démarré.")
    last_m5_close_slot = None
    last_m1_tick = 0.0
    last_weekend_close_date = None
    last_weekly_report_date = None

    while True:
        try:
            now = utc_now()
            minute_mod = now.minute % 15

            with app.app_context():
                # 1) SENTINELLE M5 : déclenchement strict à chaque clôture M5.
                m5_slot = now.replace(
                    minute=(now.minute // 5) * 5,
                    second=0,
                    microsecond=0,
                )
                if now.second >= 1 and m5_slot != last_m5_close_slot:
                    for symbol in SYMBOLS:
                        scan_market_m5(symbol, now)
                    last_m5_close_slot = m5_slot

                # 2) M1 : surveillance intensive toutes les 10 secondes uniquement
                # pendant une fenêtre déjà ouverte par la sentinelle M5.
                if time.time() - last_m1_tick >= M1_SCAN_INTERVAL_SECONDS:
                    for symbol in SYMBOLS:
                        state = _get_monitoring_state(symbol)
                        if state.get("active_monitoring"):
                            scan_market_m1(symbol, now)
                    last_m1_tick = time.time()

                # 3) Fermeture hebdomadaire des actifs non-BTC.
                if (
                    now.weekday() == 4
                    and now.hour >= 19
                    and last_weekend_close_date != now.date()
                ):
                    closed_count = close_non_btc_positions_for_weekend()
                    logging.info(
                        "Contrôle fermeture vendredi effectué : %s position(s) clôturée(s).",
                        closed_count,
                    )
                    last_weekend_close_date = now.date()

                # 4) Rapport hebdomadaire.
                if (
                    now.weekday() == 4
                    and now.hour >= 22
                    and last_weekly_report_date != now.date()
                ):
                    generate_weekly_report(now)
                    last_weekly_report_date = now.date()

                # 5) Nettoyage strict des fenêtres qui auraient dépassé :13.
                if minute_mod >= M1_MONITOR_END_MINUTE or minute_mod < M1_MONITOR_START_MINUTE:
                    for symbol in SYMBOLS:
                        state = _get_monitoring_state(symbol)
                        if state.get("active_monitoring"):
                            _reset_monitoring(symbol, "fenêtre M1 hors plage :10 -> :13")

            time.sleep(1)

        except Exception as exc:
            logging.exception("Erreur scheduler hybride : %s", exc)
            time.sleep(5)


# ============================================================
# THREADS + FLASK
# ============================================================

def start_trading_threads():
    global _threads_started

    with _threads_lock:
        if _threads_started:
            return

        _threads_started = True

        with app.app_context():
            active_count = ActiveTrade.query.count()
            processed_count = ProcessedSignal.query.count()
        logging.info(
            "État PostgreSQL restauré au démarrage : %s trade(s) actif(s), %s signal(s) déjà traité(s).",
            active_count,
            processed_count,
        )
        logging.info("DÉMARRAGE DES THREADS DE TRADING...")

        threading.Thread(
            target=track_active_trades,
            name="trade-tracker",
            daemon=True,
        ).start()

        threading.Thread(
            target=main_scheduler,
            name="market-scheduler",
            daemon=True,
        ).start()

        threading.Thread(
            target=telegram_polling_loop,
            name="telegram-controller",
            daemon=True,
        ).start()

        if TRADE_NEWS:
            threading.Thread(
                target=fundamental_news_loop,
                name="t_fundamental",
                daemon=True,
            ).start()

        threading.Thread(
            target=subscription_loop,
            name="subscription-manager",
            daemon=True,
        ).start()

        logging.info("Threads de trading et SaaS démarrés.")


# Les threads doivent démarrer au chargement de l'application.
# Avec Gunicorn, il est possible qu'aucune requête HTTP ne soit reçue
# avant le premier cycle de trading : utiliser @app.before_request ici
# rendrait donc le scheduler et le suivi des trades dépendants du healthcheck.
# Le verrou _threads_lock garantit un démarrage unique par processus Gunicorn.


@app.route("/")
def home():
    return render_page(
        "Accueil",
        "<div class='card'><p>Bienvenue sur NOVA TRADE IA.</p><p>Accédez à votre espace utilisateur pour gérer votre compte et votre abonnement.</p><a href='/register'>Créer un compte</a><a href='/login'>Se connecter</a></div>"
    )


@app.route("/health")
def health_check():
    strategies = []
    if STRAT_REVERSAL:
        strategies.append("REVERSAL")
    if STRAT_PULLBACK:
        strategies.append("PULLBACK")
    if STRAT_BREAKOUT:
        strategies.append("BREAKOUT")

    return {
        "status": "healthy",
        "provider": "biquote",
        "symbols": SYMBOLS,
        "strategies": strategies,
        "telegram_configured": telegram_is_configured(),
        "telegram_owner_configured": telegram_owner_is_configured(),
        "trade_news": TRADE_NEWS,
        "saas_database": bool(DATABASE_URL),
        "subscription_price_xof_configured": SUBSCRIPTION_PRICE_XOF > 0,
        "fundamental_symbols": FUNDAMENTAL_SYMBOLS,
        "structure_timeframe": "M15",
        "sentinel_timeframe": "M5",
        "signal_timeframe": "M1",
        "pivot_window": PIVOT_WINDOW,
        "zone_atr": ZONE_ATR_MULTIPLIER,
        "sl_atr": SL_ATR_MULTIPLIER,
        "tp_rr": ["1:1", "1:2", "1:3"],
        "display_unit": "pips",
        "trade_poll_seconds": 10,
        "timestamp": utc_now_iso(),
    }, 200


# ============================================================
# DÉMARRAGE DE L'APPLICATION
# ============================================================

# IMPORTANT POUR RAILWAY + GUNICORN :
# main:app est importé par Gunicorn sans passer par __main__.
# Le démarrage des threads doit donc avoir lieu après la définition
# complète des fonctions, mais avant que Gunicorn commence à servir.
logging.info("Initialisation de NOVA TRADE AI...")
start_trading_threads()
logging.info("Application NOVA TRADE AI prête sur le port %s.", os.environ.get("PORT", "8080"))


# ============================================================
# LANCEMENT LOCAL
# ============================================================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    app.run(
        host="0.0.0.0",
        port=port,
    )