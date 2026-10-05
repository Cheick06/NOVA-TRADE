import os
import time
import requests
import pandas as pd
import pandas_ta as ta
import yfinance as yf
import schedule
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# --- CONFIGURATION VIA VARIABLES D'ENVIRONNEMENT ---
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
BOT_TOKEN = os.environ.get("BOT_TOKEN")
ADMIN = os.environ.get("ADMIN")

# --- PARAMÈTRES DE CAPITALISATION & RISQUE ---
CAPITAL_INITIAL = 10000.0  # Modifiez selon votre capital de simulation
RISQUE_PAR_TRADE = 0.01    # 1% strict par position

# --- CONFIGURATION DES ACTIFS ---
CONFIG_ACTIFS = {
    "BTCUSDT": {"source": "binance", "keltner_mult": 2.5, "atr_mult_sl": 2.0, "tf": "15m", "pip_value": 1.0},
    "EURUSD=X": {"source": "yahoo", "keltner_mult": 1.5, "atr_mult_sl": 1.5, "tf": "5m", "pip_value": 100000.0}, # Lot standard Forex
    "GC=F": {"source": "yahoo", "keltner_mult": 2.0, "atr_mult_sl": 2.0, "tf": "5m", "pip_value": 100.0}       # 1 contrat Or = 100 onces
}

# Mémoire globale pour empêcher la répétition des signaux
DERNIERS_TIMESTAMPS = {actif: None for actif in CONFIG_ACTIFS}

def send_telegram_message(message):
    """Envoie une notification Telegram au format Markdown."""
    if not BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://telegram.org{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}
    try:
        requests.post(url, json=payload, timeout=5)
    except Exception as e:
        print(f"⚠️ Erreur réseau Telegram : {e}")

def obtenir_donnees(actif, config):
    """Récupère et normalise les données OHLC de manière isolée et sécurisée."""
    try:
        if config["source"] == "binance":
            url = f"https://binance.com{actif}&interval={config['tf']}&limit=210"
            res = requests.get(url, timeout=5).json()
            if not isinstance(res, list):
                return pd.DataFrame()
            
            df = pd.DataFrame(res, columns=['timestamp', 'open', 'high', 'low', 'close', 'v', 'ct', 'q', 'n', 'tb', 'tq', 'i'])
            df = df[['timestamp', 'open', 'high', 'low', 'close']].copy()
            df[['open', 'high', 'low', 'close']] = df[['open', 'high', 'low', 'close']].apply(pd.to_numeric)
            df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
            return df

        elif config["source"] == "yahoo":
            ticker = yf.Ticker(actif)
            df = ticker.history(period="3d", interval=config["tf"])
            if df.empty:
                return pd.DataFrame()
            
            df = df.reset_index()
            df.columns = [c.lower() for c in df.columns]
            df = df[['date', 'open', 'high', 'low', 'close']].rename(columns={'date': 'datetime'})
            return df
            
    except Exception as e:
        print(f"❌ Erreur critique de flux pour {actif} : {e}")
        return pd.DataFrame()

def analyser_strategie(actif, config):
    """Exécute l'analyse quantitative de la stratégie Breakout."""
    global DERNIERS_TIMESTAMPS
    
    df = obtenir_donnees(actif, config)
    if df.empty or len(df) < 205:
        return

    # 1. Génération vectorielle accélérée des indicateurs
    df["EMA200"] = ta.ema(df["close"], length=200)
    df["ATR"] = ta.atr(df["high"], df["low"], df["close"], length=14)
    
    mult = config["keltner_mult"]
    keltner = ta.kc(df["high"], df["low"], df["close"], length=20, scalar=mult)
    
    kc_upper_col = f"KCUe_20_{mult}"
    kc_lower_col = f"KCLe_20_{mult}"

    # 2. Isolement des bougies fermées pour éliminer le repeint (Lag-free execution)
    bougie_actuelle = df.iloc[-2]   # Clôture officielle récente
    bougie_precedente = df.iloc[-3] # Contexte de validation
    timestamp_actuel = bougie_actuelle['datetime']

    # Anti-Spam de sécurité
    if DERNIERS_TIMESTAMPS[actif] == timestamp_actuel:
        return
        
    close_actuel = bougie_actuelle["close"]
    ema200 = bougie_actuelle["EMA200"]
    atr = bougie_actuelle["ATR"]
    kc_upper = keltner[kc_upper_col].iloc[-2]
    kc_lower = keltner[kc_lower_col].iloc[-2]

    # Filtre de session intraday pour les marchés traditionnels (Forex/Gold)
    if config["source"] == "yahoo":
        if datetime.utcnow().time() >= datetime.strptime("17:45", "%H:%M").time():
            return

    # 3. Moteur mathématique des signaux de cassure directionnelle
    # ─── SIGNAL LONG (ACHAT) ───
    if close_actuel > ema200 and bougie_precedente["close"] <= kc_upper and close_actuel > kc_upper:
        sl = close_actuel - (config["atr_mult_sl"] * atr)
        tp = close_actuel + (config["atr_mult_sl"] * 2.0 * atr)
        
        # Calcul de la taille de lot idéale basée sur la formule du risque institutionnel
        distance_sl = abs(close_actuel - sl)
        taille_position = (CAPITAL_INITIAL * RISQUE_PAR_TRADE) / (distance_sl * config["pip_value"]) if distance_sl > 0 else 0
        
        DERNIERS_TIMESTAMPS[actif] = timestamp_actuel
        msg = f"🚀 *[LONG] SIGNAL D'ACHAT ALGORITHMIQUE V3*\n\n" \
              f"📊 *Actif :* `{actif}`\n" \
              f"💰 *Entrée Déclenchée :* `{close_actuel:.5f}`\n" \
              f"🎯 *Objectif (TP) :* `{tp:.5f}`\n" \
              f"🛑 *Stop-Loss (SL) :* `{sl:.5f}`\n\n" \
              f"⚙️ *Gestion du Risque (1%) :*\n" \
              f"💼 Taille suggérée du Lot : `{taille_position:.2f}`\n" \
              f"⏰ Analyse exécutée le : {datetime.now().strftime('%d/%m %H:%M')}"
        send_telegram_message(msg)

    # ─── SIGNAL SHORT (VENTE) ───
    elif close_actuel < ema200 and bougie_precedente["close"] >= kc_lower and close_actuel < kc_lower:
        sl = close_actuel + (config["atr_mult_sl"] * atr)
        tp = close_actuel - (config["atr_mult_sl"] * 2.0 * atr)
        
        distance_sl = abs(sl - close_actuel)
        taille_position = (CAPITAL_INITIAL * RISQUE_PAR_TRADE) / (distance_sl * config["pip_value"]) if distance_sl > 0 else 0
        
        DERNIERS_TIMESTAMPS[actif] = timestamp_actuel
        msg = f"📉 *[SHORT] SIGNAL DE VENTE ALGORITHMIQUE V3*\n\n" \
              f"📊 *Actif :* `{actif}`\n" \
              f"💰 *Entrée Déclenchée :* `{close_actuel:.5f}`\n" \
              f"🎯 *Objectif (TP) :* `{tp:.5f}`\n" \
              f"🛑 *Stop-Loss (SL) :* `{sl:.5f}`\n\n" \
              f"⚙️ *Gestion du Risque (1%) :*\n" \
              f"💼 Taille suggérée du Lot : `{taille_position:.2f}`\n" \
              f"⏰ Analyse exécutée le : {datetime.now().strftime('%d/%m %H:%M')}"
        send_telegram_message(msg)

def execution_parallele():
    """Moteur asynchrone qui traite tous les actifs simultanément en multi-threading."""
    print(f"🔄 [{datetime.now().strftime('%H:%M:%S')}] Scan asynchrone multi-marchés...")
    with ThreadPoolExecutor(max_workers=len(CONFIG_ACTIFS)) as executor:
        for actif, config in CONFIG_ACTIFS.items():
            executor.submit(analyser_strategie, actif, config)

# Initialisation du conteneur
send_telegram_message(f"⚡ *Moteur Quantitatif V3 Asynchrone Déployé*\nStatut : En ligne sur Railway\nAdmin actif : @{ADMIN}")

# Planification optimale : vérification toutes les minutes
schedule.every(1).minutes.do(execution_parallele)

if __name__ == "__main__":
    execution_parallele() # Analyse instantanée dès le démarrage
    while True:
        schedule.run_pending()
        time.sleep(1)
