import random
import time
from datetime import datetime, timezone
from threading import Lock

from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

HOST = "0.0.0.0"
PORT = 8080

DATA_LOCK = Lock()

SYMBOLS = ["BTCUSD", "XAUUSD", "EURUSD", "GBPUSD"]

SYMBOL_CONFIG = {
    "BTCUSD": {
        "base": 108500.00,
        "decimals": 2,
        "unit": "USD",
    },
    "XAUUSD": {
        "base": 3860.00,
        "decimals": 2,
        "unit": "USD",
    },
    "EURUSD": {
        "base": 1.17350,
        "decimals": 5,
        "unit": "",
    },
    "GBPUSD": {
        "base": 1.34680,
        "decimals": 5,
        "unit": "",
    },
}

STATE = {
    "bot": {
        "status": "RUNNING",
        "mode": "SIMULATION",
        "last_scan": None,
        "uptime": 0,
        "cycles": 0,
    },
    "markets": {},
    "signals": [],
    "trades": [],
    "journal": [],
    "stats": {
        "total_trades": 24,
        "wins": 17,
        "losses": 7,
        "win_rate": 70.83,
        "profit": 1842.50,
        "profit_factor": 2.31,
    },
}


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def format_price(symbol, value):
    decimals = SYMBOL_CONFIG[symbol]["decimals"]
    return round(float(value), decimals)


def initialize_markets():
    with DATA_LOCK:
        for symbol in SYMBOLS:
            config = SYMBOL_CONFIG[symbol]
            base = config["base"]

            price = base * (1 + random.uniform(-0.0015, 0.0015))

            if symbol in ("BTCUSD", "XAUUSD"):
                atr = price * random.uniform(0.0015, 0.0035)
            else:
                atr = price * random.uniform(0.0005, 0.0012)

            ema20 = price * (1 + random.uniform(-0.001, 0.001))
            ema50 = price * (1 + random.uniform(-0.002, 0.002))

            if ema20 > ema50:
                bias = "BULLISH"
            else:
                bias = "BEARISH"

            STATE["markets"][symbol] = {
                "symbol": symbol,
                "price": format_price(symbol, price),
                "change": round(random.uniform(-1.8, 1.8), 2),
                "change_value": round(random.uniform(-0.8, 0.8), 2),
                "volume": random.randint(800, 9500),
                "timeframe": "M15",
                "bias": bias,
                "bos": "BULLISH_BOS" if bias == "BULLISH" else "BEARISH_BOS",
                "ema20": format_price(symbol, ema20),
                "ema50": format_price(symbol, ema50),
                "atr": format_price(symbol, atr),
                "support": format_price(symbol, price - atr * 2.2),
                "resistance": format_price(symbol, price + atr * 2.2),
                "zone_status": "ACTIVE",
                "updated_at": now_iso(),
            }


def create_demo_signals():
    with DATA_LOCK:
        signals = []

        for symbol in SYMBOLS:
            market = STATE["markets"][symbol]

            if market["bias"] == "BULLISH":
                direction = "BUY"
                entry = market["price"]
                sl = entry - market["atr"] * 1.2
                tp1 = entry + market["atr"] * 1.2
                tp2 = entry + market["atr"] * 2.4
                tp3 = entry + market["atr"] * 3.6
            else:
                direction = "SELL"
                entry = market["price"]
                sl = entry + market["atr"] * 1.2
                tp1 = entry - market["atr"] * 1.2
                tp2 = entry - market["atr"] * 2.4
                tp3 = entry - market["atr"] * 3.6

            risk = abs(entry - sl)
            reward = abs(tp3 - entry)
            rr = reward / risk if risk else 0

            signals.append(
                {
                    "id": f"SIG-{symbol}-001",
                    "symbol": symbol,
                    "direction": direction,
                    "entry": format_price(symbol, entry),
                    "sl": format_price(symbol, sl),
                    "tp1": format_price(symbol, tp1),
                    "tp2": format_price(symbol, tp2),
                    "tp3": format_price(symbol, tp3),
                    "rr": round(rr, 2),
                    "status": "ACTIVE",
                    "strategy": "SMC + PRICE ACTION",
                    "timeframe": "M15 / M5 / M1",
                    "created_at": now_iso(),
                }
            )

        STATE["signals"] = signals


def create_demo_trades():
    with DATA_LOCK:
        STATE["trades"] = [
            {
                "id": "TRD-001",
                "symbol": "XAUUSD",
                "direction": "BUY",
                "entry": 3852.40,
                "sl": 3845.80,
                "tp1": 3859.00,
                "tp2": 3865.60,
                "tp3": 3872.20,
                "status": "TP2_HIT",
                "profit": 126.40,
                "rr": 3.0,
                "opened_at": now_iso(),
            },
            {
                "id": "TRD-002",
                "symbol": "BTCUSD",
                "direction": "SELL",
                "entry": 108720.00,
                "sl": 109180.00,
                "tp1": 108260.00,
                "tp2": 107800.00,
                "tp3": 107340.00,
                "status": "ACTIVE",
                "profit": 84.20,
                "rr": 3.0,
                "opened_at": now_iso(),
            },
            {
                "id": "TRD-003",
                "symbol": "EURUSD",
                "direction": "BUY",
                "entry": 1.17185,
                "sl": 1.17095,
                "tp1": 1.17275,
                "tp2": 1.17365,
                "tp3": 1.17455,
                "status": "TP3_HIT",
                "profit": 242.70,
                "rr": 3.0,
                "opened_at": now_iso(),
            },
            {
                "id": "TRD-004",
                "symbol": "GBPUSD",
                "direction": "SELL",
                "entry": 1.34820,
                "sl": 1.34910,
                "tp1": 1.34730,
                "tp2": 1.34640,
                "tp3": 1.34550,
                "status": "SL",
                "profit": -90.00,
                "rr": 3.0,
                "opened_at": now_iso(),
            },
        ]


def create_demo_journal():
    with DATA_LOCK:
        STATE["journal"] = [
            {
                "date": "2026-10-03",
                "symbol": "XAUUSD",
                "direction": "BUY",
                "result": "TP2_HIT",
                "profit": 126.40,
                "strategy": "SMC + PRICE ACTION",
            },
            {
                "date": "2026-10-03",
                "symbol": "BTCUSD",
                "direction": "SELL",
                "result": "ACTIVE",
                "profit": 84.20,
                "strategy": "SMC + PRICE ACTION",
            },
            {
                "date": "2026-10-02",
                "symbol": "EURUSD",
                "direction": "BUY",
                "result": "TP3_HIT",
                "profit": 242.70,
                "strategy": "SMC + PRICE ACTION",
            },
            {
                "date": "2026-10-02",
                "symbol": "GBPUSD",
                "direction": "SELL",
                "result": "SL",
                "profit": -90.00,
                "strategy": "SMC + PRICE ACTION",
            },
        ]


def initialize_demo():
    initialize_markets()
    create_demo_signals()
    create_demo_trades()
    create_demo_journal()

    STATE["bot"]["last_scan"] = now_iso()
    STATE["bot"]["uptime"] = int(time.time())


def simulate_market_update():
    with DATA_LOCK:
        for symbol in SYMBOLS:
            market = STATE["markets"][symbol]
            config = SYMBOL_CONFIG[symbol]

            old_price = float(market["price"])

            if symbol in ("BTCUSD", "XAUUSD"):
                movement = random.uniform(-0.0008, 0.0008)
            else:
                movement = random.uniform(-0.0004, 0.0004)

            new_price = old_price * (1 + movement)

            market["price"] = format_price(symbol, new_price)
            market["change_value"] = round(new_price - old_price, 6)
            market["change"] = round(
                ((new_price - old_price) / old_price) * 100,
                3,
            )
            market["volume"] = max(
                100,
                market["volume"] + random.randint(-500, 500),
            )
            market["updated_at"] = now_iso()

        STATE["bot"]["last_scan"] = now_iso()
        STATE["bot"]["cycles"] += 1


@app.route("/")
def index():
    return render_template_string(
        HTML,
        state=STATE,
    )


@app.route("/api/dashboard")
def api_dashboard():
    simulate_market_update()

    with DATA_LOCK:
        return jsonify(
            {
                "bot": STATE["bot"],
                "markets": STATE["markets"],
                "signals": STATE["signals"],
                "trades": STATE["trades"],
                "journal": STATE["journal"],
                "stats": STATE["stats"],
                "server_time": now_iso(),
            }
        )


@app.route("/api/markets")
def api_markets():
    simulate_market_update()

    with DATA_LOCK:
        return jsonify(
            {
                "markets": STATE["markets"],
                "updated_at": now_iso(),
            }
        )


@app.route("/api/signals")
def api_signals():
    with DATA_LOCK:
        return jsonify(
            {
                "signals": STATE["signals"],
                "updated_at": now_iso(),
            }
        )


@app.route("/api/trades")
def api_trades():
    with DATA_LOCK:
        return jsonify(
            {
                "trades": STATE["trades"],
                "updated_at": now_iso(),
            }
        )


@app.route("/api/journal")
def api_journal():
    with DATA_LOCK:
        return jsonify(
            {
                "journal": STATE["journal"],
                "stats": STATE["stats"],
                "updated_at": now_iso(),
            }
        )


@app.route("/health")
def health():
    return jsonify(
        {
            "status": "ok",
            "service": "NOVA TRADE AI SITE",
            "mode": "SIMULATION",
            "time": now_iso(),
        }
    )


HTML = r"""
<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <meta
        name="viewport"
        content="width=device-width, initial-scale=1.0"
    >

    <title>NOVA TRADE AI</title>

    <style>
        * {
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }

        body {
            font-family:
                Inter,
                -apple-system,
                BlinkMacSystemFont,
                "Segoe UI",
                sans-serif;

            background: #080b12;
            color: #f3f4f6;
            min-height: 100vh;
        }

        button {
            font-family: inherit;
        }

        .app {
            display: flex;
            min-height: 100vh;
        }

        .sidebar {
            width: 250px;
            background: #0d111a;
            border-right: 1px solid #202634;
            padding: 24px 16px;
            position: fixed;
            left: 0;
            top: 0;
            bottom: 0;
            z-index: 20;
        }

        .brand {
            display: flex;
            align-items: center;
            gap: 12px;
            padding: 4px 10px 30px;
        }

        .brand-icon {
            width: 42px;
            height: 42px;
            border-radius: 12px;
            display: flex;
            align-items: center;
            justify-content: center;
            background: #151b27;
            border: 1px solid #293245;
            font-weight: 900;
            color: #60a5fa;
        }

        .brand h1 {
            font-size: 18px;
            letter-spacing: .5px;
        }

        .brand span {
            display: block;
            color: #7d8799;
            font-size: 11px;
            margin-top: 3px;
        }

        .nav-title {
            color: #596274;
            font-size: 10px;
            text-transform: uppercase;
            letter-spacing: 1.5px;
            padding: 10px 12px;
            margin-top: 8px;
        }

        .nav {
            display: flex;
            flex-direction: column;
            gap: 5px;
        }

        .nav button {
            border: 0;
            background: transparent;
            color: #929bad;
            text-align: left;
            padding: 12px 13px;
            border-radius: 9px;
            cursor: pointer;
            font-size: 14px;
        }

        .nav button:hover,
        .nav button.active {
            background: #151b27;
            color: #fff;
        }

        .nav button.active {
            border-left: 3px solid #60a5fa;
            padding-left: 10px;
        }

        .sidebar-bottom {
            position: absolute;
            left: 16px;
            right: 16px;
            bottom: 20px;
        }

        .mode {
            background: #111722;
            border: 1px solid #222b3b;
            border-radius: 12px;
            padding: 14px;
        }

        .mode-label {
            color: #687387;
            font-size: 11px;
            text-transform: uppercase;
            margin-bottom: 7px;
        }

        .mode-value {
            display: flex;
            align-items: center;
            gap: 7px;
            font-size: 13px;
        }

        .dot {
            width: 8px;
            height: 8px;
            background: #22c55e;
            border-radius: 50%;
            box-shadow: 0 0 10px #22c55e;
        }

        .main {
            margin-left: 250px;
            width: calc(100% - 250px);
            padding: 28px;
        }

        .topbar {
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 20px;
            margin-bottom: 28px;
        }

        .topbar h2 {
            font-size: 25px;
        }

        .topbar p {
            color: #747e90;
            font-size: 13px;
            margin-top: 5px;
        }

        .top-actions {
            display: flex;
            align-items: center;
            gap: 10px;
        }

        .status {
            display: flex;
            align-items: center;
            gap: 8px;
            background: #0f1714;
            border: 1px solid #193226;
            color: #7ee2a8;
            border-radius: 20px;
            padding: 9px 14px;
            font-size: 12px;
        }

        .refresh {
            border: 1px solid #293245;
            background: #111722;
            color: #dbe2ed;
            border-radius: 9px;
            padding: 9px 13px;
            cursor: pointer;
        }

        .grid {
            display: grid;
            grid-template-columns: repeat(4, minmax(0, 1fr));
            gap: 14px;
        }

        .card {
            background: #0e131d;
            border: 1px solid #202838;
            border-radius: 13px;
        }

        .stat-card {
            padding: 18px;
        }

        .stat-label {
            color: #707b8e;
            font-size: 12px;
            margin-bottom: 10px;
        }

        .stat-value {
            font-size: 25px;
            font-weight: 700;
        }

        .stat-small {
            color: #697487;
            font-size: 11px;
            margin-top: 7px;
        }

        .positive {
            color: #4ade80;
        }

        .negative {
            color: #f87171;
        }

        .blue {
            color: #60a5fa;
        }

        .section {
            margin-top: 26px;
        }

        .section-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 13px;
        }

        .section-header h3 {
            font-size: 16px;
        }

        .section-header span {
            color: #697487;
            font-size: 11px;
        }

        .market-grid {
            display: grid;
            grid-template-columns: repeat(4, minmax(0, 1fr));
            gap: 14px;
        }

        .market-card {
            padding: 17px;
        }

        .market-top {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 17px;
        }

        .symbol {
            font-weight: 700;
            font-size: 15px;
        }

        .market-change {
            font-size: 11px;
            padding: 4px 7px;
            border-radius: 6px;
            background: #151c28;
        }

        .price {
            font-size: 25px;
            font-weight: 700;
            margin-bottom: 12px;
        }

        .market-info {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 9px;
            margin-top: 14px;
        }

        .info {
            background: #0a0f17;
            border-radius: 8px;
            padding: 9px;
        }

        .info span {
            display: block;
            color: #5f697b;
            font-size: 9px;
            text-transform: uppercase;
            margin-bottom: 5px;
        }

        .info strong {
            font-size: 11px;
        }

        .bias {
            display: inline-flex;
            padding: 4px 7px;
            border-radius: 5px;
            font-size: 10px;
            font-weight: 700;
        }

        .bullish {
            color: #4ade80;
            background: rgba(34, 197, 94, .08);
        }

        .bearish {
            color: #f87171;
            background: rgba(239, 68, 68, .08);
        }

        .tables-grid {
            display: grid;
            grid-template-columns: 1.3fr .7fr;
            gap: 14px;
        }

        .table-card {
            overflow: hidden;
        }

        table {
            width: 100%;
            border-collapse: collapse;
        }

        th {
            color: #5f6a7d;
            font-size: 10px;
            text-transform: uppercase;
            font-weight: 600;
            text-align: left;
            padding: 13px 15px;
            border-bottom: 1px solid #202838;
        }

        td {
            padding: 14px 15px;
            border-bottom: 1px solid #171e2a;
            font-size: 12px;
            color: #c8ced8;
        }

        tr:last-child td {
            border-bottom: 0;
        }

        .direction {
            font-weight: 700;
        }

        .buy {
            color: #4ade80;
        }

        .sell {
            color: #f87171;
        }

        .tag {
            display: inline-block;
            padding: 4px 7px;
            border-radius: 5px;
            background: #151c28;
            color: #9aa4b5;
            font-size: 9px;
        }

        .signal-card {
            padding: 18px;
        }

        .signal-head {
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            margin-bottom: 15px;
        }

        .signal-symbol {
            font-size: 17px;
            font-weight: 700;
        }

        .signal-direction {
            margin-top: 4px;
            font-size: 11px;
            font-weight: 700;
        }

        .signal-status {
            font-size: 9px;
            background: rgba(34, 197, 94, .08);
            color: #4ade80;
            border-radius: 5px;
            padding: 5px 7px;
        }

        .signal-levels {
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 7px;
        }

        .level {
            background: #0a0f17;
            padding: 9px;
            border-radius: 7px;
        }

        .level span {
            display: block;
            font-size: 8px;
            color: #606b7d;
            margin-bottom: 4px;
        }

        .level strong {
            font-size: 10px;
        }

        .signal-footer {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-top: 14px;
            color: #697487;
            font-size: 10px;
        }

        .rr {
            color: #60a5fa;
            font-weight: 700;
        }

        .mobile-menu {
            display: none;
        }

        .page {
            display: none;
        }

        .page.active {
            display: block;
        }

        .empty {
            text-align: center;
            padding: 40px;
            color: #697487;
        }

        @media (max-width: 1200px) {
            .grid,
            .market-grid {
                grid-template-columns: repeat(2, 1fr);
            }

            .tables-grid {
                grid-template-columns: 1fr;
            }
        }

        @media (max-width: 800px) {
            .sidebar {
                transform: translateX(-100%);
                transition: .2s;
            }

            .sidebar.open {
                transform: translateX(0);
            }

            .main {
                margin-left: 0;
                width: 100%;
                padding: 18px;
            }

            .mobile-menu {
                display: block;
                border: 1px solid #293245;
                background: #111722;
                color: white;
                border-radius: 8px;
                padding: 8px 11px;
                cursor: pointer;
            }

            .topbar {
                align-items: flex-start;
            }

            .topbar h2 {
                font-size: 20px;
            }

            .status {
                display: none;
            }

            .grid,
            .market-grid {
                grid-template-columns: 1fr;
            }

            .signal-levels {
                grid-template-columns: repeat(2, 1fr);
            }

            .table-card {
                overflow-x: auto;
            }

            table {
                min-width: 650px;
            }
        }
    </style>
</head>

<body>

<div class="app">

    <aside class="sidebar" id="sidebar">

        <div class="brand">
            <div class="brand-icon">N</div>
            <div>
                <h1>NOVA TRADE AI</h1>
                <span>Trading Intelligence</span>
            </div>
        </div>

        <div class="nav-title">Navigation</div>

        <nav class="nav">
            <button
                class="active"
                onclick="showPage('dashboard', this)"
            >
                ◉ Dashboard
            </button>

            <button
                onclick="showPage('signals', this)"
            >
                ◈ Signaux
            </button>

            <button
                onclick="showPage('trades', this)"
            >
                ◇ Trades actifs
            </button>

            <button
                onclick="showPage('journal', this)"
            >
                ▤ Journal
            </button>

            <button
                onclick="showPage('markets', this)"
            >
                ◎ Marchés
            </button>
        </nav>

        <div class="sidebar-bottom">
            <div class="mode">
                <div class="mode-label">Mode actuel</div>
                <div class="mode-value">
                    <span class="dot"></span>
                    Simulation
                </div>
            </div>
        </div>

    </aside>

    <main class="main">

        <div class="topbar">

            <div style="display:flex;align-items:center;gap:12px;">
                <button
                    class="mobile-menu"
                    onclick="toggleSidebar()"
                >
                    ☰
                </button>

                <div>
                    <h2 id="page-title">
                        Dashboard
                    </h2>

                    <p>
                        Vue globale de NOVA TRADE AI
                    </p>
                </div>
            </div>

            <div class="top-actions">

                <div class="status">
                    <span class="dot"></span>
                    BOT EN LIGNE
                </div>

                <button
                    class="refresh"
                    onclick="loadDashboard()"
                >
                    Actualiser
                </button>

            </div>

        </div>


        <section
            class="page active"
            id="dashboard"
        >

            <div class="grid">

                <div class="card stat-card">
                    <div class="stat-label">
                        Trades
                    </div>

                    <div
                        class="stat-value"
                        id="total-trades"
                    >
                        --
                    </div>

                    <div class="stat-small">
                        Total journalisé
                    </div>
                </div>

                <div class="card stat-card">
                    <div class="stat-label">
                        Win Rate
                    </div>

                    <div
                        class="stat-value positive"
                        id="win-rate"
                    >
                        --
                    </div>

                    <div class="stat-small">
                        Taux de réussite
                    </div>
                </div>

                <div class="card stat-card">
                    <div class="stat-label">
                        Profit
                    </div>

                    <div
                        class="stat-value positive"
                        id="profit"
                    >
                        --
                    </div>

                    <div class="stat-small">
                        Performance cumulée
                    </div>
                </div>

                <div class="card stat-card">
                    <div class="stat-label">
                        Profit Factor
                    </div>

                    <div
                        class="stat-value blue"
                        id="profit-factor"
                    >
                        --
                    </div>

                    <div class="stat-small">
                        Ratio gains / pertes
                    </div>
                </div>

            </div>


            <div class="section">

                <div class="section-header">
                    <h3>Marchés surveillés</h3>
                    <span>
                        M15 • M5 • M1
                    </span>
                </div>

                <div
                    class="market-grid"
                    id="market-grid"
                >
                </div>

            </div>


            <div class="section">

                <div class="section-header">
                    <h3>Signaux actifs</h3>
                    <span>
                        SMC + Price Action
                    </span>
                </div>

                <div
                    class="grid"
                    id="dashboard-signals"
                >
                </div>

            </div>

        </section>


        <section
            class="page"
            id="signals"
        >

            <div class="section">

                <div class="section-header">
                    <h3>Signaux de trading</h3>
                    <span>
                        Données simulées
                    </span>
                </div>

                <div
                    class="grid"
                    id="signals-grid"
                >
                </div>

            </div>

        </section>


        <section
            class="page"
            id="trades"
        >

            <div class="section">

                <div class="section-header">
                    <h3>Trades actifs</h3>
                    <span>
                        Suivi en temps réel
                    </span>
                </div>

                <div class="card table-card">

                    <table>

                        <thead>
                            <tr>
                                <th>ID</th>
                                <th>Marché</th>
                                <th>Direction</th>
                                <th>Entrée</th>
                                <th>SL</th>
                                <th>TP3</th>
                                <th>Statut</th>
                                <th>P&L</th>
                            </tr>
                        </thead>

                        <tbody id="trades-table">
                        </tbody>

                    </table>

                </div>

            </div>

        </section>


        <section
            class="page"
            id="journal"
        >

            <div class="section">

                <div class="section-header">
                    <h3>Journal de trading</h3>
                    <span>
                        Historique
                    </span>
                </div>

                <div class="card table-card">

                    <table>

                        <thead>
                            <tr>
                                <th>Date</th>
                                <th>Marché</th>
                                <th>Direction</th>
                                <th>Résultat</th>
                                <th>Stratégie</th>
                                <th>Profit</th>
                            </tr>
                        </thead>

                        <tbody id="journal-table">
                        </tbody>

                    </table>

                </div>

            </div>

        </section>


        <section
            class="page"
            id="markets"
        >

            <div class="section">

                <div class="section-header">
                    <h3>Analyse des marchés</h3>
                    <span>
                        Structure M15
                    </span>
                </div>

                <div
                    class="market-grid"
                    id="markets-page"
                >
                </div>

            </div>

        </section>

    </main>

</div>


<script>

let dashboardData = null;


function escapeHtml(value) {
    return String(value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#039;");
}


function formatMoney(value) {
    return Number(value).toLocaleString(
        "fr-FR",
        {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2
        }
    );
}


function showPage(pageId, button) {

    document
        .querySelectorAll(".page")
        .forEach(page => {
            page.classList.remove("active");
        });

    document
        .getElementById(pageId)
        .classList.add("active");

    document
        .querySelectorAll(".nav button")
        .forEach(btn => {
            btn.classList.remove("active");
        });

    if (button) {
        button.classList.add("active");
    }

    const titles = {
        dashboard: "Dashboard",
        signals: "Signaux",
        trades: "Trades actifs",
        journal: "Journal",
        markets: "Marchés"
    };

    document.getElementById("page-title").textContent =
        titles[pageId] || "Dashboard";

    document
        .getElementById("sidebar")
        .classList.remove("open");
}


function toggleSidebar() {
    document
        .getElementById("sidebar")
        .classList.toggle("open");
}


function renderMarketCard(market) {

    const changeClass =
        market.change >= 0
            ? "positive"
            : "negative";

    const biasClass =
        market.bias === "BULLISH"
            ? "bullish"
            : "bearish";

    return `
        <div class="card market-card">

            <div class="market-top">
                <div class="symbol">
                    ${escapeHtml(market.symbol)}
                </div>

                <div class="market-change ${changeClass}">
                    ${market.change >= 0 ? "+" : ""}
                    ${market.change}%
                </div>
            </div>

            <div class="price">
                ${Number(market.price).toFixed(
                    market.symbol === "EURUSD" ||
                    market.symbol === "GBPUSD"
                        ? 5
                        : 2
                )}
            </div>

            <div>
                <span class="bias ${biasClass}">
                    ${escapeHtml(market.bias)}
                </span>
            </div>

            <div class="market-info">

                <div class="info">
                    <span>Support</span>
                    <strong>
                        ${market.support}
                    </strong>
                </div>

                <div class="info">
                    <span>Résistance</span>
                    <strong>
                        ${market.resistance}
                    </strong>
                </div>

                <div class="info">
                    <span>EMA20</span>
                    <strong>
                        ${market.ema20}
                    </strong>
                </div>

                <div class="info">
                    <span>EMA50</span>
                    <strong>
                        ${market.ema50}
                    </strong>
                </div>

                <div class="info">
                    <span>BOS</span>
                    <strong>
                        ${escapeHtml(market.bos)}
                    </strong>
                </div>

                <div class="info">
                    <span>Zone</span>
                    <strong>
                        ${escapeHtml(market.zone_status)}
                    </strong>
                </div>

            </div>

        </div>
    `;
}


function renderSignalCard(signal) {

    const isBuy =
        signal.direction === "BUY";

    return `
        <div class="card signal-card">

            <div class="signal-head">

                <div>
                    <div class="signal-symbol">
                        ${escapeHtml(signal.symbol)}
                    </div>

                    <div
                        class="signal-direction
                        ${isBuy ? "buy" : "sell"}"
                    >
                        ${isBuy ? "ACHAT" : "VENTE"}
                    </div>
                </div>

                <div class="signal-status">
                    ${escapeHtml(signal.status)}
                </div>

            </div>

            <div class="signal-levels">

                <div class="level">
                    <span>ENTRÉE</span>
                    <strong>
                        ${signal.entry}
                    </strong>
                </div>

                <div class="level">
                    <span>SL</span>
                    <strong class="negative">
                        ${signal.sl}
                    </strong>
                </div>

                <div class="level">
                    <span>TP1</span>
                    <strong class="positive">
                        ${signal.tp1}
                    </strong>
                </div>

                <div class="level">
                    <span>TP2</span>
                    <strong class="positive">
                        ${signal.tp2}
                    </strong>
                </div>

                <div class="level">
                    <span>TP3</span>
                    <strong class="positive">
                        ${signal.tp3}
                    </strong>
                </div>

                <div class="level">
                    <span>RR</span>
                    <strong class="rr">
                        1:${signal.rr}
                    </strong>
                </div>

            </div>

            <div class="signal-footer">

                <span>
                    ${escapeHtml(signal.strategy)}
                </span>

                <span>
                    ${escapeHtml(signal.timeframe)}
                </span>

            </div>

        </div>
    `;
}


function renderTrades(trades) {

    const container =
        document.getElementById("trades-table");

    container.innerHTML = trades.map(
        trade => {

            const directionClass =
                trade.direction === "BUY"
                    ? "buy"
                    : "sell";

            const profitClass =
                trade.profit >= 0
                    ? "positive"
                    : "negative";

            return `
                <tr>

                    <td>
                        ${escapeHtml(trade.id)}
                    </td>

                    <td>
                        <strong>
                            ${escapeHtml(trade.symbol)}
                        </strong>
                    </td>

                    <td>
                        <span
                            class="direction
                            ${directionClass}"
                        >
                            ${escapeHtml(trade.direction)}
                        </span>
                    </td>

                    <td>
                        ${trade.entry}
                    </td>

                    <td>
                        ${trade.sl}
                    </td>

                    <td>
                        ${trade.tp3}
                    </td>

                    <td>
                        <span class="tag">
                            ${escapeHtml(trade.status)}
                        </span>
                    </td>

                    <td class="${profitClass}">
                        ${trade.profit >= 0 ? "+" : ""}
                        $${formatMoney(trade.profit)}
                    </td>

                </tr>
            `;
        }
    ).join("");
}


function renderJournal(journal) {

    const container =
        document.getElementById("journal-table");

    container.innerHTML = journal.map(
        row => {

            const profitClass =
                row.profit >= 0
                    ? "positive"
                    : "negative";

            return `
                <tr>

                    <td>
                        ${escapeHtml(row.date)}
                    </td>

                    <td>
                        <strong>
                            ${escapeHtml(row.symbol)}
                        </strong>
                    </td>

                    <td>
                        <span class="
                            direction
                            ${row.direction === "BUY"
                                ? "buy"
                                : "sell"}
                        ">
                            ${escapeHtml(row.direction)}
                        </span>
                    </td>

                    <td>
                        <span class="tag">
                            ${escapeHtml(row.result)}
                        </span>
                    </td>

                    <td>
                        ${escapeHtml(row.strategy)}
                    </td>

                    <td class="${profitClass}">
                        ${row.profit >= 0 ? "+" : ""}
                        $${formatMoney(row.profit)}
                    </td>

                </tr>
            `;
        }
    ).join("");
}


function updateDashboard(data) {

    dashboardData = data;

    document.getElementById("total-trades")
        .textContent =
        data.stats.total_trades;

    document.getElementById("win-rate")
        .textContent =
        data.stats.win_rate + "%";

    document.getElementById("profit")
        .textContent =
        "$" + formatMoney(data.stats.profit);

    document.getElementById("profit-factor")
        .textContent =
        data.stats.profit_factor;

    const marketHTML =
        Object.values(data.markets)
            .map(renderMarketCard)
            .join("");

    document.getElementById("market-grid")
        .innerHTML = marketHTML;

    document.getElementById("markets-page")
        .innerHTML = marketHTML;

    const signals =
        data.signals || [];

    document.getElementById("dashboard-signals")
        .innerHTML =
        signals.slice(0, 4)
            .map(renderSignalCard)
            .join("");

    document.getElementById("signals-grid")
        .innerHTML =
        signals.map(renderSignalCard)
            .join("");

    renderTrades(data.trades || []);
    renderJournal(data.journal || []);
}


async function loadDashboard() {

    try {

        const response =
            await fetch("/api/dashboard", {
                cache: "no-store"
            });

        if (!response.ok) {
            throw new Error(
                "Erreur API"
            );
        }

        const data =
            await response.json();

        updateDashboard(data);

    } catch (error) {

        console.error(
            "Impossible de charger le dashboard:",
            error
        );

    }
}


loadDashboard();


setInterval(
    loadDashboard,
    5000
);

</script>

</body>
</html>
"""


if __name__ == "__main__":
    initialize_demo()

    app.run(
        host=HOST,
        port=PORT,
        debug=False,
        threaded=True,
    )