import os, json, secrets, sqlite3, threading, time
from datetime import datetime, timedelta, timezone
from functools import wraps
from pathlib import Path
import requests
from flask import Flask, jsonify, render_template_string, request, redirect, url_for, session, flash

BASE=Path(__file__).resolve().parent
DB=BASE/'nova_site.db'
DEMO=os.getenv('NOVA_SITE_DEMO_MODE','true').lower() in ('1','true','yes','on')
TOKEN=os.getenv('TELEGRAM_BOT_TOKEN','').strip(); CHAT=os.getenv('TELEGRAM_PRIVATE_CHAT_ID','').strip()
ADMIN=os.getenv('NOVA_ADMIN_PASSWORD','').strip() or ('NOVA-ADMIN-TEST' if DEMO else ''); SECRET=os.getenv('NOVA_SITE_SECRET',secrets.token_hex(32))
MM={'ORANGE':os.getenv('NOVA_ORANGE_MONEY','À CONFIGURER'),'MOOV':os.getenv('NOVA_MOOV_MONEY','À CONFIGURER'),'WAVE':os.getenv('NOVA_WAVE_MONEY','À CONFIGURER')}
USDT={'TRC20':os.getenv('NOVA_USDT_TRC20','À CONFIGURER'),'ERC20':os.getenv('NOVA_USDT_ERC20','À CONFIGURER')}
app=Flask(__name__); app.secret_key=SECRET
DB_LOCK=threading.RLock(); JSON_LOCK=threading.RLock(); PANIC_LOCK=threading.RLock()
PANIC={'active':False,'requested_at':None}
LABELS={'WAITING_M5_LIQUIDITY':'Attente Liquidité','WAITING_M1_CHOCH':'Attente CHoCH','WAITING_M1_BOS':'Attente de confirmation','WAITING_CONFIRMATION_BOS_M1':'Attente de confirmation'}

BASE_HTML = """<!doctype html>
<html lang="fr"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="theme-color" content="#07111f"><title>{{ title or 'NOVA TRADE AI' }}</title><script src="https://cdn.tailwindcss.com"></script><script>tailwind.config={theme:{extend:{fontFamily:{sans:['Inter','ui-sans-serif','system-ui']},boxShadow:{glow:'0 0 40px rgba(34,211,238,.10)'}}}}</script><style>
*{scrollbar-width:thin;scrollbar-color:#334155 transparent}body{background:#07111f;background-image:radial-gradient(circle at 15% 0%,rgba(6,182,212,.10),transparent 28%),radial-gradient(circle at 90% 20%,rgba(59,130,246,.08),transparent 25%),linear-gradient(rgba(255,255,255,.018) 1px,transparent 1px),linear-gradient(90deg,rgba(255,255,255,.018) 1px,transparent 1px);background-size:auto,auto,42px 42px,42px 42px}.glass{background:rgba(10,22,38,.72);backdrop-filter:blur(18px);border:1px solid rgba(148,163,184,.13)}.shine{position:relative;overflow:hidden}.shine:after{content:'';position:absolute;inset:-80% -30%;background:linear-gradient(100deg,transparent 42%,rgba(255,255,255,.05) 50%,transparent 58%);transform:translateX(-45%);animation:shine 7s infinite}@keyframes shine{55%,100%{transform:translateX(45%)}}.pulse{animation:pulse 2s infinite}@keyframes pulse{50%{opacity:.45}}.ticker{animation:scroll 28s linear infinite}@keyframes scroll{from{transform:translateX(0)}to{transform:translateX(-50%)}}.navlink{transition:.2s}.navlink:hover{background:rgba(34,211,238,.08);color:#67e8f9}.cardhover{transition:transform .2s,border-color .2s,box-shadow .2s}.cardhover:hover{transform:translateY(-3px);border-color:rgba(34,211,238,.30);box-shadow:0 15px 50px rgba(0,0,0,.22)}
</style></head><body class="text-slate-100 min-h-screen"><div class="max-w-[1500px] mx-auto px-3 sm:px-5 lg:px-7 py-4">{% with messages=get_flashed_messages(with_categories=true) %}{% for category,message in messages %}<div class="mb-3 rounded-2xl glass px-4 py-3 text-sm flex items-center gap-3"><span class="w-2 h-2 rounded-full bg-cyan-400 pulse"></span>{{ message }}</div>{% endfor %}{% endwith %}{{ body|safe }}</div></body></html>"""
NAV_HTML = """
<nav class="sticky top-3 z-40 mb-5 glass rounded-2xl shadow-glow">
 <div class="px-3 py-2.5 flex items-center gap-3">
  <a href="/" class="flex items-center gap-2.5 min-w-fit"><span class="w-9 h-9 rounded-xl bg-gradient-to-br from-cyan-300 to-blue-500 text-slate-950 grid place-items-center font-black">N</span><span><b class="tracking-[.18em] text-cyan-300">NOVA</b><small class="hidden sm:block text-[9px] tracking-[.22em] text-slate-500">TRADE AI</small></span></a>
  <div class="hidden lg:flex flex-1 items-center justify-center gap-1 text-sm"><a class="navlink rounded-xl px-3 py-2" href="/">Dashboard</a><a class="navlink rounded-xl px-3 py-2" href="/history">Journal</a><a class="navlink rounded-xl px-3 py-2" href="/strategy">Stratégie</a><a class="navlink rounded-xl px-3 py-2" href="/founder">Fondateur</a><a class="navlink rounded-xl px-3 py-2" href="/roadmap">Évolution</a><a class="navlink rounded-xl px-3 py-2" href="/faq">Centre d'aide</a><a class="navlink rounded-xl px-3 py-2 text-cyan-300" href="/nova-admin">Administration</a></div>
  <div class="ml-auto flex items-center gap-2"><span class="hidden sm:flex items-center gap-2 rounded-full bg-emerald-400/10 px-3 py-1.5 text-xs text-emerald-300"><i class="w-2 h-2 rounded-full bg-emerald-400 pulse"></i> SYSTEM ONLINE</span><a href="/login" class="hidden sm:block rounded-xl border border-slate-700 px-3 py-2 text-sm">Connexion</a><button onclick="document.getElementById('mobileNav').classList.toggle('hidden')" class="lg:hidden rounded-xl border border-slate-700 px-3 py-2">☰</button></div>
 </div>
 <div id="mobileNav" class="hidden lg:hidden border-t border-slate-800/70 p-3 grid grid-cols-2 gap-2 text-sm"><a class="rounded-xl bg-slate-900/70 p-3" href="/">🏠 Dashboard</a><a class="rounded-xl bg-slate-900/70 p-3" href="/history">📒 Journal</a><a class="rounded-xl bg-slate-900/70 p-3" href="/strategy">🧠 Stratégie</a><a class="rounded-xl bg-slate-900/70 p-3" href="/founder">👤 Fondateur</a><a class="rounded-xl bg-slate-900/70 p-3" href="/roadmap">🚀 Évolution</a><a class="rounded-xl bg-slate-900/70 p-3" href="/faq">❓ Aide</a><a class="rounded-xl bg-slate-900/70 p-3 text-cyan-300" href="/nova-admin">⚙️ Administration</a></div>
</nav>
"""
PUBLIC_HTML = """{% extends_base %}{{ nav|safe }}<main class="space-y-6">{{ content|safe }}</main>"""
HISTORY_HTML = """
<div class="rounded-3xl border border-slate-800 bg-gradient-to-br from-slate-900 to-slate-950 p-6 md:p-8">
  <div class="text-xs tracking-[.35em] text-cyan-400">NOVA TRADE AI</div>
  <h1 class="mt-2 text-3xl md:text-5xl font-black">Historique de trading</h1>
  <p class="mt-3 text-slate-400 max-w-3xl">Une vue claire des opérations enregistrées par NOVA. Les données réelles apparaissent automatiquement lorsque le journal du bot est disponible.</p>
  <div class="grid grid-cols-2 md:grid-cols-4 gap-3 mt-6">
    <div class="rounded-2xl bg-slate-950 border border-slate-800 p-4"><div class="text-xs text-slate-500">TRADES</div><div class="text-2xl font-bold">{{ stats.total }}</div></div>
    <div class="rounded-2xl bg-slate-950 border border-slate-800 p-4"><div class="text-xs text-slate-500">WIN</div><div class="text-2xl font-bold text-emerald-400">{{ stats.win }}</div></div>
    <div class="rounded-2xl bg-slate-950 border border-slate-800 p-4"><div class="text-xs text-slate-500">LOSS</div><div class="text-2xl font-bold text-red-400">{{ stats.loss }}</div></div>
    <div class="rounded-2xl bg-slate-950 border border-slate-800 p-4"><div class="text-xs text-slate-500">P&L</div><div class="text-2xl font-bold">{{ '%.2f'|format(stats.pnl) }}</div></div>
  </div>
</div>
<div class="rounded-2xl border border-slate-800 bg-slate-900 p-5 overflow-auto">
  <div class="flex items-center justify-between mb-4"><h2 class="text-xl font-bold">📒 Journal NOVA</h2><a href="/" class="text-cyan-400 text-sm">Retour dashboard →</a></div>
  <table class="w-full text-sm min-w-[720px]"><thead><tr class="text-slate-500"><th class="text-left py-3">Date</th><th class="text-left">Symbole</th><th class="text-left">Direction</th><th class="text-left">Entrée</th><th class="text-left">Résultat</th><th class="text-left">P&L</th><th></th></tr></thead><tbody>
  {% for x in history %}<tr class="border-t border-slate-800 hover:bg-slate-950"><td class="py-3">{{ x.get('closed_at') or x.get('date') or x.get('created_at') or '—' }}</td><td>{{ x.get('symbol','—') }}</td><td>{{ x.get('direction','—') }}</td><td>{{ x.get('entry','—') }}</td><td><span class="rounded-lg px-2 py-1 {% if (x.get('result') or x.get('status')) in ['WIN','TP3_HIT'] %}bg-emerald-950 text-emerald-300{% elif (x.get('result') or x.get('status')) in ['LOSS','SL'] %}bg-red-950 text-red-300{% else %}bg-slate-800 text-slate-300{% endif %}">{{ x.get('result') or x.get('status') or '—' }}</span></td><td>{{ x.get('profit', x.get('pnl','—')) }}</td><td><button onclick='showTrade({{ x|tojson }})' class="text-cyan-400">Détails</button></td></tr>{% else %}<tr><td colspan="7" class="py-8 text-center text-slate-500">Aucun trade enregistré.</td></tr>{% endfor %}</tbody></table>
</div>
<div id="tradeModal" class="hidden fixed inset-0 z-50 bg-black/70 p-4" onclick="if(event.target===this)this.classList.add('hidden')"><div class="max-w-xl mx-auto mt-20 rounded-2xl border border-slate-700 bg-slate-900 p-6"><div class="flex justify-between"><h3 class="text-xl font-bold">Détail du trade</h3><button onclick="document.getElementById('tradeModal').classList.add('hidden')">✕</button></div><pre id="tradeDetails" class="mt-4 whitespace-pre-wrap text-sm text-slate-300"></pre></div></div>
<script>function showTrade(x){document.getElementById('tradeDetails').textContent=JSON.stringify(x,null,2);document.getElementById('tradeModal').classList.remove('hidden')}</script>
"""
STRATEGY_HTML = """
<div class="rounded-3xl border border-cyan-900 bg-gradient-to-br from-cyan-950/40 to-slate-950 p-7"><div class="text-xs tracking-[.35em] text-cyan-400">NOVA ENGINE</div><h1 class="mt-2 text-3xl md:text-5xl font-black">La stratégie NOVA</h1><p class="mt-4 max-w-3xl text-slate-300">NOVA combine Smart Money Concepts, Price Action et lecture de structure pour transformer un contexte de marché en exécution.</p></div>
<div class="grid md:grid-cols-2 xl:grid-cols-4 gap-4">
{% for n,t,d in [('01','Contexte de marché','Biais, BOS, zones majeures de support et résistance.'),('02','Liquidité','Recherche d’un sweep de liquidité et d’une réintégration cohérente.'),('03','CHoCH','Le changement de caractère valide le passage du contexte à l’exécution.'),('04','Confirmation finale','Confirmation finale puis préparation de l’entrée, SL et TP1/TP2/TP3.') ]}<div class="rounded-2xl border border-slate-800 bg-slate-900 p-5"><div class="text-cyan-400 font-black text-2xl">{{ n }}</div><h2 class="font-bold mt-3">{{ t }}</h2><p class="text-sm text-slate-400 mt-2">{{ d }}</p></div>{% endfor %}</div>
<div class="rounded-2xl border border-slate-800 bg-slate-900 p-6"><h2 class="text-xl font-bold">🎯 Gestion du risque</h2><div class="grid md:grid-cols-3 gap-4 mt-4"><div><b>TP1</b><p class="text-slate-400 text-sm">1R — première sécurisation.</p></div><div><b>TP2</b><p class="text-slate-400 text-sm">Objectif intermédiaire basé sur la structure.</p></div><div><b>TP3</b><p class="text-slate-400 text-sm">Extension vers l’zone cible avec contrôle du RR.</p></div></div></div>
<div class="rounded-2xl border border-amber-900 bg-amber-950/20 p-5 text-sm text-amber-200">NOVA est un système d’analyse et d’exécution algorithmique. Les signaux ne constituent pas une garantie de résultat financier.</div>
"""
FOUNDER_HTML = """
<div class="rounded-3xl border border-slate-800 bg-gradient-to-br from-slate-900 to-slate-950 p-7 md:p-10"><div class="text-xs tracking-[.35em] text-cyan-400">À PROPOS</div><h1 class="mt-2 text-3xl md:text-5xl font-black">Le fondateur de NOVA</h1><div class="mt-7 grid md:grid-cols-[180px_1fr] gap-7 items-center"><div class="h-40 w-40 rounded-3xl border border-cyan-800 bg-cyan-950/40 flex items-center justify-center text-5xl">N</div><div><h2 class="text-2xl font-bold">SAWADOGO CHEICK HAMED</h2><p class="mt-2 text-slate-400 leading-7">Fondateur et concepteur du projet NOVA TRADE AI. Le projet est pensé comme une infrastructure de trading algorithmique combinant analyse de marché, gestion des opportunités, suivi des positions, journalisation et interface utilisateur.</p><div class="flex flex-wrap gap-2 mt-4"><span class="rounded-full bg-slate-800 px-3 py-1 text-sm">NOVA TRADE AI</span><span class="rounded-full bg-slate-800 px-3 py-1 text-sm">Trading algorithmique</span><span class="rounded-full bg-slate-800 px-3 py-1 text-sm">SMC / Price Action</span></div></div></div></div>
<div class="grid md:grid-cols-3 gap-4"><div class="rounded-2xl border border-slate-800 bg-slate-900 p-5"><b>Vision</b><p class="text-sm text-slate-400 mt-2">Construire un système lisible, discipliné et automatisable.</p></div><div class="rounded-2xl border border-slate-800 bg-slate-900 p-5"><b>Architecture</b><p class="text-sm text-slate-400 mt-2">Bot, journal, notifications Telegram et interface web séparés pour limiter les risques.</p></div><div class="rounded-2xl border border-slate-800 bg-slate-900 p-5"><b>Objectif</b><p class="text-sm text-slate-400 mt-2">Centraliser l’information du trader dans une expérience simple et professionnelle.</p></div></div>
"""
ROADMAP_HTML = """
<div class="rounded-3xl border border-slate-800 bg-slate-900 p-7"><div class="text-xs tracking-[.35em] text-cyan-400">ÉVOLUTION</div><h1 class="mt-2 text-3xl md:text-5xl font-black">L’histoire et les prochaines étapes</h1><p class="mt-3 text-slate-400">Une timeline vivante du développement de NOVA.</p></div>
<div class="space-y-4">{% for n,title,text in timeline %}<div class="flex gap-4"><div class="shrink-0 w-12 h-12 rounded-2xl bg-cyan-500 text-slate-950 font-black flex items-center justify-center">{{ n }}</div><div class="flex-1 rounded-2xl border border-slate-800 bg-slate-900 p-5"><h2 class="font-bold text-lg">{{ title }}</h2><p class="text-slate-400 mt-2">{{ text }}</p></div></div>{% endfor %}</div>
"""
FAQ_HTML = """
<div class="rounded-3xl border border-slate-800 bg-slate-900 p-7"><div class="text-xs tracking-[.35em] text-cyan-400">CENTRE D’AIDE</div><h1 class="mt-2 text-3xl md:text-5xl font-black">FAQ NOVA</h1><div class="mt-6 space-y-3">{% for q,a in faqs %}<details class="rounded-2xl border border-slate-800 bg-slate-950 p-5"><summary class="cursor-pointer font-bold">{{ q }}</summary><p class="text-slate-400 mt-3 leading-6">{{ a }}</p></details>{% endfor %}</div></div>
"""
LOGIN_HTML = """<div class="max-w-md mx-auto mt-10 rounded-3xl border border-slate-800 bg-slate-900 p-7 shadow-2xl"><div class="text-3xl font-black">Connexion NOVA</div><p class="mt-2 text-slate-400">Retrouvez votre compte et votre abonnement sans créer un nouveau compte.</p><form method="post" class="mt-6 space-y-4"><input name="email" type="email" required placeholder="Email utilisé lors de l'inscription" class="w-full rounded-xl bg-slate-950 border border-slate-700 p-3"><button class="w-full rounded-xl bg-cyan-500 py-3 font-bold text-slate-950">Se connecter</button></form><a href="{{ url_for('register') }}" class="mt-5 block text-center text-sm text-cyan-400">Nouvel utilisateur ? Créer un compte</a></div>"""

REGISTER_HTML = """{% extends_base %}<div class="min-h-[80vh] flex items-center justify-center"><div class="w-full max-w-md rounded-2xl border border-slate-800 bg-slate-900 p-6 shadow-2xl"><div class="text-xs tracking-[.3em] text-cyan-400">NOVA TRADE AI</div><h1 class="mt-2 text-3xl font-bold">Créer votre accès</h1><p class="mt-2 text-slate-400">Essai TRIAL de 7 jours avec accès aux fonctionnalités NOVA.</p><form method="post" class="mt-6 space-y-3"><input name="username" required placeholder="Nom d'utilisateur" class="w-full rounded-xl bg-slate-950 border border-slate-700 p-3"><input name="email" type="email" required placeholder="Email" class="w-full rounded-xl bg-slate-950 border border-slate-700 p-3"><input name="telegram_user_id" placeholder="Telegram User ID (optionnel)" class="w-full rounded-xl bg-slate-950 border border-slate-700 p-3"><button class="w-full rounded-xl bg-cyan-500 hover:bg-cyan-400 text-slate-950 font-bold p-3">Démarrer mon essai 7 jours</button></form></div></div>"""
ADMIN_LOGIN_HTML = """{% extends_base %}<div class="min-h-[80vh] flex items-center justify-center"><div class="w-full max-w-md rounded-2xl border border-slate-800 bg-slate-900 p-6"><div class="text-xs tracking-[.3em] text-cyan-400">NOVA ADMIN</div><h1 class="mt-2 text-2xl font-bold">Connexion</h1><form method="post" class="mt-5 space-y-3"><input type="password" name="password" required placeholder="Mot de passe admin" class="w-full rounded-xl bg-slate-950 border border-slate-700 p-3"><button class="w-full rounded-xl bg-cyan-500 text-slate-950 font-bold p-3">Entrer</button></form></div></div>"""
PAYMENT_HTML = """{% extends_base %}<div class="flex items-center justify-between mb-6"><div><div class="text-xs tracking-[.3em] text-cyan-400">NOVA VIP</div><h1 class="text-3xl font-bold">Réabonnement</h1></div><a href="/" class="text-cyan-400">← Dashboard</a></div><div class="grid md:grid-cols-2 gap-4">{% for k,v in mobile_money.items() %}<div class="rounded-2xl border border-slate-800 bg-slate-900 p-5"><h2 class="font-bold">{{ k }} Money</h2><div class="mt-2 text-slate-300 break-all">{{ v }}</div></div>{% endfor %}{% for k,v in usdt.items() %}<div class="rounded-2xl border border-slate-800 bg-slate-900 p-5"><h2 class="font-bold">USDT {{ k }}</h2><div class="mt-2 text-slate-300 break-all">{{ v }}</div></div>{% endfor %}</div><form method="post" class="mt-6 rounded-2xl border border-slate-800 bg-slate-900 p-5 space-y-3"><select name="payment_method" required class="w-full rounded-xl bg-slate-950 border border-slate-700 p-3"><option value="">Méthode de paiement</option><option>ORANGE</option><option>MOOV</option><option>WAVE</option><option>USDT TRC20</option><option>USDT ERC20</option></select><input name="payment_reference" required placeholder="Référence / TXID" class="w-full rounded-xl bg-slate-950 border border-slate-700 p-3"><button class="rounded-xl bg-emerald-500 text-slate-950 font-bold px-5 py-3">Soumettre le paiement</button></form>"""
DASHBOARD_HTML = """{{ nav|safe }}
<div class="mb-5 overflow-hidden rounded-2xl glass border-cyan-500/10"><div class="whitespace-nowrap flex gap-10 py-2 text-[11px] text-slate-400 ticker w-max"><span>BTCUSD <b class="text-emerald-300">● LIVE</b></span><span>XAUUSD <b class="text-emerald-300">● LIVE</b></span><span>EURUSD <b class="text-emerald-300">● LIVE</b></span><span>GBPUSD <b class="text-emerald-300">● LIVE</b></span><span>SMC ENGINE <b class="text-cyan-300">ACTIVE</b></span><span>NOVA <b class="text-cyan-300">PIPELINE</b></span><span>BTCUSD <b class="text-emerald-300">● LIVE</b></span><span>XAUUSD <b class="text-emerald-300">● LIVE</b></span><span>EURUSD <b class="text-emerald-300">● LIVE</b></span></div></div>
<header class="grid xl:grid-cols-[1fr_auto] gap-5 mb-5"><div><div class="flex items-center gap-2 text-xs text-cyan-300 font-bold tracking-[.3em]"><span class="w-2 h-2 rounded-full bg-cyan-400 pulse"></span>NOVA MARKET COMMAND</div><h1 class="mt-2 text-4xl md:text-6xl font-black tracking-tight">Bienvenue, {{ user.username }}</h1><p class="mt-2 text-slate-400 max-w-2xl">Votre centre de surveillance NOVA : contexte marché, opportunités, exécution et historique réunis dans une seule interface.</p></div><div class="flex items-start gap-2"><a href="/payment" class="rounded-xl bg-gradient-to-r from-cyan-300 to-blue-500 text-slate-950 font-black px-4 py-3 shadow-glow">{{ 'Gérer mon VIP' if user.access_status=='VIP' else 'Passer VIP' }}</a><a href="/logout" class="rounded-xl glass px-4 py-3">Sortir</a></div></header>
<div class="grid grid-cols-2 xl:grid-cols-4 gap-3 mb-5"><div class="glass cardhover rounded-2xl p-4"><div class="text-xs text-slate-500">STATUT</div><div id="status" class="mt-2 text-xl font-black text-cyan-300">{{ user.access_status }}</div><div class="text-xs text-slate-500 mt-1">Compte NOVA</div></div><div class="glass cardhover rounded-2xl p-4"><div class="text-xs text-slate-500">MARCHÉS</div><div class="mt-2 text-xl font-black">04</div><div class="text-xs text-slate-500 mt-1">BTC · XAU · EUR · GBP</div></div><div class="glass cardhover rounded-2xl p-4"><div class="text-xs text-slate-500">ENGINE</div><div class="mt-2 text-xl font-black text-emerald-300">ONLINE</div><div class="text-xs text-slate-500 mt-1">Analyse algorithmique active</div></div><div class="glass cardhover rounded-2xl p-4"><div class="text-xs text-slate-500">DERNIÈRE SYNCHRO</div><div id="sync" class="mt-2 text-xl font-black">—</div><div class="text-xs text-slate-500 mt-1">actualisation automatique</div></div></div>
<div id="trial" class="hidden mb-5 rounded-2xl border border-cyan-400/20 bg-cyan-400/5 p-5"><div class="flex flex-col sm:flex-row sm:items-center justify-between gap-3"><div><b class="text-cyan-200">Votre période TRIAL est active</b><p class="text-sm text-slate-400 mt-1">Profitez de l'analyse NOVA avant de passer VIP.</p></div><div id="countdown" class="text-2xl md:text-3xl font-black font-mono text-cyan-300"></div></div></div>
<div id="expired" class="hidden rounded-3xl border border-red-400/20 bg-red-500/5 p-10 text-center"><div class="text-5xl">🔒</div><h2 class="text-3xl font-black mt-3">Accès temporairement verrouillé</h2><p class="text-slate-400 mt-2">Votre abonnement doit être renouvelé pour retrouver les fonctions premium.</p><a href="/payment" class="inline-block mt-5 rounded-xl bg-cyan-300 text-slate-950 font-black px-5 py-3">Réactiver mon accès</a></div>
<div id="app" class="space-y-5"></div>
<script>
const initial={{ data|tojson }}; let expires={{ (user.subscription_expires_at|tojson) }}; const initialUser={{ user|tojson }};
function esc(x){return String(x??'').replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#039;'}[m]));}
function section(icon,title,sub,html){return `<section class="glass cardhover rounded-3xl p-4 md:p-5"><div class="flex flex-col sm:flex-row sm:items-center justify-between gap-2 mb-4"><div><h2 class="font-black text-lg md:text-xl">${icon} ${title}</h2><p class="text-xs text-slate-500 mt-1">${sub}</p></div><span class="text-[10px] uppercase tracking-[.2em] text-slate-500">NOVA LIVE</span></div>${html}</section>`}
function render(d){const u=d.user||initialUser; expires=u.subscription_expires_at; document.getElementById('status').textContent=u.access_status; document.getElementById('sync').textContent=new Date().toLocaleTimeString('fr-FR',{hour:'2-digit',minute:'2-digit',second:'2-digit'}); if(u.access_status==='EXPIRED'||u.access_status==='PENDING_VALIDATION'){document.getElementById('app').innerHTML='';document.getElementById('expired').classList.remove('hidden');document.getElementById('trial').classList.add('hidden');return;} document.getElementById('expired').classList.add('hidden'); document.getElementById('trial').classList.toggle('hidden',u.access_status!=='TRIAL');
let r=Object.values(d.radar||{}).map(x=>{let bull=(x.bias||'').includes('BULL');return `<div class="rounded-2xl border border-slate-800/80 bg-slate-950/60 p-4"><div class="flex items-center justify-between"><div class="flex items-center gap-2"><span class="w-2.5 h-2.5 rounded-full ${bull?'bg-emerald-400':'bg-red-400'}"></span><b>${esc(x.symbol)}</b></div><span class="text-xs font-bold ${bull?'text-emerald-300':'text-red-300'}">${esc(x.bias||'NEUTRAL')}</span></div><div class="mt-3 flex items-center justify-between text-xs"><span class="text-slate-500">Structure</span><b class="text-slate-300">${esc(x.bos||'—')}</b></div><div class="mt-3"><div class="text-[10px] uppercase text-slate-600 mb-1">Support</div>${(x.supports||[]).slice(0,2).map(z=>`<span class="inline-block rounded-lg bg-emerald-400/5 border border-emerald-400/10 px-2 py-1 mr-1 mb-1 text-[11px] text-emerald-200">${z.zone_min??''} — ${z.zone_max??''}</span>`).join('')||'<span class="text-xs text-slate-600">Aucune zone</span>'}</div><div class="mt-2"><div class="text-[10px] uppercase text-slate-600 mb-1">Résistance</div>${(x.resistances||[]).slice(0,2).map(z=>`<span class="inline-block rounded-lg bg-red-400/5 border border-red-400/10 px-2 py-1 mr-1 mb-1 text-[11px] text-red-200">${z.zone_min??''} — ${z.zone_max??''}</span>`).join('')||'<span class="text-xs text-slate-600">Aucune zone</span>'}</div></div>`}).join('');
let p=(d.pipeline||[]).map(x=>`<div class="rounded-2xl bg-slate-950/60 border border-slate-800/80 p-4 cardhover"><div class="flex justify-between gap-2"><b>${esc(x.symbol)} <span class="text-slate-500">· ${esc(x.direction)}</span></b><span class="text-[10px] text-slate-500">${esc(x.candidate_id||'')}</span></div><div class="mt-3 rounded-xl bg-cyan-400/5 border border-cyan-400/10 p-3"><div class="text-cyan-300 text-sm font-bold">${esc(x.status_label||x.status)}</div><div class="text-xs text-slate-500 mt-1">Prix trigger : <b class="text-slate-300">${esc(x.trigger_price)}</b></div></div></div>`).join('')||'<div class="rounded-2xl bg-slate-950/40 p-6 text-center text-slate-600">Aucune opportunité dans le pipeline.</div>';
let t=(d.active_trades||[]).map(x=>`<div class="rounded-2xl bg-slate-950/60 border border-slate-800/80 p-4"><div class="flex flex-col sm:flex-row sm:items-center justify-between gap-2"><div><b class="text-lg">${esc(x.symbol)}</b><span class="ml-2 text-xs ${x.direction==='BUY'?'text-emerald-300':'text-red-300'}">${esc(x.direction)}</span></div><span class="rounded-full bg-cyan-400/10 text-cyan-300 px-3 py-1 text-xs">${esc(x.status)}</span></div><div class="grid grid-cols-2 md:grid-cols-5 gap-2 mt-4">${[['Entry',x.entry],['SL',x.sl],['TP1',x.tp1],['TP2',x.tp2],['TP3',x.tp3]].map((a,i)=>`<div class="rounded-xl bg-slate-900/80 p-3"><div class="text-[10px] text-slate-600">${a[0]}</div><div class="font-bold mt-1">${x[['tp1_hit','tp2_hit','tp3_hit'][i-2]]?'🔒 ':''}${a[1]??'—'}</div></div>`).join('')}</div><div class="mt-3 text-sm text-cyan-300 font-bold">RR ${x.rr??'—'}</div></div>`).join('')||'<div class="rounded-2xl bg-slate-950/40 p-6 text-center text-slate-600">Aucun trade actif.</div>';
let h=(d.trade_history||[]).slice(0,8).map(x=>`<tr class="border-t border-slate-800/70"><td class="py-3 font-bold">${esc(x.symbol)}</td><td>${esc(x.direction)}</td><td><span class="rounded-lg px-2 py-1 text-xs ${(x.result||'').includes('WIN')||(x.result||'').includes('TP')?'bg-emerald-400/10 text-emerald-300':'bg-red-400/10 text-red-300'}">${esc(x.result||x.status)}</span></td><td>${x.profit??'—'}</td></tr>`).join('');
let vip=u.access_status==='VIP'?`<section class="rounded-3xl border border-cyan-400/15 bg-gradient-to-r from-cyan-400/5 to-blue-500/5 p-5"><div class="flex flex-col md:flex-row md:items-center justify-between gap-4"><div><div class="text-xs tracking-[.2em] text-cyan-300">ESPACE VIP</div><h2 class="text-xl font-black mt-1">Canal Telegram privé</h2><p class="text-sm text-slate-500 mt-1">Votre accès premium est actif.</p></div><button onclick="invite()" class="rounded-xl bg-cyan-300 text-slate-950 font-black px-5 py-3">Rejoindre Telegram →</button></div><div id="invite" class="mt-3 break-all text-sm text-cyan-300"></div></section>`:'';
document.getElementById('app').innerHTML=section('📡','Radar NOVA','Contexte, biais, BOS et zones majeures',`<div class="grid md:grid-cols-2 xl:grid-cols-4 gap-3">${r}</div>`)+section('⏳','Pipeline d’opportunités','Pipeline de confirmation',`<div class="grid md:grid-cols-3 gap-3">${p}</div>`)+section('⚡','Trades actifs','Entrée, risque, objectifs et verrous TP',`<div class="space-y-3">${t}</div>`)+section('📒','Journal récent','Les dernières opérations enregistrées',`<div class="overflow-auto"><table class="w-full text-sm"><thead class="text-xs text-slate-600"><tr><th class="text-left py-2">Marché</th><th class="text-left">Sens</th><th class="text-left">Résultat</th><th class="text-left">P&L</th></tr></thead><tbody>${h||'<tr><td colspan="4" class="py-6 text-center text-slate-600">Aucun historique.</td></tr>'}</tbody></table></div><a href="/history" class="inline-block mt-4 text-sm text-cyan-300">Voir tout l’historique →</a>`)+vip;}
async function refresh(){try{const r=await fetch('/api/dashboard',{cache:'no-store'});if(r.ok)render(await r.json());}catch(e){}} async function invite(){const r=await fetch('/api/telegram-invite');const d=await r.json();document.getElementById('invite').innerHTML=d.invite_link?`<a class="underline" target="_blank" href="${esc(d.invite_link)}">${esc(d.invite_link)}</a>`:esc(d.error);} function tick(){if(!expires)return;const ms=new Date(expires).getTime()-Date.now();if(ms<=0){document.getElementById('countdown').textContent='Expiré';return;}const d=Math.floor(ms/86400000),h=Math.floor(ms%86400000/3600000),m=Math.floor(ms%3600000/60000),s=Math.floor(ms%60000/1000);document.getElementById('countdown').textContent=`${d}j ${h}h ${m}m ${s}s`;}
render(initial);setInterval(refresh,5000);setInterval(tick,1000);tick();</script>"""
ADMIN_HTML = """{% extends_base %}{{ nav|safe }}<div class="flex items-center justify-between mb-6 gap-3"><div><div class="text-xs tracking-[.3em] text-cyan-400">NOVA ADMIN</div><h1 class="text-3xl font-bold">CRM & contrôle</h1><p class="text-sm text-slate-400 mt-1">Gestion des comptes, paiements et accès VIP.</p></div><a href="/nova-admin/logout" class="border border-slate-700 rounded-xl px-4 py-2">Sortir</a></div><div class="grid grid-cols-2 md:grid-cols-6 gap-3 mb-6">{% for k,v in metrics.items() %}<div class="rounded-2xl border border-slate-800 bg-slate-900 p-4"><div class="text-xs text-slate-500 uppercase">{{ k }}</div><div class="text-2xl font-bold">{{ v }}</div></div>{% endfor %}</div><div class="rounded-2xl border border-amber-900 bg-amber-950/20 p-5 mb-6"><b>💳 Validation des paiements</b><p class="text-sm text-slate-400 mt-1">Lorsqu'un client soumet une référence, son compte passe en attente. Vérifie la méthode, la référence et le montant sur ton moyen de paiement, puis accepte ou refuse.</p></div><div class="rounded-2xl border border-red-900 bg-red-950/20 p-5 mb-6"><div class="flex items-center justify-between"><div><b>🚨 PANIC</b><div class="text-sm text-slate-400">Mode test : n'agit pas sur le moteur de trading.</div></div><form method="post" action="/nova-admin/panic"><button class="rounded-xl bg-red-500 text-white font-bold px-4 py-2">PANIC</button></form></div></div><div class="rounded-2xl border border-slate-800 bg-slate-900 p-5 overflow-auto"><table class="w-full text-sm min-w-[1100px]"><thead><tr class="text-slate-500"><th class="text-left py-2">Utilisateur</th><th class="text-left">Statut</th><th class="text-left">Méthode</th><th class="text-left">Référence / TXID</th><th class="text-left">IP</th><th class="text-left">Fin</th><th class="text-left">Actions</th></tr></thead><tbody>{% for u in users %}<tr class="border-t border-slate-800"><td class="py-3"><div class="font-bold">{{ u.username }}</div><div class="text-xs text-slate-500">{{ u.email }}</div></td><td><span class="rounded-lg px-2 py-1 {% if u.access_status=='PENDING_VALIDATION' %}bg-amber-950 text-amber-300{% elif u.access_status=='VIP' %}bg-emerald-950 text-emerald-300{% elif u.access_status=='PAYMENT_REJECTED' %}bg-red-950 text-red-300{% else %}bg-slate-800 text-slate-300{% endif %}">{{ u.access_status }}</span></td><td>{{ u.payment_method or '—' }}</td><td class="font-mono text-xs">{{ u.payment_reference or '—' }}</td><td>{{ u.last_ip or '—' }}</td><td>{{ u.subscription_expires_at or 'Lifetime' }}</td><td><div class="flex flex-wrap gap-2">{% if u.access_status=='PENDING_VALIDATION' %}<form method="post" action="/nova-admin/validate/{{ u.user_id }}"><select name="duration" class="bg-slate-950 border border-slate-700 rounded p-2"><option value="7d">7 jours</option><option value="1m">1 mois</option><option value="3m">3 mois</option></select><button class="bg-emerald-500 text-slate-950 rounded px-3 py-2 font-bold">✓ Accepter</button></form><form method="post" action="/nova-admin/reject/{{ u.user_id }}"><button class="bg-red-500/90 text-white rounded px-3 py-2 font-bold">✕ Refuser</button></form>{% endif %}<form method="post" action="/nova-admin/free-vip/{{ u.user_id }}"><select name="duration" class="bg-slate-950 border border-slate-700 rounded p-2"><option value="7d">7 jours</option><option value="1m">1 mois</option><option value="lifetime">Lifetime</option></select><button class="bg-cyan-500 text-slate-950 rounded px-3 py-2 font-bold">Free VIP</button></form></div></td></tr>{% else %}<tr><td colspan="7" class="py-8 text-center text-slate-500">Aucun utilisateur.</td></tr>{% endfor %}</tbody></table></div>"""


def now(): return datetime.now(timezone.utc)
def iso(x): return x.astimezone(timezone.utc).isoformat() if x else None
def dt(x):
    try:
        x=x.replace('Z','+00:00'); d=datetime.fromisoformat(x); return d.replace(tzinfo=timezone.utc) if d.tzinfo is None else d.astimezone(timezone.utc)
    except Exception: return None

def conn():
    c=sqlite3.connect(DB,timeout=15,check_same_thread=False); c.row_factory=sqlite3.Row; c.execute('PRAGMA journal_mode=WAL'); c.execute('PRAGMA busy_timeout=15000'); return c

def init_db():
    with DB_LOCK:
        c=conn(); c.execute('''CREATE TABLE IF NOT EXISTS users(user_id TEXT PRIMARY KEY,username TEXT,email TEXT,telegram_user_id TEXT,date_inscription TEXT NOT NULL,access_status TEXT NOT NULL,subscription_expires_at TEXT,has_received_3day_warning INTEGER DEFAULT 0,last_ip TEXT,payment_reference TEXT,payment_method TEXT,created_at TEXT NOT NULL,updated_at TEXT NOT NULL)'''); c.commit(); c.close()

def users():
    with DB_LOCK:
        c=conn(); r=[dict(x) for x in c.execute('SELECT * FROM users ORDER BY created_at DESC')]; c.close();
        for x in r:x['has_received_3day_warning']=bool(x['has_received_3day_warning'])
        return r

def user(uid):
    with DB_LOCK:
        c=conn(); x=c.execute('SELECT * FROM users WHERE user_id=?',(uid,)).fetchone(); c.close()
    if not x:return None
    x=dict(x);x['has_received_3day_warning']=bool(x['has_received_3day_warning']);return refresh(x)

def update(uid,**kw):
    allowed={'username','email','telegram_user_id','access_status','subscription_expires_at','has_received_3day_warning','last_ip','payment_reference','payment_method'}; kw={k:v for k,v in kw.items() if k in allowed}; kw['updated_at']=iso(now())
    with DB_LOCK:
        c=conn(); c.execute('UPDATE users SET '+','.join(f'{k}=?' for k in kw)+' WHERE user_id=?',[*kw.values(),uid]);c.commit();c.close()

def find_user_by_email(email):
    email=(email or '').strip().lower()
    if not email:return None
    with DB_LOCK:
        c=conn();x=c.execute('SELECT * FROM users WHERE lower(email)=? ORDER BY created_at ASC LIMIT 1',(email,)).fetchone();c.close()
    if not x:return None
    x=dict(x);x['has_received_3day_warning']=bool(x['has_received_3day_warning']);return refresh(x)

def create_user(name,email,tg):
    email=(email or '').strip().lower(); existing=find_user_by_email(email)
    if existing:
        if tg and not existing.get('telegram_user_id'):update(existing['user_id'],telegram_user_id=tg,last_ip=request.remote_addr or '')
        else:update(existing['user_id'],last_ip=request.remote_addr or '')
        return user(existing['user_id'])
    t=now(); uid=secrets.token_urlsafe(12); exp=t+timedelta(days=7)
    with DB_LOCK:
        c=conn();c.execute('INSERT INTO users VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',(uid,name,email,tg,iso(t),'TRIAL',iso(exp),0,request.remote_addr or '', '', '',iso(t),iso(t)));c.commit();c.close()
    return user(uid)

def telegram(method,payload):
    if not TOKEN:return {'ok':False,'error':'TELEGRAM_BOT_TOKEN non configuré'}
    try:
        r=requests.post(f'https://api.telegram.org/bot{TOKEN}/{method}',json=payload,timeout=15);r.raise_for_status();return r.json()
    except Exception as e:return {'ok':False,'error':str(e)}

def send_telegram_message(chat_id,text): return telegram('sendMessage',{'chat_id':chat_id,'text':text}) if chat_id else {'ok':False,'error':'chat_id manquant'}
def telegram_remove(tg):
    if DEMO:return {'ok':True,'demo':True}
    if not CHAT or not tg:return {'ok':False,'error':'Telegram configuration missing'}
    a=telegram('banChatMember',{'chat_id':CHAT,'user_id':tg,'revoke_messages':True})
    if not a.get('ok'):return a
    time.sleep(.4);return telegram('unbanChatMember',{'chat_id':CHAT,'user_id':tg,'only_if_banned':True})
def telegram_invite():
    if DEMO:return {'ok':True,'result':{'invite_link':'#demo-telegram-invite'}}
    return telegram('createChatInviteLink',{'chat_id':CHAT,'name':'NOVA VIP'}) if CHAT else {'ok':False,'error':'TELEGRAM_PRIVATE_CHAT_ID non configuré'}

def refresh(u):
    if u['access_status'] in ('TRIAL','VIP'):
        e=dt(u.get('subscription_expires_at'))
        if e and now()>=e:
            update(u['user_id'],access_status='EXPIRED')
            if u['access_status']=='VIP':telegram_remove(u.get('telegram_user_id',''))
            u=user_raw(u['user_id'])
    return u

def user_raw(uid):
    with DB_LOCK:
        c=conn();x=c.execute('SELECT * FROM users WHERE user_id=?',(uid,)).fetchone();c.close()
    if not x:return None
    x=dict(x);x['has_received_3day_warning']=bool(x['has_received_3day_warning']);return x

def worker():
    while True:
        try:
            n=now()
            for u in users():
                e=dt(u.get('subscription_expires_at'))
                if not e or u['access_status'] not in ('TRIAL','VIP'):continue
                if n>=e:
                    update(u['user_id'],access_status='EXPIRED');
                    if u['access_status']=='VIP':telegram_remove(u.get('telegram_user_id',''))
                elif u['access_status']=='VIP' and not u['has_received_3day_warning'] and timedelta(0)<e-n<=timedelta(hours=72):
                    r=send_telegram_message(u.get('telegram_user_id',''),'⚠️ Alerte NOVA VIP : Votre abonnement expire dans 3 jours. Pensez à vous réabonner pour ne pas perdre vos accès.')
                    if r.get('ok'):update(u['user_id'],has_received_3day_warning=True)
        except Exception:pass
        time.sleep(86400)

def json_load(name,default):
    try:
        with JSON_LOCK:
            p=BASE/name
            if not p.exists():return default
            with p.open(encoding='utf8') as f:return json.load(f)
    except Exception:return default

def vals(x):return list(x.values()) if isinstance(x,dict) else x if isinstance(x,list) else []
def demo_radar():
    return {s:{'symbol':s,'bias':('BULLISH' if s in ('BTCUSD','EURUSD') else 'BEARISH'),'bos':('BULLISH_BOS' if s in ('BTCUSD','EURUSD') else 'BEARISH_BOS'),'supports':[{'zone_min':100,'zone_max':101,'touches':2,'is_active':True}],'resistances':[{'zone_min':105,'zone_max':106,'touches':2,'is_active':True}]} for s in ('BTCUSD','XAUUSD','EURUSD','GBPUSD')}
def dashboard_data():
    z=json_load('m15_major_zones.json',{}); st=json_load('m15_zone_state.json',{}); radar={}
    for s in ('BTCUSD','XAUUSD','EURUSD','GBPUSD'):
        zz=z.get(s,[]) if isinstance(z,dict) else []; ss=st.get(s,{}) if isinstance(st,dict) else {}; radar[s]={'symbol':s,'bias':ss.get('bias'),'bos':ss.get('bos'),'supports':[x for x in vals(zz) if isinstance(x,dict) and x.get('level_type')=='support' and x.get('is_active',True)],'resistances':[x for x in vals(zz) if isinstance(x,dict) and x.get('level_type')=='resistance' and x.get('is_active',True)]}
    p=vals(json_load('pending_opportunities.json',{}));
    for x in p:
        if isinstance(x,dict):x['status_label']=LABELS.get(x.get('status',''),x.get('status',''))
    t=vals(json_load('active_trades.json',{})); h=vals(json_load('trade_history.json',{}))[-50:][::-1]
    if DEMO and not any(x.get('bias') for x in radar.values()):radar=demo_radar()
    if DEMO and not p:p=[{'candidate_id':'DEMO-001','symbol':'XAUUSD','direction':'HAUSSIER','trigger_price':3851.25,'status':'WAITING_M5_LIQUIDITY','status_label':LABELS['WAITING_M5_LIQUIDITY']},{'candidate_id':'DEMO-002','symbol':'BTCUSD','direction':'BAISSIER','trigger_price':108420,'status':'WAITING_M1_CHOCH','status_label':LABELS['WAITING_M1_CHOCH']}]
    if DEMO and not t:t=[{'trade_id':'DEMO-TRD-001','symbol':'XAUUSD','direction':'BUY','entry':3851.25,'sl':3845.90,'tp1':3856.60,'tp2':3864.80,'tp3':3872.90,'rr':4.04,'tp1_hit':True,'tp2_hit':False,'tp3_hit':False,'status':'TP1_HIT'}]
    if DEMO and not h:h=[{'trade_id':'H1','symbol':'EURUSD','direction':'BUY','result':'WIN','profit':182.4},{'trade_id':'H2','symbol':'GBPUSD','direction':'SELL','result':'LOSS','profit':-74.2}]
    with PANIC_LOCK:panic=dict(PANIC)
    return {'radar':radar,'pipeline':p,'active_trades':t,'trade_history':h,'panic':panic,'generated_at':iso(now())}


def page(template, **ctx):
    tpl=globals()[template]
    ctx.setdefault('nav', NAV_HTML)
    body=render_template_string(tpl.replace('{% extends_base %}',''), **ctx)
    return render_template_string(BASE_HTML, body=body, **ctx)

def current():return user(session.get('uid')) if session.get('uid') else None
def admin_required(f):
    @wraps(f)
    def w(*a,**k):return f(*a,**k) if session.get('admin') else redirect(url_for('admin_login'))
    return w

@app.route('/register',methods=['GET','POST'])
def register():
    if request.method=='POST':
        name=request.form.get('username','').strip();email=request.form.get('email','').strip().lower();tg=request.form.get('telegram_user_id','').strip()
        if not name or not email:flash('Nom et email requis.','error');return redirect(url_for('register'))
        existing=find_user_by_email(email)
        if existing:
            if existing.get('username') and name and name != existing['username']:
                flash('Compte existant retrouvé. Votre abonnement et vos données ont été conservés.','success')
            if tg and not existing.get('telegram_user_id'):update(existing['user_id'],telegram_user_id=tg,last_ip=request.remote_addr or '')
            else:update(existing['user_id'],last_ip=request.remote_addr or '')
            session['uid']=existing['user_id'];return redirect('/')
        u=create_user(name,email,tg);session['uid']=u['user_id'];return redirect('/')
    return page('REGISTER_HTML', title='NOVA — Inscription / Connexion')
@app.route('/login',methods=['GET','POST'])
def login():
    if request.method=='POST':
        email=request.form.get('email','').strip().lower()
        u=find_user_by_email(email)
        if not u:
            flash('Aucun compte NOVA trouvé avec cet email.','error');return redirect(url_for('login'))
        update(u['user_id'],last_ip=request.remote_addr or '')
        session['uid']=u['user_id'];return redirect('/')
    return page('LOGIN_HTML', title='NOVA — Connexion')
@app.route('/logout')
def logout():session.pop('uid',None);return redirect(url_for('login'))
@app.route('/')
def index():
    u=current();return redirect(url_for('register')) if not u else page('DASHBOARD_HTML', title='NOVA — Dashboard', user=u, data=dashboard_data())
@app.route('/api/dashboard')
def api_dashboard():
    u=current()
    if not u:return jsonify(error='unauthorized'),401
    d=dashboard_data();
    if u['access_status'] not in ('TRIAL','VIP'):d.update(radar=None,pipeline=None,active_trades=None)
    d['user']=u;return jsonify(d)
@app.route('/api/telegram-invite')
def invite():
    u=current()
    if not u or u['access_status']!='VIP':return jsonify(error='Réservé aux VIP'),403
    r=telegram_invite();return jsonify(r.get('result',{})) if r.get('ok') else (jsonify(error=r.get('error')),503)
@app.route('/payment',methods=['GET','POST'])
def payment():
    u=current()
    if not u:return redirect(url_for('register'))
    if request.method=='POST':
        ref=request.form.get('payment_reference','').strip();method=request.form.get('payment_method','').strip()
        if not ref or not method:flash('Méthode et référence requises.','error');return redirect(url_for('payment'))
        update(u['user_id'],access_status='PENDING_VALIDATION',payment_reference=ref,payment_method=method);flash('Paiement soumis pour validation.','success');return redirect('/')
    return page('PAYMENT_HTML', title='NOVA — Paiement', mobile_money=MM, usdt=USDT)
@app.route('/nova-admin',methods=['GET','POST'])
def admin_login():
    if session.get('admin'):return redirect(url_for('admin_dashboard'))
    if request.method=='POST' and ADMIN and secrets.compare_digest(request.form.get('password',''),ADMIN):session['admin']=True;return redirect(url_for('admin_dashboard'))
    if request.method=='POST':flash('Mot de passe incorrect.','error')
    return page('ADMIN_LOGIN_HTML', title='NOVA — Admin')
@app.route('/nova-admin/logout')
def admin_logout():session.pop('admin',None);return redirect(url_for('admin_login'))
@app.route('/nova-admin/dashboard')
@admin_required
def admin_dashboard():
    us=users();m={'total':len(us),'vip':sum(x['access_status']=='VIP' for x in us),'trial':sum(x['access_status']=='TRIAL' for x in us),'expired':sum(x['access_status']=='EXPIRED' for x in us),'pending':sum(x['access_status']=='PENDING_VALIDATION' for x in us),'rejected':sum(x['access_status']=='PAYMENT_REJECTED' for x in us)}
    return page('ADMIN_HTML', title='NOVA — Admin Dashboard', users=us, metrics=m, panic=PANIC)
@app.route('/nova-admin/validate/<uid>',methods=['POST'])
@admin_required
def validate(uid):
    d={'7d':7,'1m':30,'3m':90}.get(request.form.get('duration'))
    if not d:flash('Durée invalide.','error');return redirect(url_for('admin_dashboard'))
    update(uid,access_status='VIP',subscription_expires_at=iso(now()+timedelta(days=d)),has_received_3day_warning=False);flash('VIP validé.','success');return redirect(url_for('admin_dashboard'))
@app.route('/nova-admin/reject/<uid>',methods=['POST'])
@admin_required
def reject_payment(uid):
    if not user_raw(uid):
        flash('Utilisateur introuvable.','error');return redirect(url_for('admin_dashboard'))
    update(uid,access_status='PAYMENT_REJECTED')
    flash('Paiement refusé. Le client peut soumettre une nouvelle référence.','success')
    return redirect(url_for('admin_dashboard'))
@app.route('/nova-admin/free-vip/<uid>',methods=['POST'])
@admin_required
def freevip(uid):
    d=request.form.get('duration');e=None if d=='lifetime' else now()+timedelta(days={'7d':7,'1m':30}.get(d,0));update(uid,access_status='VIP',subscription_expires_at=iso(e),has_received_3day_warning=False);flash('Accès VIP accordé.','success');return redirect(url_for('admin_dashboard'))
@app.route('/nova-admin/panic',methods=['POST'])
@admin_required
def panic():
    with PANIC_LOCK:PANIC.update(active=True,requested_at=iso(now()))
    flash('PANIC activé. En mode test, main.py reste totalement indépendant.','success');return redirect(url_for('admin_dashboard'))
@app.route('/nova-admin/panic/reset',methods=['POST'])
@admin_required
def panic_reset():
    with PANIC_LOCK:PANIC.update(active=False,requested_at=None)
    return redirect(url_for('admin_dashboard'))

def history_records():
    h=vals(json_load('trade_history.json',{}))
    if DEMO and not h:
        h=[
            {'trade_id':'DEMO-H1','symbol':'EURUSD','direction':'BUY','entry':1.17250,'result':'WIN','profit':182.40,'closed_at':'2026-10-02 14:22'},
            {'trade_id':'DEMO-H2','symbol':'GBPUSD','direction':'SELL','entry':1.35120,'result':'LOSS','profit':-74.20,'closed_at':'2026-10-02 10:15'},
            {'trade_id':'DEMO-H3','symbol':'XAUUSD','direction':'BUY','entry':3848.20,'result':'TP3_HIT','profit':321.60,'closed_at':'2026-10-01 18:41'},
            {'trade_id':'DEMO-H4','symbol':'BTCUSD','direction':'SELL','entry':108420.00,'result':'SL','profit':-95.00,'closed_at':'2026-10-01 12:07'},
        ]
    return h[::-1] if h else []

@app.route('/history')
def history_page():
    h=history_records(); win=sum(1 for x in h if x.get('result') in ('WIN','TP3_HIT')); loss=sum(1 for x in h if x.get('result') in ('LOSS','SL'))
    pnl=sum(float(x.get('profit',x.get('pnl',0)) or 0) for x in h if isinstance(x,dict))
    return page('PUBLIC_HTML',title='NOVA — Historique',content=render_template_string(HISTORY_HTML,history=h,stats={'total':len(h),'win':win,'loss':loss,'pnl':pnl}))

@app.route('/strategy')
def strategy_page():
    return page('PUBLIC_HTML',title='NOVA — Stratégie',content=render_template_string(STRATEGY_HTML))

@app.route('/founder')
def founder_page():
    return page('PUBLIC_HTML',title='NOVA — Fondateur',content=render_template_string(FOUNDER_HTML))

@app.route('/roadmap')
def roadmap_page():
    timeline=[
        ('01','Conception de NOVA','Création de l’idée NOVA TRADE AI autour d’une approche structurée du marché et de l’automatisation.'),
        ('02','Structure SMC + Price Action','Mise en place de la lecture de structure, des zones, BOS et configurations Price Action.'),
        ('03','Pipeline NOVA','Séparation du contexte, de la liquidité et de la confirmation d’exécution.'),
        ('04','Journal & suivi','Ajout du suivi des opportunités, positions, résultats et historique des trades.'),
        ('05','Écosystème Telegram','Séparation des alertes publiques et du contrôle privé du propriétaire.'),
        ('06','Site NOVA','Création d’une interface web avec dashboard, CRM VIP, paiements, historique et documentation.'),
        ('07','Prochaine phase','Connexion contrôlée entre le site et le bot après validation complète de l’interface.')
    ]
    return page('PUBLIC_HTML',title='NOVA — Évolution',content=render_template_string(ROADMAP_HTML,timeline=timeline))

@app.route('/faq')
def faq_page():
    faqs=[
        ('NOVA exécute-t-il automatiquement les trades ?','Le Site NOVA de test est indépendant de main.py. Il affiche et organise les informations sans modifier le moteur de trading.'),
        ('Quels marchés sont suivis ?','BTCUSD, XAUUSD, EURUSD et GBPUSD font partie du périmètre NOVA actuel.'),
        ('Comment NOVA construit-il un signal ?','NOVA combine contexte de marché, liquidité et confirmations successives avant de préparer une opportunité.'),
        ('Que contient le journal ?','Les opérations enregistrées peuvent afficher le symbole, la direction, l’entrée, le résultat, le P&L et les informations disponibles dans trade_history.json.'),
        ('Quelle est la différence TRIAL et VIP ?','Le TRIAL dure 7 jours. Le VIP donne accès aux fonctions premium et au lien du canal Telegram privé.'),
        ('Le Site NOVA modifie-t-il mon bot ?','Non dans cette phase de test. Le site lit éventuellement les fichiers JSON du bot mais ne les modifie pas.')
    ]
    return page('PUBLIC_HTML',title='NOVA — FAQ',content=render_template_string(FAQ_HTML,faqs=faqs))

@app.route('/health')
def health():return jsonify(status='ok',service='Site NOVA',mode='DEMO' if DEMO else 'LIVE')

init_db()
threading.Thread(target=worker,daemon=True,name='nova-subscriptions').start()
if __name__=='__main__':app.run(host='0.0.0.0',port=int(os.getenv('PORT','8080')),debug=False,threaded=True)
