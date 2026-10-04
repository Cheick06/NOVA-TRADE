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
ADMIN=os.getenv('NOVA_ADMIN_PASSWORD',''); SECRET=os.getenv('NOVA_SITE_SECRET',secrets.token_hex(32))
MM={'ORANGE':os.getenv('NOVA_ORANGE_MONEY','À CONFIGURER'),'MOOV':os.getenv('NOVA_MOOV_MONEY','À CONFIGURER'),'WAVE':os.getenv('NOVA_WAVE_MONEY','À CONFIGURER')}
USDT={'TRC20':os.getenv('NOVA_USDT_TRC20','À CONFIGURER'),'ERC20':os.getenv('NOVA_USDT_ERC20','À CONFIGURER')}
app=Flask(__name__); app.secret_key=SECRET
DB_LOCK=threading.RLock(); JSON_LOCK=threading.RLock(); PANIC_LOCK=threading.RLock()
PANIC={'active':False,'requested_at':None}
LABELS={'WAITING_M5_LIQUIDITY':'Attente Liquidité M5','WAITING_M1_CHOCH':'Attente CHoCH M1','WAITING_M1_BOS':'Attente Confirmation BOS M1','WAITING_CONFIRMATION_BOS_M1':'Attente Confirmation BOS M1'}

BASE_HTML = """<!doctype html>
<html lang="fr" class="dark"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>{{ title or 'NOVA' }}</title><script src="https://cdn.tailwindcss.com"></script><script>tailwind.config={darkMode:'class'}</script></head>
<body class="bg-slate-950 text-slate-100 min-h-screen"><div class="max-w-7xl mx-auto p-4 md:p-6">{% with messages=get_flashed_messages(with_categories=true) %}{% for category,message in messages %}<div class="mb-3 rounded-xl border border-slate-700 bg-slate-900 px-4 py-3 text-sm">{{ message }}</div>{% endfor %}{% endwith %}{{ body|safe }}</div></body></html>"""
REGISTER_HTML = """{% extends_base %}<div class="min-h-[80vh] flex items-center justify-center"><div class="w-full max-w-md rounded-2xl border border-slate-800 bg-slate-900 p-6 shadow-2xl"><div class="text-xs tracking-[.3em] text-cyan-400">NOVA TRADE AI</div><h1 class="mt-2 text-3xl font-bold">Créer votre accès</h1><p class="mt-2 text-slate-400">Essai TRIAL de 7 jours avec accès complet au radar M15 et aux signaux M1.</p><form method="post" class="mt-6 space-y-3"><input name="username" required placeholder="Nom d'utilisateur" class="w-full rounded-xl bg-slate-950 border border-slate-700 p-3"><input name="email" type="email" required placeholder="Email" class="w-full rounded-xl bg-slate-950 border border-slate-700 p-3"><input name="telegram_user_id" placeholder="Telegram User ID (optionnel)" class="w-full rounded-xl bg-slate-950 border border-slate-700 p-3"><button class="w-full rounded-xl bg-cyan-500 hover:bg-cyan-400 text-slate-950 font-bold p-3">Démarrer mon essai 7 jours</button></form></div></div>"""
ADMIN_LOGIN_HTML = """{% extends_base %}<div class="min-h-[80vh] flex items-center justify-center"><div class="w-full max-w-md rounded-2xl border border-slate-800 bg-slate-900 p-6"><div class="text-xs tracking-[.3em] text-cyan-400">NOVA ADMIN</div><h1 class="mt-2 text-2xl font-bold">Connexion</h1><form method="post" class="mt-5 space-y-3"><input type="password" name="password" required placeholder="Mot de passe admin" class="w-full rounded-xl bg-slate-950 border border-slate-700 p-3"><button class="w-full rounded-xl bg-cyan-500 text-slate-950 font-bold p-3">Entrer</button></form></div></div>"""
PAYMENT_HTML = """{% extends_base %}<div class="flex items-center justify-between mb-6"><div><div class="text-xs tracking-[.3em] text-cyan-400">NOVA VIP</div><h1 class="text-3xl font-bold">Réabonnement</h1></div><a href="/" class="text-cyan-400">← Dashboard</a></div><div class="grid md:grid-cols-2 gap-4">{% for k,v in mobile_money.items() %}<div class="rounded-2xl border border-slate-800 bg-slate-900 p-5"><h2 class="font-bold">{{ k }} Money</h2><div class="mt-2 text-slate-300 break-all">{{ v }}</div></div>{% endfor %}{% for k,v in usdt.items() %}<div class="rounded-2xl border border-slate-800 bg-slate-900 p-5"><h2 class="font-bold">USDT {{ k }}</h2><div class="mt-2 text-slate-300 break-all">{{ v }}</div></div>{% endfor %}</div><form method="post" class="mt-6 rounded-2xl border border-slate-800 bg-slate-900 p-5 space-y-3"><select name="payment_method" required class="w-full rounded-xl bg-slate-950 border border-slate-700 p-3"><option value="">Méthode de paiement</option><option>ORANGE</option><option>MOOV</option><option>WAVE</option><option>USDT TRC20</option><option>USDT ERC20</option></select><input name="payment_reference" required placeholder="Référence / TXID" class="w-full rounded-xl bg-slate-950 border border-slate-700 p-3"><button class="rounded-xl bg-emerald-500 text-slate-950 font-bold px-5 py-3">Soumettre le paiement</button></form>"""
DASHBOARD_HTML = """{% extends_base %}<header class="flex flex-col md:flex-row md:items-center justify-between gap-4 mb-6"><div><div class="text-xs tracking-[.3em] text-cyan-400">NOVA TRADE AI</div><h1 class="text-3xl font-bold">Dashboard</h1><div class="text-slate-400">{{ user.username }} · <span id="status">{{ user.access_status }}</span></div></div><div class="flex gap-2"><a href="/payment" class="rounded-xl bg-cyan-500 text-slate-950 font-bold px-4 py-2">VIP</a><a href="/logout" class="rounded-xl border border-slate-700 px-4 py-2">Sortir</a></div></header><div id="trial" class="hidden mb-5 rounded-2xl border border-cyan-900 bg-cyan-950/30 p-4"><b>Essai TRIAL</b><div id="countdown" class="text-2xl font-mono mt-1"></div></div><div id="expired" class="hidden rounded-2xl border border-red-900 bg-red-950/30 p-8 text-center"><div class="text-5xl">🔒</div><h2 class="text-2xl font-bold mt-3">Accès expiré</h2><p class="text-slate-400 mt-2">Réabonnez-vous pour retrouver le radar et les signaux.</p><a href="/payment" class="inline-block mt-5 rounded-xl bg-cyan-500 text-slate-950 font-bold px-5 py-3">Se réabonner</a></div><div id="app" class="space-y-6"></div><script>
const initial={{ data|tojson }}; let expires={{ (user.subscription_expires_at|tojson) }}; const initialUser={{ user|tojson }};
function esc(x){return String(x??'').replace(/[&<>\"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;',"'":'&#039;'}[m]));}
function card(title,html){return `<section class="rounded-2xl border border-slate-800 bg-slate-900 p-5"><h2 class="font-bold text-lg mb-4">${title}</h2>${html}</section>`}
function render(d){const u=d.user||initialUser; document.getElementById('status').textContent=u.access_status; if(u.access_status==='EXPIRED'||u.access_status==='PENDING_VALIDATION'){document.getElementById('app').innerHTML='';document.getElementById('expired').classList.remove('hidden');document.getElementById('trial').classList.add('hidden');return;} document.getElementById('expired').classList.add('hidden'); if(u.access_status==='TRIAL')document.getElementById('trial').classList.remove('hidden'); else document.getElementById('trial').classList.add('hidden'); let r=Object.values(d.radar||{}).map(x=>`<div class="rounded-xl border border-slate-800 bg-slate-950 p-4"><div class="flex justify-between"><b>${esc(x.symbol)}</b><span>${esc(x.bias||'—')}</span></div><div class="text-xs text-slate-400 mt-1">BOS: ${esc(x.bos||'—')}</div><div class="mt-3 text-xs">Support: ${(x.supports||[]).map(z=>`<span class="inline-block bg-emerald-950 border border-emerald-900 rounded px-2 py-1 mr-1 mb-1">${z.zone_min??''} - ${z.zone_max??''}</span>`).join('')||'—'}</div><div class="mt-1 text-xs">Résistance: ${(x.resistances||[]).map(z=>`<span class="inline-block bg-red-950 border border-red-900 rounded px-2 py-1 mr-1 mb-1">${z.zone_min??''} - ${z.zone_max??''}</span>`).join('')||'—'}</div></div>`).join(''); let p=(d.pipeline||[]).map(x=>`<div class="rounded-xl bg-slate-950 border border-slate-800 p-4"><b>${esc(x.symbol)}</b> · ${esc(x.direction)}<div class="text-cyan-400 mt-1">${esc(x.status_label||x.status)}</div><div class="text-sm text-slate-400">Trigger: ${esc(x.trigger_price)}</div></div>`).join('')||'<div class="text-slate-500">Aucune opportunité.</div>'; let t=(d.active_trades||[]).map(x=>`<div class="rounded-xl bg-slate-950 border border-slate-800 p-4"><div class="flex justify-between"><b>${esc(x.symbol)} ${esc(x.direction)}</b><span>${esc(x.status)}</span></div><div class="grid grid-cols-2 md:grid-cols-5 gap-2 mt-3 text-sm"><span>Entry ${x.entry??'—'}</span><span>SL ${x.sl??'—'}</span><span>TP1 ${x.tp1_hit?'🔒 ':''}${x.tp1??'—'}</span><span>TP2 ${x.tp2_hit?'🔒 ':''}${x.tp2??'—'}</span><span>TP3 ${x.tp3_hit?'🔒 ':''}${x.tp3??'—'}</span></div><div class="text-cyan-400 mt-2">RR ${x.rr??'—'}</div></div>`).join('')||'<div class="text-slate-500">Aucun trade actif.</div>'; let h=(d.trade_history||[]).map(x=>`<tr class="border-t border-slate-800"><td class="py-2">${esc(x.symbol)}</td><td>${esc(x.direction)}</td><td>${esc(x.result||x.status)}</td><td>${x.profit??'—'}</td></tr>`).join(''); document.getElementById('app').innerHTML=card('📡 Radar M15',`<div class="grid md:grid-cols-2 xl:grid-cols-4 gap-3">${r}</div>`)+card('⏳ Pipeline M15 → M5 → M1',`<div class="grid md:grid-cols-3 gap-3">${p}</div>`)+card('⚡ Trades M1 actifs',`<div class="space-y-3">${t}</div>`)+card('📒 Journal',`<div class="overflow-auto"><table class="w-full text-sm"><thead><tr><th class="text-left py-2">Symbole</th><th class="text-left">Direction</th><th class="text-left">Résultat</th><th class="text-left">P&L</th></tr></thead><tbody>${h}</tbody></table></div>`)+(u.access_status==='VIP'?`<section class="rounded-2xl border border-cyan-900 bg-cyan-950/20 p-5"><h2 class="font-bold">Telegram VIP</h2><button onclick="invite()" class="mt-3 rounded-xl bg-cyan-500 text-slate-950 font-bold px-4 py-2">Rejoindre le Canal Telegram Privé</button><div id="invite" class="mt-2 break-all text-cyan-300"></div></section>`:'');}
async function refresh(){try{const r=await fetch('/api/dashboard');if(r.ok)render(await r.json());}catch(e){}}
async function invite(){const r=await fetch('/api/telegram-invite');const d=await r.json();document.getElementById('invite').innerHTML=d.invite_link?`<a class="underline" href="${esc(d.invite_link)}">${esc(d.invite_link)}</a>`:esc(d.error);}
function tick(){if(!expires)return;const ms=new Date(expires).getTime()-Date.now();if(ms<=0){document.getElementById('countdown').textContent='Expiré';return;}const d=Math.floor(ms/86400000),h=Math.floor(ms%86400000/3600000),m=Math.floor(ms%3600000/60000),s=Math.floor(ms%60000/1000);document.getElementById('countdown').textContent=`${d}j ${h}h ${m}m ${s}s`;}
render(initial);setInterval(refresh,5000);setInterval(tick,1000);tick();</script>"""
ADMIN_HTML = """{% extends_base %}<div class="flex items-center justify-between mb-6"><div><div class="text-xs tracking-[.3em] text-cyan-400">NOVA ADMIN</div><h1 class="text-3xl font-bold">CRM & contrôle</h1></div><a href="/nova-admin/logout" class="border border-slate-700 rounded-xl px-4 py-2">Sortir</a></div><div class="grid grid-cols-2 md:grid-cols-5 gap-3 mb-6">{% for k,v in metrics.items() %}<div class="rounded-2xl border border-slate-800 bg-slate-900 p-4"><div class="text-xs text-slate-500 uppercase">{{ k }}</div><div class="text-2xl font-bold">{{ v }}</div></div>{% endfor %}</div><div class="rounded-2xl border border-red-900 bg-red-950/20 p-5 mb-6"><div class="flex items-center justify-between"><div><b>🚨 PANIC</b><div class="text-sm text-slate-400">Mode test : n'agit pas sur main.py.</div></div><form method="post" action="/nova-admin/panic"><button class="rounded-xl bg-red-500 text-white font-bold px-4 py-2">PANIC</button></form></div></div><div class="rounded-2xl border border-slate-800 bg-slate-900 p-5 overflow-auto"><table class="w-full text-sm"><thead><tr><th class="text-left py-2">Username</th><th class="text-left">Statut</th><th class="text-left">IP</th><th class="text-left">Fin</th><th class="text-left">Actions</th></tr></thead><tbody>{% for u in users %}<tr class="border-t border-slate-800"><td class="py-3">{{ u.username }}</td><td>{{ u.access_status }}</td><td>{{ u.last_ip }}</td><td>{{ u.subscription_expires_at or 'Lifetime' }}</td><td><div class="flex flex-wrap gap-2">{% if u.access_status=='PENDING_VALIDATION' %}<form method="post" action="/nova-admin/validate/{{ u.user_id }}"><select name="duration" class="bg-slate-950 border border-slate-700 rounded p-2"><option value="1m">1 mois</option><option value="3m">3 mois</option></select><button class="bg-emerald-500 text-slate-950 rounded px-3 py-2 font-bold">Valider</button></form>{% endif %}<form method="post" action="/nova-admin/free-vip/{{ u.user_id }}"><select name="duration" class="bg-slate-950 border border-slate-700 rounded p-2"><option value="7d">7 jours</option><option value="1m">1 mois</option><option value="lifetime">Lifetime</option></select><button class="bg-cyan-500 text-slate-950 rounded px-3 py-2 font-bold">Free VIP</button></form></div></td></tr>{% endfor %}</tbody></table></div>"""

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

def create_user(name,email,tg):
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
        if not request.form.get('username') or not request.form.get('email'):flash('Nom et email requis.','error');return redirect(url_for('register'))
        u=create_user(request.form['username'].strip(),request.form['email'].strip(),request.form.get('telegram_user_id','').strip());session['uid']=u['user_id'];return redirect('/')
    return page('REGISTER_HTML', title='NOVA — Inscription')
@app.route('/logout')
def logout():session.pop('uid',None);return redirect(url_for('register'))
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
    us=users();m={'total':len(us),'vip':sum(x['access_status']=='VIP' for x in us),'trial':sum(x['access_status']=='TRIAL' for x in us),'expired':sum(x['access_status']=='EXPIRED' for x in us),'pending':sum(x['access_status']=='PENDING_VALIDATION' for x in us)}
    return page('ADMIN_HTML', title='NOVA — Admin Dashboard', users=us, metrics=m, panic=PANIC)
@app.route('/nova-admin/validate/<uid>',methods=['POST'])
@admin_required
def validate(uid):
    d={'7d':7,'1m':30,'3m':90}.get(request.form.get('duration'))
    if not d:flash('Durée invalide.','error');return redirect(url_for('admin_dashboard'))
    update(uid,access_status='VIP',subscription_expires_at=iso(now()+timedelta(days=d)),has_received_3day_warning=False);flash('VIP validé.','success');return redirect(url_for('admin_dashboard'))
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
@app.route('/health')
def health():return jsonify(status='ok',service='Site NOVA',mode='DEMO' if DEMO else 'LIVE')

init_db()
threading.Thread(target=worker,daemon=True,name='nova-subscriptions').start()
if __name__=='__main__':app.run(host='0.0.0.0',port=int(os.getenv('PORT','8080')),debug=False,threaded=True)
