import streamlit as st
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from datetime import date
import base64
import os
import time
from fpdf import FPDF
from streamlit_option_menu import option_menu
import random
import urllib.parse

# ==========================================
# 1. DATABASE & ENGINE
# ==========================================
raw_url = st.secrets["DATABASE_URL"]
if raw_url.startswith("postgres://"): raw_url = raw_url.replace("postgres://", "postgresql://", 1)
if ":6543" in raw_url: raw_url = raw_url.replace(":6543", ":5432")
if "?" not in raw_url: raw_url += "?sslmode=require"
elif "sslmode=" not in raw_url: raw_url += "&sslmode=require"

DB_URL = raw_url
engine = create_engine(DB_URL)

def get_db_connection(): return psycopg2.connect(DB_URL)

def init_db():
    conn = get_db_connection()
    try:
        with conn.cursor() as c:
            c.execute("CREATE TABLE IF NOT EXISTS public.workers (id SERIAL PRIMARY KEY, name TEXT UNIQUE, tjm REAL)")
            c.execute("CREATE TABLE IF NOT EXISTS public.clients (id SERIAL PRIMARY KEY, client_name TEXT UNIQUE, work_type TEXT, budget REAL, advance REAL, total_points REAL DEFAULT 0)")
            c.execute("CREATE TABLE IF NOT EXISTS public.labor_logs (id SERIAL PRIMARY KEY, date TEXT, client_name TEXT, worker_name TEXT, days REAL, cost REAL, phase TEXT DEFAULT 'Général')")
            c.execute("CREATE TABLE IF NOT EXISTS public.expenses (id SERIAL PRIMARY KEY, date TEXT, client_name TEXT, item TEXT, amount REAL, phase TEXT DEFAULT 'Général')")
            c.execute("CREATE TABLE IF NOT EXISTS public.progress (client_name TEXT PRIMARY KEY, phase1 REAL, phase2 REAL, phase3 REAL, phase4 REAL)")
            c.execute("CREATE TABLE IF NOT EXISTS public.site_photos (id SERIAL PRIMARY KEY, upload_date TEXT, client_name TEXT, phase TEXT, photo_data TEXT, notes TEXT)")
            c.execute("CREATE TABLE IF NOT EXISTS public.inventory (id SERIAL PRIMARY KEY, item_name TEXT UNIQUE, category TEXT, quantity REAL, unit TEXT)")
            c.execute("CREATE TABLE IF NOT EXISTS public.inventory_logs (id SERIAL PRIMARY KEY, date TEXT, item_name TEXT, change_amount REAL, site_allocated TEXT, notes TEXT)")
            c.execute("CREATE TABLE IF NOT EXISTS public.system_users (username TEXT PRIMARY KEY, password TEXT, role TEXT, reference TEXT)")
            c.execute("INSERT INTO public.system_users (username, password, role, reference) VALUES ('admin', 'Admin2026!', 'Admin', 'Master') ON CONFLICT DO NOTHING")
        conn.commit()
    finally: conn.close()

init_db()

# ==========================================
# 2. UI & SECURITY
# ==========================================
st.set_page_config(page_title="Newlightemara OS", page_icon="💡", layout="wide")

st.markdown("""
    <style>
        .stButton>button { border-radius: 8px; font-weight: bold; }
        .stMetric { background-color: #f8f9fb; padding: 15px; border-radius: 8px; border: 1px solid #e2e8f0; }
        button[data-testid="stNumberInputStepUp"], button[data-testid="stNumberInputStepDown"] { display: none !important; }
        input[type="number"] { -moz-appearance: textfield; }
        input[type="number"]::-webkit-inner-spin-button, input[type="number"]::-webkit-outer-spin-button { -webkit-appearance: none; margin: 0; }
    </style>
""", unsafe_allow_html=True)

LOGO_FILE = "logo.png"

def fetch_data(table):
    try: return pd.read_sql(f'SELECT * FROM public.{table}', engine)
    except: return pd.DataFrame()

def rain_money():
    uid = str(int(time.time() * 1000))
    st.markdown(f"""<style>@keyframes f-{uid} {{0% {{top:-10%; opacity:1;}} 100% {{top:110%; opacity:0;}}}} .b-{uid} {{position:fixed; z-index:9999; font-size:3rem; animation: f-{uid} 3s linear forwards; pointer-events:none;}}</style>""", unsafe_allow_html=True)
    for _ in range(30): st.markdown(f'<div class="b-{uid}" style="left:{random.randint(0,100)}%; animation-delay:{random.uniform(0,1.5)}s;">{random.choice(["💸","💵","💰"])}</div>', unsafe_allow_html=True)

if "auth" not in st.session_state: st.session_state.update({"auth": False, "role": None, "user": None, "ref": None})

if not st.session_state["auth"]:
    c1, c2, c3 = st.columns([1, 1.5, 1])
    with c2:
        if os.path.exists(LOGO_FILE): st.image(LOGO_FILE)
        else: st.title("💡 Newlightemara")
        with st.form("login"):
            u, p = st.text_input("Username"), st.text_input("Password", type="password")
            if st.form_submit_button("Login"):
                users = fetch_data('system_users')
                m = users[(users['username']==u) & (users['password']==p)]
                if not m.empty:
                    st.session_state.update({"auth": True, "role": m.iloc[0]['role'], "user": u, "ref": m.iloc[0]['reference']})
                    st.rerun()
                else: st.error("Access Denied")
    st.stop()

# ==========================================
# 3. SIDEBAR & MENU
# ==========================================
ROLE, REF = st.session_state["role"], st.session_state["ref"]

with st.sidebar:
    if os.path.exists(LOGO_FILE): st.image(LOGO_FILE)
    else: st.markdown("### Newlightemara")
    st.caption(f"Connected: {st.session_state['user']} ({ROLE})")
    
    if ROLE == "Admin":
        m_opts = ["Dashboard", "Smart Estimator", "Client Portfolios", "Timesheets", "Payroll", "Efficiency Matrix", "Procurement", "Milestones", "Site Photos", "Warehouse", "Invoicing", "Dispatch", "Settings"]
        m_icos = ["bar-chart", "calculator", "building", "clock", "wallet", "speedometer", "cart", "check-circle", "camera", "box", "receipt", "send", "shield"]
    elif ROLE == "Technician":
        m_opts = ["Timesheets", "Site Photos"]
        m_icos = ["clock", "camera"]
    else: # Client
        m_opts = ["VIP Portal"]
        m_icos = ["star"]
        
    menu = option_menu(None, m_opts, icons=m_icos, menu_icon="cast", default_index=0)
    if st.button("Logout"):
        st.session_state.update({"auth": False}); st.rerun()

# ==========================================
# 4. ROUTING LOGIC (FULL MODULES)
# ==========================================

# --- DASHBOARD ---
if menu == "Dashboard":
    st.title("Executive Command Center")
    cl, lb, ex = fetch_data('clients'), fetch_data('labor_logs'), fetch_data('expenses')
    if not cl.empty:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Contracts", f"{pd.to_numeric(cl['budget']).sum():,.0f} DH")
        c2.metric("Liquidity", f"{pd.to_numeric(cl['advance']).sum():,.0f} DH")
        c3.metric("Labor Cost", f"{pd.to_numeric(lb['cost']).sum() if not lb.empty else 0:,.0f} DH")
        c4.metric("Materials", f"{pd.to_numeric(ex['amount']).sum() if not ex.empty else 0:,.0f} DH")
        
        rep = []
        for _, r in cl.iterrows():
            lc = lb[lb['client_name']==r['client_name']]['cost'].sum() if not lb.empty else 0
            mc = ex[ex['client_name']==r['client_name']]['amount'].sum() if not ex.empty else 0
            prof = float(r['budget']) - (lc + mc)
            rep.append({"Site": r['client_name'], "Budget": f"{r['budget']:,.0f}", "Profit": f"{prof:,.0f}", "Margin": f"{(prof/r['budget']*100) if r['budget']>0 else 0:.1f}%"})
        st.table(pd.DataFrame(rep))

# --- SMART ESTIMATOR ---
elif menu == "Smart Estimator":
    st.title("🧠 Smart Estimator")
    cl, lb, ex = fetch_data('clients'), fetch_data('labor_logs'), fetch_data('expenses')
    with st.form("est"):
        c1, c2 = st.columns(2)
        nm = c1.text_input("Project Name")
        pts = c2.number_input("Est. Points", min_value=1, value=None, placeholder="Enter points")
        marg = st.slider("Target Margin %", 10, 50, 30)
        if st.form_submit_button("Generate Quote"):
            if nm and pts:
                cost_pt = (lb['cost'].sum() + ex['amount'].sum()) / cl['total_points'].sum() if not cl.empty else 150
                base = pts * cost_pt
                quote = base / (1 - (marg/100))
                st.success(f"Recommended Quote: {quote:,.2f} DH")
                pdf = FPDF(); pdf.add_page(); pdf.set_font("Arial", 'B', 16)
                pdf.cell(200, 10, f"DEVIS: {nm}", ln=True, align='C')
                pdf.set_font("Arial", '', 12); pdf.cell(200, 10, f"Total: {quote:,.2f} DH", ln=True)
                pdf.output("q.pdf")
                with open("q.pdf", "rb") as f: st.download_button("Download Devis PDF", f.read(), f"{nm}.pdf")

# --- PORTFOLIOS ---
elif menu == "Client Portfolios":
    st.title("Portfolios & Payments")
    with st.expander("➕ New Project"):
        with st.form("add_cl"):
            c1, c2, c3 = st.columns(3)
            nm = c1.text_input("Client Name")
            pts = c2.number_input("Points", value=None, placeholder="0")
            bg = c3.number_input("Budget", value=None, placeholder="0.0")
            if st.form_submit_button("Save"):
                conn = get_db_connection()
                with conn.cursor() as c: c.execute("INSERT INTO public.clients (client_name, budget, total_points, advance) VALUES (%s,%s,%s,0)", (nm, bg or 0, pts or 0))
                conn.commit(); st.success("Created.")
    
    with st.expander("💰 Log Payment"):
        cls = fetch_data('clients')
        with st.form("pay"):
            sel = st.selectbox("Client", cls['client_name'])
            amt = st.number_input("Amount (DH)", value=None, placeholder="0.0")
            if st.form_submit_button("Collect"):
                conn = get_db_connection()
                with conn.cursor() as c: c.execute("UPDATE public.clients SET advance = advance + %s WHERE client_name=%s", (amt, sel))
                conn.commit(); rain_money(); st.success("Verified.")

# --- TIMESHEETS ---
elif menu == "Timesheets":
    st.title("Daily Timesheets")
    wk, cl = fetch_data('workers'), fetch_data('clients')
    with st.form("ts"):
        d = st.date_input("Date")
        s = st.selectbox("Site", cl['client_name'])
        ph = st.selectbox("Phase", ["Incorporation", "Tirage", "Appareillage", "Tableau"])
        target = [REF] if ROLE == "Technician" else wk['name'].tolist()
        sel = st.multiselect("Technicians", target, default=target if ROLE=="Technician" else None)
        hours = {w: st.number_input(f"{w} (Days)", value=None, placeholder="0.0") for w in sel}
        if st.form_submit_button("Submit Logs"):
            conn = get_db_connection()
            with conn.cursor() as c:
                for w, h in hours.items():
                    if h:
                        tjm = wk[wk['name']==w]['tjm'].values[0]
                        c.execute("INSERT INTO public.labor_logs (date, client_name, worker_name, days, cost, phase) VALUES (%s,%s,%s,%s,%s,%s)", (d, s, w, h, h*tjm, ph))
            conn.commit(); st.success("Saved.")

# --- PAYROLL ---
elif menu == "Payroll":
    st.title("Payroll obligation")
    lb = fetch_data('labor_logs')
    if not lb.empty:
        c1, c2 = st.columns(2)
        start = c1.date_input("From", date.today() - pd.Timedelta(days=7))
        end = c2.date_input("To", date.today())
        lb['date'] = pd.to_datetime(lb['date']).dt.date
        f = lb[(lb['date']>=start) & (lb['date']<=end)]
        pay = f.groupby('worker_name').agg(Days=('days','sum'), Total=('cost','sum')).reset_index()
        st.dataframe(pay, use_container_width=True, hide_index=True)

# --- EFFICIENCY MATRIX ---
elif menu == "Efficiency Matrix":
    st.title("Performance Analytics")
    lb = fetch_data('labor_logs')
    if not lb.empty:
        st.subheader("Days Spent per Phase vs Site")
        st.dataframe(pd.pivot_table(lb, values='days', index='client_name', columns='phase', aggfunc='sum', fill_value=0))
        st.subheader("Efficiency per Worker")
        st.dataframe(pd.pivot_table(lb, values='days', index='worker_name', columns='phase', aggfunc='sum', fill_value=0))

# --- PROCUREMENT ---
elif menu == "Procurement":
    st.title("Material Expenses")
    cl = fetch_data('clients')
    with st.form("exp"):
        c1, c2 = st.columns(2)
        s = c1.selectbox("Site", cl['client_name'])
        ph = c2.selectbox("Class", ["Incorporation", "Tirage", "Tableau"])
        it = st.text_input("Item Description")
        am = st.number_input("Amount", value=None, placeholder="0.0")
        if st.form_submit_button("Log Expense"):
            conn = get_db_connection()
            with conn.cursor() as c: c.execute("INSERT INTO public.expenses (date, client_name, item, amount, phase) VALUES (%s,%s,%s,%s,%s)", (date.today(), s, it, am, ph))
            conn.commit(); st.success("Logged.")

# --- MILESTONES ---
elif menu == "Milestones":
    st.title("Technical Progress")
    cl = fetch_data('clients')
    sel = st.selectbox("Select Site", cl['client_name'])
    curr = pd.read_sql(f"SELECT * FROM public.progress WHERE client_name='{sel}'", engine)
    v1, v2, v3, v4 = (curr.iloc[0][['phase1','phase2','phase3','phase4']] if not curr.empty else [0,0,0,0])
    with st.form("prog"):
        p1 = st.slider("Incorporation %", 0, 100, int(v1))
        p2 = st.slider("Tirage %", 0, 100, int(v2))
        p3 = st.slider("Appareillage %", 0, 100, int(v3))
        p4 = st.slider("Tableau %", 0, 100, int(v4))
        if st.form_submit_button("Update"):
            conn = get_db_connection()
            with conn.cursor() as c: c.execute("INSERT INTO public.progress VALUES (%s,%s,%s,%s,%s) ON CONFLICT (client_name) DO UPDATE SET phase1=EXCLUDED.phase1, phase2=EXCLUDED.phase2, phase3=EXCLUDED.phase3, phase4=EXCLUDED.phase4", (sel, p1, p2, p3, p4))
            conn.commit(); st.success("Updated.")

# --- SITE PHOTOS ---
elif menu == "Site Photos":
    st.title("As-Built Archives")
    cl = fetch_data('clients')
    with st.form("img"):
        s = st.selectbox("Site", cl['client_name'])
        f = st.file_uploader("Upload Image")
        if st.form_submit_button("Save") and f:
            b64 = base64.b64encode(f.getvalue()).decode()
            conn = get_db_connection()
            with conn.cursor() as c: c.execute("INSERT INTO public.site_photos (upload_date, client_name, photo_data) VALUES (%s,%s,%s)", (date.today(), s, b64))
            conn.commit(); st.success("Saved.")
    ph = fetch_data('site_photos')
    if not ph.empty:
        for i, r in ph[ph['client_name']==s].iterrows(): st.image(base64.b64decode(r['photo_data']))

# --- WAREHOUSE ---
elif menu == "Warehouse":
    st.title("Stock Control")
    with st.form("inv"):
        it = st.text_input("Item Name")
        qty = st.number_input("Qty", value=None, placeholder="0")
        if st.form_submit_button("Check-In"):
            conn = get_db_connection()
            with conn.cursor() as c: c.execute("INSERT INTO public.inventory (item_name, quantity) VALUES (%s,%s) ON CONFLICT (item_name) DO UPDATE SET quantity=inventory.quantity+EXCLUDED.quantity", (it, qty))
            conn.commit(); st.success("Stocked.")
    st.table(fetch_data('inventory'))

# --- INVOICING ---
elif menu == "Invoicing":
    st.title("Billing Engine")
    cl = fetch_data('clients')
    sel = st.selectbox("Client", cl['client_name'])
    d = cl[cl['client_name']==sel].iloc[0]
    st.info(f"Outstanding: {d['budget']-d['advance']} DH")
    if st.button("Generate Invoice"):
        pdf = FPDF(); pdf.add_page(); pdf.set_font("Arial", 'B', 16)
        pdf.cell(200, 10, f"FACTURE: {sel}", align='C')
        pdf.output("i.pdf")
        with open("i.pdf", "rb") as f: st.download_button("Download", f.read(), f"{sel}.pdf")

# --- DISPATCH ---
elif menu == "Dispatch":
    st.title("Team Dispatch")
    wk, cl = fetch_data('workers'), fetch_data('clients')
    with st.form("ds"):
        t = st.selectbox("Worker", wk['name'])
        s = st.selectbox("Site", cl['client_name'])
        if st.form_submit_button("Create WhatsApp Link"):
            msg = urllib.parse.quote(f"Newlightemara: {t}, demain tu es à {s}.")
            st.markdown(f"[Send WhatsApp](https://wa.me/?text={msg})")

# --- SETTINGS ---
elif menu == "Settings":
    st.title("System Administration")
    with st.form("usr"):
        u, p, r = st.text_input("Username"), st.text_input("Pass"), st.selectbox("Role", ["Admin", "Technician", "Client"])
        if st.form_submit_button("Create"):
            conn = get_db_connection()
            with conn.cursor() as c: c.execute("INSERT INTO public.system_users (username,password,role,reference) VALUES (%s,%s,%s,'Master')", (u,p,r))
            conn.commit(); st.success("User Added.")

# --- VIP PORTAL ---
elif menu == "VIP Portal":
    st.title(f"Project Portal: {REF}")
    cl = fetch_data('clients')
    my = cl[cl['client_name']==REF].iloc[0]
    st.metric("Balance Due", f"{my['budget']-my['advance']:,.0f} DH")
    pr = pd.read_sql(f"SELECT * FROM public.progress WHERE client_name='{REF}'", engine)
    if not pr.empty: st.progress(int(pr.iloc[0]['phase1'])/100, "Progress")
