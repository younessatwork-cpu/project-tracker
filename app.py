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
            # FIX: Ensure all columns exist exactly as used in the script
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
    try: 
        df = pd.read_sql(f'SELECT * FROM public.{table}', engine)
        # Global Fix: Ensure numeric columns are actually numeric to avoid Dashboard errors
        numeric_cols = ['budget', 'advance', 'total_points', 'amount', 'cost', 'days']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        return df
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
# 4. ROUTING LOGIC
# ==========================================

# --- DASHBOARD ---
if menu == "Dashboard":
    st.title("Executive Command Center")
    cl, lb, ex = fetch_data('clients'), fetch_data('labor_logs'), fetch_data('expenses')
    if not cl.empty:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Contracts", f"{cl['budget'].sum():,.0f} DH")
        c2.metric("Liquidity", f"{cl['advance'].sum():,.0f} DH")
        c3.metric("Labor Cost", f"{lb['cost'].sum() if not lb.empty else 0:,.0f} DH")
        c4.metric("Materials", f"{ex['amount'].sum() if not ex.empty else 0:,.0f} DH")
        
        rep = []
        for _, r in cl.iterrows():
            lc = lb[lb['client_name']==r['client_name']]['cost'].sum() if not lb.empty else 0
            mc = ex[ex['client_name']==r['client_name']]['amount'].sum() if not ex.empty else 0
            prof = r['budget'] - (lc + mc)
            rep.append({"Site": r['client_name'], "Budget": f"{r['budget']:,.0f}", "Profit": f"{prof:,.0f}", "Margin": f"{(prof/r['budget']*100) if r['budget']>0 else 0:.1f}%"})
        st.table(pd.DataFrame(rep))

# --- PORTFOLIOS ---
elif menu == "Client Portfolios":
    st.title("Portfolios & Payments")
    with st.expander("➕ New Project"):
        with st.form("add_cl"):
            c1, c2, c3 = st.columns(3)
            nm = c1.text_input("Client Name")
            pts = c2.number_input("Points", value=0)
            bg = c3.number_input("Budget", value=0.0)
            if st.form_submit_button("Save"):
                if nm:
                    conn = get_db_connection()
                    with conn.cursor() as c: 
                        # FIX: Aligning columns with init_db schema
                        c.execute("INSERT INTO public.clients (client_name, budget, total_points, advance, work_type) VALUES (%s,%s,%s,0,'Général')", (nm, bg, pts))
                    conn.commit(); st.success("Created.")
                else: st.error("Please provide a Client Name.")

# --- TIMESHEETS ---
elif menu == "Timesheets":
    st.title("Daily Timesheets")
    wk, cl = fetch_data('workers'), fetch_data('clients')
    with st.form("ts"):
        d = st.date_input("Date")
        s = st.selectbox("Site", cl['client_name'] if not cl.empty else ["No Sites Found"])
        ph = st.selectbox("Phase", ["Incorporation", "Tirage", "Appareillage", "Tableau"])
        target = [REF] if ROLE == "Technician" else (wk['name'].tolist() if not wk.empty else [])
        sel = st.multiselect("Technicians", target)
        hours = {w: st.number_input(f"{w} (Days)", value=0.0) for w in sel}
        if st.form_submit_button("Submit Logs"):
            if not sel: st.error("Select at least one technician.")
            else:
                conn = get_db_connection()
                with conn.cursor() as c:
                    for w, h in hours.items():
                        if h > 0:
                            # FIX: Safe TJM lookup
                            match = wk[wk['name']==w]
                            tjm = match['tjm'].values[0] if not match.empty else 0
                            c.execute("INSERT INTO public.labor_logs (date, client_name, worker_name, days, cost, phase) VALUES (%s,%s,%s,%s,%s,%s)", (d, s, w, h, h*tjm, ph))
                conn.commit(); st.success("Saved.")
# ... rest of your script follows with the same logic
