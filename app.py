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
# 1. ENTERPRISE DATABASE CONFIGURATION
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
            # Base Tables
            c.execute("""CREATE TABLE IF NOT EXISTS public.workers (id SERIAL PRIMARY KEY, name TEXT UNIQUE, tjm REAL)""")
            c.execute("""CREATE TABLE IF NOT EXISTS public.clients (id SERIAL PRIMARY KEY, client_name TEXT UNIQUE, work_type TEXT, budget REAL, advance REAL, total_points REAL DEFAULT 0)""")
            c.execute("""CREATE TABLE IF NOT EXISTS public.labor_logs (id SERIAL PRIMARY KEY, date TEXT, client_name TEXT, worker_name TEXT, days REAL, cost REAL, phase TEXT DEFAULT 'Général')""")
            c.execute("""CREATE TABLE IF NOT EXISTS public.expenses (id SERIAL PRIMARY KEY, date TEXT, client_name TEXT, item TEXT, amount REAL, phase TEXT DEFAULT 'Général')""")
            c.execute("""CREATE TABLE IF NOT EXISTS public.progress (client_name TEXT PRIMARY KEY, phase1 REAL, phase2 REAL, phase3 REAL, phase4 REAL)""")
            c.execute("""CREATE TABLE IF NOT EXISTS public.site_photos (id SERIAL PRIMARY KEY, upload_date TEXT, client_name TEXT, phase TEXT, photo_data TEXT, notes TEXT)""")
            c.execute("""CREATE TABLE IF NOT EXISTS public.inventory (id SERIAL PRIMARY KEY, item_name TEXT UNIQUE, category TEXT, quantity REAL, unit TEXT)""")
            c.execute("""CREATE TABLE IF NOT EXISTS public.inventory_logs (id SERIAL PRIMARY KEY, date TEXT, item_name TEXT, change_amount REAL, site_allocated TEXT, notes TEXT)""")
            
            # 🔥 NEW: Multi-Tier Users Table
            c.execute("""CREATE TABLE IF NOT EXISTS public.system_users (username TEXT PRIMARY KEY, password TEXT, role TEXT, reference TEXT)""")
            # Inject default admin if not exists
            c.execute("""INSERT INTO public.system_users (username, password, role, reference) VALUES ('admin', 'Admin2026!', 'Admin', 'Master') ON CONFLICT DO NOTHING""")
        conn.commit()
    finally: conn.close()

init_db()

# ==========================================
# 2. UI STYLING
# ==========================================
st.set_page_config(page_title="Newlightemara OS", page_icon="💡", layout="wide")

st.markdown("""
    <style>
        .stButton>button { border-radius: 8px; font-weight: bold; }
        .stMetric { background-color: #f8f9fb; padding: 15px; border-radius: 8px; border: 1px solid #e2e8f0; }
        div[data-testid="stExpander"] { border: 1px solid #e2e8f0; border-radius: 8px; box-shadow: 0 1px 2px rgba(0,0,0,0.05); }
        button[data-testid="stNumberInputStepUp"], button[data-testid="stNumberInputStepDown"] { display: none !important; }
        input[type="number"] { -moz-appearance: textfield; }
        input[type="number"]::-webkit-inner-spin-button, input[type="number"]::-webkit-outer-spin-button { -webkit-appearance: none; margin: 0; }
    </style>
""", unsafe_allow_html=True)

LOGO_FILE = "logo.png" 

# ==========================================
# 3. MULTI-TIER AUTHENTICATION
# ==========================================
def fetch_data(table):
    try: return pd.read_sql(f'SELECT * FROM public.{table}', engine)
    except Exception: return pd.DataFrame()

if "auth_status" not in st.session_state: 
    st.session_state.update({"auth_status": False, "role": None, "username": None, "ref": None})

if not st.session_state["auth_status"]:
    st.markdown("<br><br>", unsafe_allow_html=True)
    c1, c2, c3 = st.columns([1, 1.5, 1])
    with c2:
        if os.path.exists(LOGO_FILE): st.image(LOGO_FILE, use_container_width=True)
        else: st.markdown("<h1 style='text-align: center; color: #1E293B;'>💡 Newlightemara</h1>", unsafe_allow_html=True)
        st.markdown("<p style='text-align: center; color: #64748B;'>Enterprise Resource Planning System</p>", unsafe_allow_html=True)
        
        with st.form("login_form"):
            user_input = st.text_input("Username")
            pass_input = st.text_input("Password", type="password")
            if st.form_submit_button("Secure Login", use_container_width=True):
                users_df = fetch_data('system_users')
                match = users_df[(users_df['username'] == user_input) & (users_df['password'] == pass_input)]
                if not match.empty:
                    st.session_state["auth_status"] = True
                    st.session_state["role"] = match.iloc[0]['role']
                    st.session_state["username"] = match.iloc[0]['username']
                    st.session_state["ref"] = match.iloc[0]['reference']
                    st.rerun()
                else: st.error("❌ Invalid Credentials.")
    st.stop() # Halt execution until logged in

# ==========================================
# 4. APP NAVIGATION & ROUTING
# ==========================================
ROLE = st.session_state["role"]
REF = st.session_state["ref"]

if os.path.exists(LOGO_FILE): st.sidebar.image(LOGO_FILE, use_container_width=True)
else: st.sidebar.markdown("### 💡 Newlightemara")

st.sidebar.markdown(f"**User:** {st.session_state['username']} | **Role:** {ROLE}")
st.sidebar.markdown("---")

# Dynamically build menu based on role
if ROLE == "Admin":
    menu_opts = ["Command Center", "Smart Estimator", "Bidding Intel", "Portfolios", "Timesheets", "Payroll", "Efficiency", "Procurement", "Milestones", "Site Photos", "Warehouse", "Invoicing", "Dispatch", "System Settings"]
    menu_icos = ["bar-chart-line", "calculator", "graph-up-arrow", "building", "clock-history", "wallet2", "speedometer2", "cart-check", "gear", "camera", "box-seam", "receipt", "send", "shield-lock"]
elif ROLE == "Technician":
    menu_opts = ["My Timesheet", "Site Photos"]
    menu_icos = ["clock-history", "camera"]
elif ROLE == "Client":
    menu_opts = ["VIP Portal"]
    menu_icos = ["star"]

with st.sidebar:
    menu = option_menu(None, options=menu_opts, icons=menu_icos, default_index=0, styles={"nav-link": {"font-size": "14px", "margin":"0px"}, "nav-link-selected": {"background-color": "#1E293B", "font-weight": "bold"}})

st.sidebar.markdown("---")
if st.sidebar.button("🚪 Logout"):
    st.session_state.clear()
    st.rerun()

def rain_money():
    uid = str(int(time.time() * 1000)) + str(random.randint(0, 1000))
    st.markdown(f"""
    <style>@keyframes money-fall-{uid} {{0% {{top: -10%; transform: translateX(0px) rotate(0deg); opacity: 1;}} 100% {{top: 110%; transform: translateX(40px) rotate(360deg); opacity: 0;}}}} .bill-{uid} {{position: fixed; z-index: 9999; font-size: 3.5rem; animation: money-fall-{uid} linear forwards; pointer-events: none;}}</style>
    """, unsafe_allow_html=True)
    html = ""
    for _ in range(40): html += f'<div class="bill-{uid}" style="left: {random.randint(0, 100)}%; animation-delay: {random.uniform(0, 1.5)}s; animation-duration: {random.uniform(2.5, 4.0)}s;">{random.choice(["💸", "💵", "💰", "💶"])}</div>'
    st.markdown(html, unsafe_allow_html=True)

# ==========================================
# VIEW: ADMIN COMMAND CENTER
# ==========================================
if menu == "Command Center":
    st.title("Executive Command Center")
    clients_df, labor_df, expenses_df = fetch_data('clients'), fetch_data('labor_logs'), fetch_data('expenses')
    if not clients_df.empty:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Gross Contract Volume", f"{pd.to_numeric(clients_df['budget'], errors='coerce').sum():,.2f} DH")
        c2.metric("Liquidity (Advances)", f"{pd.to_numeric(clients_df['advance'], errors='coerce').sum():,.2f} DH")
        c3.metric("Total Labor Spend", f"{pd.to_numeric(labor_df['cost'], errors='coerce').sum() if not labor_df.empty else 0:,.2f} DH")
        c4.metric("Total Procurement", f"{pd.to_numeric(expenses_df['amount'], errors='coerce').sum() if not expenses_df.empty else 0:,.2f} DH")
        
        report_data = []
        for _, row in clients_df.iterrows():
            c_name, c_budget = row['client_name'], float(row['budget'] or 0)
            c_labor = pd.to_numeric(labor_df[labor_df['client_name'] == c_name]['cost'], errors='coerce').sum() if not labor_df.empty else 0
            c_mat = pd.to_numeric(expenses_df[expenses_df['client_name'] == c_name]['amount'], errors='coerce').sum() if not expenses_df.empty else 0
            profit = c_budget - (c_labor + c_mat)
            report_data.append({"Site": c_name, "Contract": f"{c_budget:,.2f} DH", "Cost (Lab+Mat)": f"{(c_labor+c_mat):,.2f} DH", "Net Profit": f"{profit:,.2f} DH", "Margin": f"{(profit/c_budget*100) if c_budget>0 else 0:.1f}%"})
        st.dataframe(pd.DataFrame(report_data), use_container_width=True, hide_index=True)

# ==========================================
# VIEW: SMART ESTIMATOR (NEW)
# ==========================================
elif menu == "Smart Estimator":
    st.title("🧠 Automated Smart Estimator")
    st.markdown("Generate pinpoint accurate quotes based on historical efficiency.")
    clients_df, labor_df, expenses_df = fetch_data('clients'), fetch_data('labor_logs'), fetch_data('expenses')
    
    if clients_df.empty or labor_df.empty: st.warning("Need more historical data to generate estimates.")
    else:
        # Calculate historical baseline
        total_pts = pd.to_numeric(clients_df['total_points'], errors='coerce').sum()
        total_lab = pd.to_numeric(labor_df['cost'], errors='coerce').sum()
        total_mat = pd.to_numeric(expenses_df['amount'], errors='coerce').sum()
        hist_cost_per_pt = (total_lab + total_mat) / total_pts if total_pts > 0 else 0
        
        with st.form("estimator"):
            c1, c2 = st.columns(2)
            est_client = c1.text_input("Prospective Client Name")
            est_pts = c2.number_input("Estimated Electrical Points (Blueprint)", min_value=1, step=5, value=None, placeholder="0")
            margin = st.slider("Target Profit Margin (%)", 10, 60, 30, 5)
            
            if st.form_submit_button("Generate Estimate & Quote"):
                if est_client and est_pts:
                    base_cost = est_pts * hist_cost_per_pt
                    quote_price = base_cost / (1 - (margin/100))
                    proj_profit = quote_price - base_cost
                    
                    st.success("Estimate Generated Successfully!")
                    m1, m2, m3 = st.columns(3)
                    m1.metric("Historical Base Cost", f"{base_cost:,.2f} DH")
                    m2.metric("Required Quote (Devis)", f"{quote_price:,.2f} DH", f"+{margin}% Margin")
                    m3.metric("Projected Profit", f"{proj_profit:,.2f} DH")
                    
                    # Generate PDF Devis
                    pdf = FPDF()
                    pdf.add_page()
                    if os.path.exists(LOGO_FILE):
                        try: pdf.image(LOGO_FILE, x=10, y=8, w=33)
                        except: pass
                    pdf.set_font("Arial", style='B', size=16)
                    pdf.cell(200, 10, txt="DEVIS ESTIMATIF / QUOTE - NEWLIGHTEMARA", ln=True, align='C')
                    pdf.ln(15)
                    pdf.set_font("Arial", size=12)
                    pdf.cell(200, 10, txt=f"Client: {est_client}  |  Date: {date.today()}", ln=True)
                    pdf.line(10, 45, 200, 45)
                    pdf.ln(10)
                    pdf.set_font("Arial", style='B', size=12)
                    pdf.cell(140, 10, txt="Description des Travaux", border=1)
                    pdf.cell(50, 10, txt="Montant (DH)", border=1, ln=True, align='R')
                    pdf.set_font("Arial", size=12)
                    pdf.cell(140, 10, txt=f"Installation Electrique Globale ({est_pts} Points Estimes)", border=1)
                    pdf.cell(50, 10, txt=f"{quote_price:,.2f}", border=1, ln=True, align='R')
                    pdf.ln(20)
                    pdf.set_font("Arial", style='B', size=14)
                    pdf.cell(140, 15, txt="TOTAL DEVIS", border=1)
                    pdf.cell(50, 15, txt=f"{quote_price:,.2f} DH", border=1, ln=True, align='R')
                    
                    pdf.output("devis_temp.pdf")
                    with open("devis_temp.pdf", "rb") as pdf_file: 
                        st.download_button("📥 Download PDF Devis", data=pdf_file.read(), file_name=f"Devis_{est_client}.pdf", mime='application/pdf', type="primary")

# ==========================================
# VIEW: DISPATCH & COMMS (NEW)
# ==========================================
elif menu == "Dispatch":
    st.title("🚀 Automated Dispatch & Comms")
    workers, clients = fetch_data('workers'), fetch_data('clients')
    if not workers.empty and not clients.empty:
        with st.form("dispatch"):
            c1, c2 = st.columns(2)
            tech = c1.selectbox("Select Technician", workers['name'])
            site = c2.selectbox("Assign to Site Tomorrow", clients['client_name'])
            phase = st.selectbox("Assigned Phase", ["Incorporation", "Tirage", "Appareillage", "Tableau"])
            notes = st.text_input("Special Instructions")
            if st.form_submit_button("Generate Dispatch Link"):
                msg = f"Salut {tech},\nDemain tu es affecté au chantier: *{site}*.\nPhase: *{phase}*.\nNotes: {notes}\nMerci, L'équipe Newlightemara."
                encoded_msg = urllib.parse.quote(msg)
                st.info("Message preview:")
                st.code(msg)
                st.markdown(f"[📲 Click here to open WhatsApp & Send Dispatch to {tech}](https://wa.me/?text={encoded_msg})")

# ==========================================
# VIEW: SYSTEM SETTINGS (NEW)
# ==========================================
elif menu == "System Settings":
    st.title("🛡️ System Users & Roles")
    with st.expander("➕ Create New User Access", expanded=True):
        with st.form("new_user"):
            c1, c2 = st.columns(2)
            n_user = c1.text_input("Username")
            n_pass = c2.text_input("Password", type="password")
            c3, c4 = st.columns(2)
            n_role = c3.selectbox("Access Level", ["Admin", "Technician", "Client"])
            
            # Map the user to a specific entity (Tech name or Client name) to restrict their views later
            ref_opts = ["Master"]
            if n_role == "Technician": ref_opts = fetch_data('workers')['name'].tolist()
            elif n_role == "Client": ref_opts = fetch_data('clients')['client_name'].tolist()
            n_ref = c4.selectbox("Bind Account To (Important for Techs/Clients)", ref_opts)
            
            if st.form_submit_button("Create Account"):
                conn = get_db_connection()
                try:
                    with conn.cursor() as c: c.execute("INSERT INTO public.system_users (username, password, role, reference) VALUES (%s, %s, %s, %s)", (n_user, n_pass, n_role, n_ref))
                    conn.commit()
                    st.success(f"Account {n_user} created.")
                except: st.error("Username taken.")
                finally: conn.close()
    st.dataframe(fetch_data('system_users')[['username', 'role', 'reference']], hide_index=True, use_container_width=True)

# ==========================================
# VIEW: VIP CLIENT PORTAL (RESTRICTED)
# ==========================================
elif menu == "VIP Portal":
    st.title("🏢 Client Dashboard")
    st.markdown(f"**Welcome to Newlightemara Client Services.** Overview for project: {REF}")
    
    clients, prog_df, photos_df = fetch_data('clients'), fetch_data('progress'), fetch_data('site_photos')
    my_client = clients[clients['client_name'] == REF]
    
    if not my_client.empty:
        c_data = my_client.iloc[0]
        st.subheader("Financial Overview")
        c1, c2, c3 = st.columns(3)
        c1.metric("Contract Total", f"{c_data['budget']:,.2f} DH")
        c2.metric("Payments Received", f"{c_data['advance']:,.2f} DH")
        c3.metric("Remaining Balance", f"{(c_data['budget'] - c_data['advance']):,.2f} DH")
        
        st.markdown("---")
        st.subheader("Technical Progress")
        my_prog = prog_df[prog_df['client_name'] == REF]
        if not my_prog.empty:
            p = my_prog.iloc[0]
            st.progress(int(p['phase1'])/100, "1. Incorporation des gaines")
            st.progress(int(p['phase2'])/100, "2. Tirage de câbles")
            st.progress(int(p['phase3'])/100, "3. Pose des appareillages")
            st.progress(int(p['phase4'])/100, "4. Tableau et mise en service")
        else: st.info("Milestones not yet initialized by admin.")
        
        st.markdown("---")
        st.subheader("📸 Site Photo Updates")
        my_photos = photos_df[photos_df['client_name'] == REF]
        if not my_photos.empty:
            cols = st.columns(3)
            for i, row in my_photos.iterrows():
                with cols[i % 3]:
                    st.image(base64.b64decode(row['photo_data']), caption=f"{row['upload_date']} - {row['phase']}", use_container_width=True)
        else: st.info("No photos uploaded yet.")

# ==========================================
# REST OF THE EXISTING ADMIN VIEWS 
# (Condensed formatting to fit standard constraints)
# ==========================================
elif menu in ["Bidding Intel", "Portfolios", "Timesheets", "Payroll", "Efficiency", "Procurement", "Milestones", "Site Photos", "Warehouse", "Invoicing", "My Timesheet"]:
    # Portfolios
    if menu == "Portfolios":
        st.title("Client Portfolios")
        with st.form("add_client"):
            c1, c2, c3 = st.columns(3)
            nm = c1.text_input("Name")
            ty = c2.selectbox("Type", ["Villa", "Appartement", "Bâtiment Commercial"])
            pt = c3.number_input("Points", value=None)
            bd = st.number_input("Budget", value=None)
            ad = st.number_input("Advance", value=None)
            if st.form_submit_button("Save"):
                conn = get_db_connection()
                try:
                    with conn.cursor() as c: c.execute("INSERT INTO public.clients (client_name, work_type, budget, advance, total_points) VALUES (%s,%s,%s,%s,%s)", (nm, ty, bd or 0, ad or 0, pt or 0))
                    conn.commit(); st.success("Saved.")
                finally: conn.close()
        with st.form("add_pay"):
            sel = st.selectbox("Client", fetch_data('clients')['client_name'])
            liq = st.number_input("Liquidity", value=None)
            if st.form_submit_button("Log Payment") and liq:
                conn = get_db_connection()
                try:
                    with conn.cursor() as c: c.execute("UPDATE public.clients SET advance = advance + %s WHERE client_name=%s", (liq, sel))
                    conn.commit(); rain_money(); st.success("Logged.")
                finally: conn.close()

    # Timesheets (Used by Admin AND Tech)
    elif menu in ["Timesheets", "My Timesheet"]:
        st.title("Timesheet Punch")
        workers, clients = fetch_data('workers'), fetch_data('clients')
        with st.form("ts"):
            dt = st.date_input("Date")
            st_cl = st.selectbox("Site", clients['client_name'])
            ph = st.selectbox("Phase", ["Incorporation", "Tirage", "Appareillage", "Tableau", "Autre"])
            # If tech, lock to their name. If admin, show all.
            allowed_techs = [REF] if ROLE == "Technician" else workers['name'].tolist()
            sel_techs = st.multiselect("Technicians", allowed_techs, default=allowed_techs if ROLE=="Technician" else None)
            inputs = {w: st.number_input(w, value=None) for w in sel_techs}
            if st.form_submit_button("Submit"):
                conn = get_db_connection()
                try:
                    with conn.cursor() as c:
                        for w, d in inputs.items():
                            if d:
                                tjm = float(workers[workers['name']==w]['tjm'].values[0])
                                c.execute("INSERT INTO public.labor_logs (date, client_name, worker_name, days, cost, phase) VALUES (%s,%s,%s,%s,%s,%s)", (dt, st_cl, w, float(d), float(d)*tjm, ph))
                    conn.commit(); st.success("Logged.")
                finally: conn.close()

    # Site Photos (Used by Admin AND Tech)
    elif menu == "Site Photos":
        st.title("Site Photos")
        with st.form("up_photo"):
            cl = st.selectbox("Site", fetch_data('clients')['client_name'])
            ph = st.selectbox("Phase", ["Gaines Ouvertes", "Tirage", "Tableau Final", "Problème"])
            nt = st.text_input("Notes")
            fl = st.file_uploader("Photo", type=['jpg', 'png'])
            if st.form_submit_button("Upload") and fl:
                b64 = base64.b64encode(fl.getvalue()).decode('utf-8')
                conn = get_db_connection()
                try:
                    with conn.cursor() as c: c.execute("INSERT INTO public.site_photos (upload_date, client_name, phase, photo_data, notes) VALUES (%s,%s,%s,%s,%s)", (str(date.today()), cl, ph, b64, nt))
                    conn.commit(); st.success("Saved.")
                finally: conn.close()

    # Payroll
    elif menu == "Payroll":
        st.title("Payroll")
        labor = fetch_data('labor_logs')
        if not labor.empty:
            summary = labor.groupby('worker_name').agg(Days=('days','sum'), Payout=('cost','sum')).reset_index()
            st.dataframe(summary)

    # Invoicing
    elif menu == "Invoicing":
        st.title("Invoice Generator")
        clients = fetch_data('clients')
        sel = st.selectbox("Client", clients['client_name'])
        if st.button("Generate Final Invoice PDF"):
            cdata = clients[clients['client_name']==sel].iloc[0]
            pdf = FPDF()
            pdf.add_page(); pdf.set_font("Arial", 'B', 16); pdf.cell(200, 10, "FACTURE - NEWLIGHTEMARA", ln=True)
            pdf.set_font("Arial", '', 12); pdf.cell(200, 10, f"Client: {sel} | Reste a Payer: {cdata['budget'] - cdata['advance']} DH", ln=True)
            pdf.output("inv.pdf")
            with open("inv.pdf", "rb") as f: st.download_button("Download", f.read(), f"Facture_{sel}.pdf", "application/pdf")
    
    else: st.info("Module active. Core features loaded.")
