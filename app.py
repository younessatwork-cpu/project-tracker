import streamlit as st
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from datetime import date
import base64
import os
from fpdf import FPDF
from streamlit_option_menu import option_menu
import random

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
            c.execute("""CREATE TABLE IF NOT EXISTS public.workers (id SERIAL PRIMARY KEY, name TEXT UNIQUE, tjm REAL)""")
            c.execute("""CREATE TABLE IF NOT EXISTS public.clients (id SERIAL PRIMARY KEY, client_name TEXT UNIQUE, work_type TEXT, budget REAL, advance REAL)""")
            c.execute("""CREATE TABLE IF NOT EXISTS public.labor_logs (id SERIAL PRIMARY KEY, date TEXT, client_name TEXT, worker_name TEXT, days REAL, cost REAL)""")
            c.execute("""CREATE TABLE IF NOT EXISTS public.expenses (id SERIAL PRIMARY KEY, date TEXT, client_name TEXT, item TEXT, amount REAL)""")
            c.execute("""CREATE TABLE IF NOT EXISTS public.progress (client_name TEXT PRIMARY KEY, phase1 REAL, phase2 REAL, phase3 REAL, phase4 REAL)""")
            
            c.execute("""CREATE TABLE IF NOT EXISTS public.site_photos (id SERIAL PRIMARY KEY, upload_date TEXT, client_name TEXT, phase TEXT, photo_data TEXT, notes TEXT)""")
            c.execute("""CREATE TABLE IF NOT EXISTS public.inventory (id SERIAL PRIMARY KEY, item_name TEXT UNIQUE, category TEXT, quantity REAL, unit TEXT)""")
            c.execute("""CREATE TABLE IF NOT EXISTS public.inventory_logs (id SERIAL PRIMARY KEY, date TEXT, item_name TEXT, change_amount REAL, site_allocated TEXT, notes TEXT)""")
            
            c.execute("""ALTER TABLE public.clients ADD COLUMN IF NOT EXISTS total_points REAL DEFAULT 0""")
            c.execute("""ALTER TABLE public.labor_logs ADD COLUMN IF NOT EXISTS phase TEXT DEFAULT 'Général' """)
            c.execute("""ALTER TABLE public.expenses ADD COLUMN IF NOT EXISTS phase TEXT DEFAULT 'Général' """)
        conn.commit()
    finally: conn.close()

init_db()

# ==========================================
# 2. UI STYLING & SECURITY (REBRANDED)
# ==========================================
st.set_page_config(page_title="Newlightemara OS", page_icon="💡", layout="wide")

st.markdown("""
    <style>
        .stButton>button { border-radius: 8px; font-weight: bold; }
        .stMetric { background-color: #f8f9fb; padding: 15px; border-radius: 8px; border: 1px solid #e2e8f0; }
        div[data-testid="stExpander"] { border: 1px solid #e2e8f0; border-radius: 8px; box-shadow: 0 1px 2px rgba(0,0,0,0.05); }
        
        /* 🔥 Hide the annoying plus/minus buttons on all number inputs 🔥 */
        button[title="Step up"], button[title="Step down"] { display: none !important; }
        input[type="number"] { -moz-appearance: textfield; }
        input[type="number"]::-webkit-inner-spin-button, 
        input[type="number"]::-webkit-outer-spin-button { -webkit-appearance: none; margin: 0; }
    </style>
""", unsafe_allow_html=True)

LOGO_FILE = "logo.png" 

def check_password():
    if "password_correct" not in st.session_state: st.session_state["password_correct"] = False
    if not st.session_state["password_correct"]:
        st.markdown("<br><br>", unsafe_allow_html=True)
        col1, col2, col3 = st.columns([1, 1.5, 1])
        with col2:
            if os.path.exists(LOGO_FILE):
                st.image(LOGO_FILE, use_container_width=True)
            else:
                st.markdown("<h1 style='text-align: center; color: #1E293B;'>💡 Newlightemara</h1>", unsafe_allow_html=True)
            
            st.markdown("<p style='text-align: center; color: #64748B;'>Enterprise Electrical Contracting Management</p>", unsafe_allow_html=True)
            
            pwd = st.text_input("System Authentication", type="password", placeholder="Enter Master Password")
            if st.button("Secure Login", use_container_width=True):
                if pwd == "Admin2026!": 
                    st.session_state["password_correct"] = True
                    st.rerun()
                else: st.error("❌ Access Denied.")
        return False
    return True

# ==========================================
# 3. MAIN APPLICATION
# ==========================================
if check_password():
    
    # Sidebar Branding
    if os.path.exists(LOGO_FILE):
        st.sidebar.image(LOGO_FILE, use_container_width=True)
    else:
        st.sidebar.markdown("### 💡 Newlightemara")
        
    st.sidebar.markdown("---")
    
    with st.sidebar:
        menu = option_menu(
            menu_title=None,
            options=[
                "Command Center", 
                "Bidding Intelligence", 
                "Client Portfolios", 
                "Timesheets", 
                "Procurement",
                "Milestones",
                "Site Photos",
                "Warehouse",
                "Invoicing",
                "Team Roster"
            ],
            icons=[
                "bar-chart-line", 
                "graph-up-arrow", 
                "building", 
                "clock-history", 
                "cart-check",
                "gear",
                "camera",
                "box-seam",
                "receipt",
                "people"
            ],
            default_index=0,
            styles={
                "nav-link": {"font-size": "14px", "text-align": "left", "margin":"0px"},
                "nav-link-selected": {"background-color": "#1E293B", "font-weight": "bold"},
            }
        )
    
    st.sidebar.markdown("---")
    if st.sidebar.button("🚪 Secure Logout"):
        st.session_state["password_correct"] = False
        st.rerun()

    def fetch_data(table):
        try: return pd.read_sql(f'SELECT * FROM public.{table}', engine)
        except Exception: return pd.DataFrame()

    # 🔥 NEW: Custom Money Rain Animation
    def rain_money():
        rain_html = """
        <style>
        @keyframes money-fall {
            0% { top: -10%; transform: translateX(0px) rotate(0deg); opacity: 1; }
            100% { top: 110%; transform: translateX(40px) rotate(360deg); opacity: 0; }
        }
        .bill {
            position: fixed;
            z-index: 9999;
            font-size: 3.5rem;
            animation: money-fall linear forwards;
            pointer-events: none;
        }
        </style>
        """
        emojis = ['💸', '💵', '💰', '💶']
        for _ in range(40): 
            emoji = random.choice(emojis)
            left_pos = random.randint(0, 100)
            delay = random.uniform(0, 1.5)
            duration = random.uniform(2.5, 4.0)
            rain_html += f'<div class="bill" style="left: {left_pos}%; animation-delay: {delay}s; animation-duration: {duration}s;">{emoji}</div>'
        
        st.markdown(rain_html, unsafe_allow_html=True)

    # ==========================================
    # VIEW: COMMAND CENTER
    # ==========================================
    if menu == "Command Center":
        st.title("Executive Command Center")
        
        clients_df, labor_df, expenses_df = fetch_data('clients'), fetch_data('labor_logs'), fetch_data('expenses')
        
        if not clients_df.empty:
            tab1, tab2 = st.tabs(["💰 Financial Overview", "🏗️ Active Portfolio Margins"])
            with tab1:
                total_budget = pd.to_numeric(clients_df['budget'], errors='coerce').sum()
                total_advance = pd.to_numeric(clients_df['advance'], errors='coerce').sum()
                total_labor = pd.to_numeric(labor_df['cost'], errors='coerce').sum() if not labor_df.empty else 0
                total_expenses = pd.to_numeric(expenses_df['amount'], errors='coerce').sum() if not expenses_df.empty else 0
                
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Gross Contract Volume", f"{total_budget:,.2f} DH")
                c2.metric("Liquidity (Advances)", f"{total_advance:,.2f} DH")
                c3.metric("Total Labor Expenditure", f"{total_labor:,.2f} DH")
                c4.metric("Total Procurement", f"{total_expenses:,.2f} DH")
            
            with tab2:
                report_data = []
                for _, row in clients_df.iterrows():
                    c_name, c_type, c_budget = row['client_name'], row['work_type'], float(row['budget']) if pd.notna(row['budget']) else 0.0
                    c_points = float(row['total_points']) if 'total_points' in row and pd.notna(row['total_points']) else 0.0
                    c_labor = pd.to_numeric(labor_df[labor_df['client_name'] == c_name]['cost'], errors='coerce').sum() if not labor_df.empty else 0
                    c_mat = pd.to_numeric(expenses_df[expenses_df['client_name'] == c_name]['amount'], errors='coerce').sum() if not expenses_df.empty else 0
                    
                    c_true_profit = c_budget - (c_labor + c_mat)
                    c_margin = (c_true_profit / c_budget * 100) if c_budget > 0 else 0
                    
                    report_data.append({
                        "Portfolio / Site": c_name, "Facility Type": c_type, "Est. Points": int(c_points),
                        "Contract Value": f"{c_budget:,.2f} DH", "Labor Cost": f"{c_labor:,.2f} DH", 
                        "Procurement": f"{c_mat:,.2f} DH", "Net Profit": f"{c_true_profit:,.2f} DH", "Margin": f"{c_margin:.1f}%"
                    })
                st.dataframe(pd.DataFrame(report_data), use_container_width=True, hide_index=True)
        else: st.info("System initializing. No active portfolios detected.")

    # ==========================================
    # VIEW: BIDDING INTELLIGENCE
    # ==========================================
    elif menu == "Bidding Intelligence":
        st.title("Bidding Intelligence")
        clients_df, labor_df, expenses_df = fetch_data('clients'), fetch_data('labor_logs'), fetch_data('expenses')
        if clients_df.empty or labor_df.empty: st.info("Insufficient data to generate historical averages.")
        else:
            analysis_data = []
            for _, row in clients_df.iterrows():
                if 'total_points' in row and pd.notna(row['total_points']) and float(row['total_points']) > 0:
                    c_name, c_points = row['client_name'], float(row['total_points'])
                    c_labor = pd.to_numeric(labor_df[labor_df['client_name'] == c_name]['cost'], errors='coerce').sum() if not labor_df.empty else 0
                    c_mat = pd.to_numeric(expenses_df[expenses_df['client_name'] == c_name]['amount'], errors='coerce').sum() if not expenses_df.empty else 0
                    analysis_data.append({"Facility Type": row['work_type'], "Labor Cost per Point": c_labor / c_points, "Material Cost per Point": c_mat / c_points})
            if analysis_data:
                avg_df = pd.DataFrame(analysis_data).groupby('Facility Type').mean().reset_index()
                for col in avg_df.columns[1:]: avg_df[col] = avg_df[col].apply(lambda x: f"{x:,.2f} DH")
                st.dataframe(avg_df, hide_index=True, use_container_width=True)

    # ==========================================
    # VIEW: CLIENT PORTFOLIOS
    # ==========================================
    elif menu == "Client Portfolios":
        st.title("Client Portfolios & Sites")
        with st.expander("➕ Register New Client Portfolio", expanded=False):
            with st.form("add_client", clear_on_submit=True):
                c1, c2, c3 = st.columns(3)
                c_name = c1.text_input("Client / Owner Name")
                c_type = c2.selectbox("Facility Type", ["Villa", "Appartement", "Maison", "Bâtiment Commercial", "Maintenance"])
                c_points = c3.number_input("Est. Electrical Points", min_value=0, step=5)
                c4, c5 = st.columns(2)
                c_budget = c4.number_input("Gross Contract Value (DH)", min_value=0.0, step=1000.0)
                c_avance = c5.number_input("Initial Advance Paid (DH)", min_value=0.0, step=1000.0)
                if st.form_submit_button("Initialize Portfolio"):
                    if c_name:
                        conn = get_db_connection()
                        try:
                            with conn.cursor() as c:
                                c.execute("""INSERT INTO public.clients (client_name, work_type, budget, advance, total_points) VALUES (%s, %s, %s, %s, %s)""", 
                                          (c_name.strip(), c_type, c_budget, c_avance, c_points))
                            conn.commit()
                            st.success("Portfolio successfully initialized.")
                        finally: conn.close()
        
        with st.expander("💳 Log Accounts Receivable (Advances)", expanded=False):
            clients = fetch_data('clients')
            if not clients.empty:
                with st.form("add_payment", clear_on_submit=True):
                    c1, c2 = st.columns([2, 1])
                    sel_client = c1.selectbox("Target Portfolio", clients['client_name'])
                    new_money = c2.number_input("Liquidity Received (DH)", min_value=0.0, step=500.0)
                    if st.form_submit_button("Log Transaction"):
                        if new_money > 0:
                            conn = get_db_connection()
                            try:
                                with conn.cursor() as c: c.execute("""UPDATE public.clients SET advance = advance + %s WHERE client_name = %s""", (new_money, sel_client))
                                conn.commit()
                                
                                rain_money() # 🔥 Triggers the money flow!
                                st.success("Transaction verified. Liquidity added.")
                            finally: conn.close()

        st.dataframe(fetch_data('clients'), hide_index=True, use_container_width=True)

    # ==========================================
    # VIEW: TIMESHEETS & ALLOCATION
    # ==========================================
    elif menu == "Timesheets":
        st.title("Timesheets & Labor Allocation")
        workers, clients = fetch_data('workers'), fetch_data('clients')
        if workers.empty or clients.empty: st.warning("Requires registered technicians and portfolios.")
        else:
            with st.expander("📝 File New Timesheet", expanded=True):
                with st.form("labor_entry"):
                    c1, c2, c3 = st.columns(3)
                    log_date = c1.date_input("Date of Execution", date.today())
                    sel_client = c2.selectbox("Assigned Site", clients['client_name'])
                    sel_phase = c3.selectbox("Execution Phase", ["Incorporation (Gaines)", "Tirage de Câbles", "Appareillage", "Tableau & Mise en Service", "Autre"])
                    
                    active_workers = st.multiselect("Allocate Technicians:", workers['name'].tolist())
                    worker_inputs = {}
                    if active_workers:
                        cols = st.columns(min(len(active_workers), 4))
                        for i, w_name in enumerate(active_workers):
                            w_tjm = workers[workers['name'] == w_name]['tjm'].values[0]
                            with cols[i % 4]: worker_inputs[w_name] = st.number_input(f"{w_name} (Rate: {w_tjm})", min_value=0.0, max_value=31.0, step=0.5)
                    
                    if st.form_submit_button("Commit Timesheets"):
                        if active_workers:
                            logs_added = 0
                            conn = get_db_connection()
                            try:
                                with conn.cursor() as c:
                                    for w_name, days in worker_inputs.items():
                                        if days > 0:
                                            tjm = float(workers[workers['name'] == w_name]['tjm'].values[0])
                                            clean_days, clean_cost = float(days), float(days) * tjm
                                            c.execute("""INSERT INTO public.labor_logs (date, client_name, worker_name, days, cost, phase) 
                                                         VALUES (%s, %s, %s, %s, %s, %s)""", (log_date, sel_client, w_name, clean_days, clean_cost, sel_phase))
                                            logs_added += 1
                                conn.commit()
                                if logs_added > 0: st.success(f"{logs_added} allocation records committed.")
                            finally: conn.close()
            st.dataframe(fetch_data('labor_logs').sort_values(by='date', ascending=False).head(15), hide_index=True, use_container_width=True)

    # ==========================================
    # VIEW: PROCUREMENT & EXPENSES
    # ==========================================
    elif menu == "Procurement":
        st.title("Procurement & Site Expenses")
        clients = fetch_data('clients')
        if not clients.empty:
            with st.expander("🛒 Log Direct Site Purchase", expanded=True):
                with st.form("log_expense", clear_on_submit=True):
                    c1, c2, c3 = st.columns(3)
                    exp_date = c1.date_input("Date of Purchase", date.today())
                    sel_client = c2.selectbox("Charge to Site", ["Stock / General"] + list(clients['client_name']))
                    sel_phase = c3.selectbox("Material Class", ["Incorporation", "Tirage de Câbles", "Appareillage", "Tableau", "Outillage"])
                    item_desc = st.text_input("Invoice / Manifest Description")
                    amount = st.number_input("Total Disbursement (DH)", min_value=0.0, step=50.0)
                    
                    if st.form_submit_button("Commit Procurement Log"):
                        if item_desc and amount > 0:
                            conn = get_db_connection()
                            try:
                                with conn.cursor() as c:
                                    c.execute("""INSERT INTO public.expenses (date, client_name, item, amount, phase) VALUES (%s, %s, %s, %s, %s)""", 
                                              (exp_date, sel_client, item_desc, amount, sel_phase))
                                conn.commit()
                                st.success("Procurement logged successfully.")
                            finally: conn.close()
            st.dataframe(fetch_data('expenses').sort_values(by='date', ascending=False).head(15), hide_index=True, use_container_width=True)

    # ==========================================
    # VIEW: MILESTONE TRACKING
    # ==========================================
    elif menu == "Milestones":
        st.title("Technical Milestone Tracking")
        clients = fetch_data('clients')
        if not clients.empty:
            sel_client = st.selectbox("Select Active Site", clients['client_name'])
            prog_df = pd.read_sql(f"SELECT * FROM public.progress WHERE client_name='{sel_client}'", engine)
            v1, v2, v3, v4 = 0.0, 0.0, 0.0, 0.0
            if not prog_df.empty:
                v1, v2, v3, v4 = prog_df.iloc[0][['phase1', 'phase2', 'phase3', 'phase4']]
                if max(v1, v2, v3, v4) <= 1.0 and sum((v1, v2, v3, v4)) > 0: v1, v2, v3, v4 = v1*100, v2*100, v3*100, v4*100

            with st.form("update_progress"):
                p1 = st.slider("1. Incorporation des gaines (%)", 0, 100, int(v1), 5)
                p2 = st.slider("2. Tirage de câbles (%)", 0, 100, int(v2), 5)
                p3 = st.slider("3. Pose des appareillages (%)", 0, 100, int(v3), 5)
                p4 = st.slider("4. Tableau et mise en service (%)", 0, 100, int(v4), 5)
                if st.form_submit_button("Update Site Milestones"):
                    conn = get_db_connection()
                    try:
                        with conn.cursor() as c:
                            c.execute("""INSERT INTO public.progress (client_name, phase1, phase2, phase3, phase4) VALUES (%s, %s, %s, %s, %s)
                                         ON CONFLICT (client_name) DO UPDATE SET phase1=EXCLUDED.phase1, phase2=EXCLUDED.phase2, phase3=EXCLUDED.phase3, phase4=EXCLUDED.phase4""", 
                                      (sel_client, p1, p2, p3, p4))
                        conn.commit()
                        st.success(f"Milestones updated for {sel_client}.")
                    finally: conn.close()

    # ==========================================
    # VIEW: SITE PHOTOS
    # ==========================================
    elif menu == "Site Photos":
        st.title("📸 Site Photo Evidence & As-Builts")
        clients = fetch_data('clients')
        if clients.empty: st.warning("Requires active portfolio.")
        else:
            with st.expander("⬆️ Upload New Site Photo", expanded=True):
                with st.form("photo_upload", clear_on_submit=True):
                    c1, c2 = st.columns(2)
                    sel_client = c1.selectbox("Assign to Site", clients['client_name'])
                    sel_phase = c2.selectbox("Construction Phase", ["Gaines Ouvertes", "Tirage", "Tableau Final", "Problème/Bloquant"])
                    notes = st.text_input("Technical Notes")
                    
                    uploaded_file = st.file_uploader("Take Photo or Upload", type=['jpg', 'jpeg', 'png'])
                    
                    if st.form_submit_button("Securely Save to Database"):
                        if uploaded_file is not None:
                            bytes_data = uploaded_file.getvalue()
                            base64_string = base64.b64encode(bytes_data).decode('utf-8')
                            conn = get_db_connection()
                            try:
                                with conn.cursor() as c:
                                    c.execute("""INSERT INTO public.site_photos (upload_date, client_name, phase, photo_data, notes) VALUES (%s, %s, %s, %s, %s)""", 
                                              (str(date.today()), sel_client, sel_phase, base64_string, notes))
                                conn.commit()
                                st.success("Image secured and assigned to client portfolio.")
                            finally: conn.close()
                        else: st.error("Please attach an image.")

            st.markdown("### 🗂️ Site Archives")
            photos_df = fetch_data('site_photos')
            if not photos_df.empty:
                archive_client = st.selectbox("Filter Archives by Site", ["All Sites"] + list(clients['client_name']))
                if archive_client != "All Sites": photos_df = photos_df[photos_df['client_name'] == archive_client]
                
                cols = st.columns(3)
                for i, row in photos_df.iterrows():
                    with cols[i % 3]:
                        st.image(base64.b64decode(row['photo_data']), caption=f"{row['upload_date']} - {row['phase']}", use_container_width=True)
                        if row['notes']: st.caption(row['notes'])
            else: st.info("No photos archived yet.")

    # ==========================================
    # VIEW: WAREHOUSE INVENTORY
    # ==========================================
    elif menu == "Warehouse":
        st.title("🏭 Warehouse & Bulk Material")
        tab1, tab2 = st.tabs(["📦 Check-In Stock", "🚚 Allocate to Site"])
        
        with tab1:
            with st.form("check_in_stock", clear_on_submit=True):
                c1, c2 = st.columns(2)
                item_name = c1.text_input("Material Name (e.g., Câble 1.5mm Rouge)")
                category = c2.selectbox("Category", ["Câblage", "Gaines", "Appareillage", "Disjoncteurs"])
                c3, c4 = st.columns(2)
                qty = c3.number_input("Quantity Received", min_value=1.0, step=1.0)
                unit = c4.selectbox("Unit of Measurement", ["Rouleaux (100m)", "Mètres", "Unités", "Boîtes"])
                
                if st.form_submit_button("Receive Stock"):
                    conn = get_db_connection()
                    try:
                        with conn.cursor() as c:
                            c.execute("""INSERT INTO public.inventory (item_name, category, quantity, unit) VALUES (%s, %s, %s, %s)
                                         ON CONFLICT (item_name) DO UPDATE SET quantity = public.inventory.quantity + EXCLUDED.quantity""",
                                      (item_name.strip(), category, qty, unit))
                            c.execute("""INSERT INTO public.inventory_logs (date, item_name, change_amount, site_allocated, notes) VALUES (%s, %s, %s, %s, %s)""",
                                      (str(date.today()), item_name.strip(), qty, "Warehouse Check-In", "Initial Delivery"))
                        conn.commit()
                        st.success(f"Received {qty} {unit} of {item_name}.")
                    finally: conn.close()

        with tab2:
            inv_df, clients = fetch_data('inventory'), fetch_data('clients')
            if not inv_df.empty and not clients.empty:
                with st.form("allocate_stock", clear_on_submit=True):
                    c1, c2 = st.columns(2)
                    sel_item = c1.selectbox("Select Material to Deploy", inv_df['item_name'])
                    sel_site = c2.selectbox("Target Site", clients['client_name'])
                    
                    max_stock = float(inv_df[inv_df['item_name'] == sel_item]['quantity'].values[0])
                    deploy_qty = st.number_input(f"Quantity to Deploy (Max {max_stock})", min_value=1.0, max_value=max_stock, step=1.0)
                    
                    if st.form_submit_button("Deploy to Site"):
                        conn = get_db_connection()
                        try:
                            with conn.cursor() as c:
                                c.execute("""UPDATE public.inventory SET quantity = quantity - %s WHERE item_name = %s""", (deploy_qty, sel_item))
                                c.execute("""INSERT INTO public.inventory_logs (date, item_name, change_amount, site_allocated, notes) VALUES (%s, %s, %s, %s, %s)""",
                                          (str(date.today()), sel_item, -deploy_qty, sel_site, "Deployed to field"))
                            conn.commit()
                            st.success(f"Deployed {deploy_qty} to {sel_site}.")
                        finally: conn.close()
        
        st.markdown("### Current Stock Levels")
        st.dataframe(fetch_data('inventory'), hide_index=True, use_container_width=True)

    # ==========================================
    # VIEW: AUTOMATED INVOICING
    # ==========================================
    elif menu == "Invoicing":
        st.title("📄 Invoice Generator")
        clients = fetch_data('clients')
        if clients.empty: st.warning("Requires active portfolio.")
        else:
            sel_client = st.selectbox("Select Client to Bill", clients['client_name'])
            client_data = clients[clients['client_name'] == sel_client].iloc[0]
            budget, advance = float(client_data['budget']), float(client_data['advance'])
            balance_due = budget - advance
            
            st.info(f"**Contract:** {budget:,.2f} DH  |  **Paid:** {advance:,.2f} DH  |  **Balance Due:** {balance_due:,.2f} DH")
            
            if st.button("Generer Facture / Generate PDF"):
                pdf = FPDF()
                pdf.add_page()
                
                if os.path.exists(LOGO_FILE):
                    try: pdf.image(LOGO_FILE, x=10, y=8, w=33)
                    except: pass
                
                pdf.set_font("Arial", style='B', size=16)
                pdf.cell(200, 10, txt="FACTURE OFFICIELLE / NEWLIGHTEMARA", ln=True, align='C')
                pdf.ln(10)
                
                pdf.set_font("Arial", size=12)
                pdf.cell(200, 10, txt=f"Date: {date.today()}", ln=True)
                pdf.cell(200, 10, txt=f"Client: {sel_client}", ln=True)
                pdf.cell(200, 10, txt=f"Type d'installation: {client_data['work_type']}", ln=True)
                
                pdf.line(10, 60, 200, 60)
                pdf.ln(10)
                
                pdf.set_font("Arial", style='B', size=12)
                pdf.cell(100, 10, txt="Description", border=1)
                pdf.cell(90, 10, txt="Montant (DH)", border=1, ln=True, align='R')
                
                pdf.set_font("Arial", size=12)
                pdf.cell(100, 10, txt="Valeur Totale du Contrat", border=1)
                pdf.cell(90, 10, txt=f"{budget:,.2f}", border=1, ln=True, align='R')
                
                pdf.cell(100, 10, txt="Avances Deja Payees", border=1)
                pdf.cell(90, 10, txt=f"- {advance:,.2f}", border=1, ln=True, align='R')
                
                pdf.set_font("Arial", style='B', size=14)
                pdf.cell(100, 15, txt="NET A PAYER / BALANCE DUE", border=1)
                pdf.cell(90, 15, txt=f"{balance_due:,.2f} DH", border=1, ln=True, align='R')
                
                pdf.ln(20)
                pdf.set_font("Arial", style='I', size=10)
                pdf.cell(200, 10, txt="Merci pour votre confiance. / Thank you for your business.", ln=True, align='C')
                
                pdf.output("facture_temp.pdf")
                with open("facture_temp.pdf", "rb") as pdf_file: PDFbyte = pdf_file.read()
                
                st.download_button(label="📥 Download PDF Invoice", data=PDFbyte, file_name=f"Facture_{sel_client}.pdf", mime='application/octet-stream', type="primary")

    # ==========================================
    # VIEW: TEAM ROSTER
    # ==========================================
    elif menu == "Team Roster":
        st.title("Team & Technician Roster")
        with st.form("add_worker", clear_on_submit=True):
            c1, c2 = st.columns(2)
            w_name = c1.text_input("Technician Full Name")
            w_tjm = c2.number_input("Daily Rate / TJM (DH)", min_value=0.0, step=10.0)
            if st.form_submit_button("Authorize & Add to Roster"):
                if w_name:
                    conn = get_db_connection()
                    try:
                        with conn.cursor() as c: c.execute("""INSERT INTO public.workers (name, tjm) VALUES (%s, %s)""", (w_name.strip(), w_tjm))
                        conn.commit()
                        st.success(f"{w_name} successfully onboarded.")
                    except: st.error("Technician already exists.")
                    finally: conn.close()
        st.dataframe(fetch_data('workers'), hide_index=True, use_container_width=True)
