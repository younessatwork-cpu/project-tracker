import streamlit as st
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from datetime import date

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
            c.execute('''CREATE TABLE IF NOT EXISTS public.workers (id SERIAL PRIMARY KEY, name TEXT UNIQUE, tjm REAL)''')
            c.execute('''CREATE TABLE IF NOT EXISTS public.clients (id SERIAL PRIMARY KEY, client_name TEXT UNIQUE, work_type TEXT, budget REAL, advance REAL)''')
            c.execute('''CREATE TABLE IF NOT EXISTS public.labor_logs (id SERIAL PRIMARY KEY, date TEXT, client_name TEXT, worker_name TEXT, days REAL, cost REAL)''')
            c.execute('''CREATE TABLE IF NOT EXISTS public.expenses (id SERIAL PRIMARY KEY, date TEXT, client_name TEXT, item TEXT, amount REAL)''')
            c.execute('''CREATE TABLE IF NOT EXISTS public.progress (client_name TEXT PRIMARY KEY, phase1 REAL, phase2 REAL, phase3 REAL, phase4 REAL)''')
            c.execute('''ALTER TABLE public.clients ADD COLUMN IF NOT EXISTS total_points REAL DEFAULT 0''')
            c.execute('''ALTER TABLE public.labor_logs ADD COLUMN IF NOT EXISTS phase TEXT DEFAULT 'Général'''')
            c.execute('''ALTER TABLE public.expenses ADD COLUMN IF NOT EXISTS phase TEXT DEFAULT 'Général'''')
        conn.commit()
    finally: conn.close()

init_db()

# ==========================================
# 2. UI STYLING & SECURITY
# ==========================================
st.set_page_config(page_title="Contractor OS", page_icon="⚡", layout="wide")

# Custom CSS for a cleaner, modern look
st.markdown("""
    <style>
        .stButton>button { border-radius: 8px; font-weight: bold; }
        .stMetric { background-color: #f8f9fb; padding: 15px; border-radius: 8px; border: 1px solid #e2e8f0; }
        div[data-testid="stExpander"] { border: 1px solid #e2e8f0; border-radius: 8px; box-shadow: 0 1px 2px rgba(0,0,0,0.05); }
    </style>
""", unsafe_allow_html=True)

def check_password():
    if "password_correct" not in st.session_state: st.session_state["password_correct"] = False
    if not st.session_state["password_correct"]:
        st.markdown("<br><br><h1 style='text-align: center; color: #1E293B;'>⚡ Contractor OS</h1>", unsafe_allow_html=True)
        st.markdown("<p style='text-align: center; color: #64748B;'>Enterprise Electrical Contracting Management</p>", unsafe_allow_html=True)
        col1, col2, col3 = st.columns([1, 1.5, 1])
        with col2:
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
    
    # --- Sidebar Navigation ---
    st.sidebar.markdown("### ⚡ System Menu")
    menu = st.sidebar.radio("Navigation", [
        "📊 Executive Command Center", 
        "📈 Bidding Intelligence", 
        "👥 Team & Technician Roster", 
        "🏢 Client Portfolios & Sites", 
        "💳 Accounts Receivable", 
        "⏱️ Timesheets & Allocation", 
        "📦 Procurement & Materials",
        "⚙️ Milestone Tracking"
    ], label_visibility="collapsed")
    
    st.sidebar.markdown("---")
    st.sidebar.caption("Data Security")
    if st.sidebar.button("📥 Export SQL Backup"):
        st.sidebar.info("Authorized. Access raw CSV data via Supabase console.")
    if st.sidebar.button("🚪 Logout"):
        st.session_state["password_correct"] = False
        st.rerun()

    def fetch_data(table):
        try: return pd.read_sql(f'SELECT * FROM public.{table}', engine)
        except Exception: return pd.DataFrame()

    # ==========================================
    # VIEW 1: EXECUTIVE COMMAND CENTER
    # ==========================================
    if menu == "📊 Executive Command Center":
        st.title("Executive Command Center")
        st.markdown("Global overview of financial health and active portfolios.")
        
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
                        "Portfolio / Client": c_name, "Facility Type": c_type, "Est. Points": int(c_points),
                        "Contract Value": f"{c_budget:,.2f} DH", "Labor Cost": f"{c_labor:,.2f} DH", 
                        "Procurement": f"{c_mat:,.2f} DH", "Net Profit": f"{c_true_profit:,.2f} DH", "Margin": f"{c_margin:.1f}%"
                    })
                st.dataframe(pd.DataFrame(report_data), use_container_width=True, hide_index=True)
        else: st.info("System initializing. No active portfolios detected.")

    # ==========================================
    # VIEW 2: BIDDING INTELLIGENCE
    # ==========================================
    elif menu == "📈 Bidding Intelligence":
        st.title("Bidding Intelligence")
        st.markdown("Historical unit economics for precision estimating.")
        
        clients_df, labor_df, expenses_df = fetch_data('clients'), fetch_data('labor_logs'), fetch_data('expenses')
        if clients_df.empty or labor_df.empty: st.info("Insufficient data to generate historical averages.")
        else:
            analysis_data = []
            for _, row in clients_df.iterrows():
                if 'total_points' in row and pd.notna(row['total_points']) and float(row['total_points']) > 0:
                    c_name, c_points = row['client_name'], float(row['total_points'])
                    c_labor = pd.to_numeric(labor_df[labor_df['client_name'] == c_name]['cost'], errors='coerce').sum() if not labor_df.empty else 0
                    c_mat = pd.to_numeric(expenses_df[expenses_df['client_name'] == c_name]['amount'], errors='coerce').sum() if not expenses_df.empty else 0
                    
                    analysis_data.append({
                        "Facility Type": row['work_type'],
                        "Labor Cost per Point": c_labor / c_points,
                        "Material Cost per Point": c_mat / c_points,
                        "Total Base Cost per Point": (c_labor + c_mat) / c_points
                    })
            if analysis_data:
                avg_df = pd.DataFrame(analysis_data).groupby('Facility Type').mean().reset_index()
                for col in avg_df.columns[1:]: avg_df[col] = avg_df[col].apply(lambda x: f"{x:,.2f} DH")
                
                c1, c2 = st.columns([2, 1])
                with c1:
                    st.subheader("Unit Economics by Facility Type")
                    st.dataframe(avg_df, hide_index=True, use_container_width=True)
                with c2:
                    st.subheader("Bleed by Phase")
                    if 'phase' in labor_df.columns:
                        phase_cost = labor_df.groupby('phase')['cost'].sum().reset_index()
                        st.bar_chart(phase_cost.set_index('phase'))
            else: st.warning("Ensure 'Electrical Points' are logged in Client Portfolios.")

    # ==========================================
    # VIEW 3: TEAM & TECHNICIAN ROSTER
    # ==========================================
    elif menu == "👥 Team & Technician Roster":
        st.title("Team & Technician Roster")
        
        with st.expander("➕ Onboard New Technician", expanded=False):
            with st.form("add_worker", clear_on_submit=True):
                c1, c2 = st.columns(2)
                w_name = c1.text_input("Technician Full Name")
                w_tjm = c2.number_input("Daily Rate / TJM (DH)", min_value=0.0, step=10.0)
                if st.form_submit_button("Authorize & Add to Roster"):
                    if w_name:
                        conn = get_db_connection()
                        try:
                            with conn.cursor() as c: c.execute("INSERT INTO public.workers (name, tjm) VALUES (%s, %s)", (w_name.strip(), w_tjm))
                            conn.commit()
                            st.success(f"{w_name} successfully onboarded.")
                        except: st.error("Technician already exists in the system.")
                        finally: conn.close()
        
        workers_df = fetch_data('workers')
        if not workers_df.empty:
            clean_workers = workers_df[['name', 'tjm']].rename(columns={'name': 'Technician Name', 'tjm': 'Daily Rate (DH)'})
            st.dataframe(clean_workers, hide_index=True, use_container_width=True)

    # ==========================================
    # VIEW 4: CLIENT PORTFOLIOS
    # ==========================================
    elif menu == "🏢 Client Portfolios & Sites":
        st.title("Client Portfolios & Sites")
        
        with st.expander("➕ Register New Client Portfolio", expanded=False):
            with st.form("add_client", clear_on_submit=True):
                c1, c2, c3 = st.columns(3)
                c_name = c1.text_input("Client / Owner Name")
                c_type = c2.selectbox("Facility Type", ["Villa", "Appartement", "Maison", "Bâtiment Commercial", "Maintenance/Réparation"])
                c_points = c3.number_input("Est. Electrical Points", min_value=0, step=5)
                
                c4, c5 = st.columns(2)
                c_budget = c4.number_input("Gross Contract Value (DH)", min_value=0.0, step=1000.0)
                c_avance = c5.number_input("Initial Advance Paid (DH)", min_value=0.0, step=1000.0)
                
                if st.form_submit_button("Initialize Portfolio"):
                    if c_name:
                        conn = get_db_connection()
                        try:
                            with conn.cursor() as c:
                                c.execute("INSERT INTO public.clients (client_name, work_type, budget, advance, total_points) VALUES (%s, %s, %s, %s, %s)", 
                                          (c_name.strip(), c_type, c_budget, c_avance, c_points))
                            conn.commit()
                            st.success("Portfolio successfully initialized.")
                        except: st.error("Client identity conflict.")
                        finally: conn.close()
        
        clients_df = fetch_data('clients')
        if not clients_df.empty:
            clean_clients = clients_df[['client_name', 'work_type', 'total_points', 'budget', 'advance']].rename(
                columns={'client_name': 'Client / Site', 'work_type': 'Facility', 'total_points': 'Points', 'budget': 'Contract Value', 'advance': 'Advances'}
            )
            st.dataframe(clean_clients, hide_index=True, use_container_width=True)

    # ==========================================
    # VIEW 5: ACCOUNTS RECEIVABLE
    # ==========================================
    elif menu == "💳 Accounts Receivable":
        st.title("Accounts Receivable")
        clients = fetch_data('clients')
        if clients.empty: st.warning("Requires active client portfolio.")
        else:
            with st.container(border=True):
                with st.form("add_payment", clear_on_submit=True):
                    c1, c2 = st.columns([2, 1])
                    sel_client = c1.selectbox("Target Portfolio", clients['client_name'])
                    new_money = c2.number_input("Liquidity Received (DH)", min_value=0.0, step=500.0)
                    if st.form_submit_button("Log Transaction"):
                        if new_money > 0:
                            conn = get_db_connection()
                            try:
                                with conn.cursor() as c: c.execute("UPDATE public.clients SET advance = advance + %s WHERE client_name = %s", (new_money, sel_client))
                                conn.commit()
                                st.success("Transaction verified and logged.")
                            finally: conn.close()

    # ==========================================
    # VIEW 6: TIMESHEETS & ALLOCATION
    # ==========================================
    elif menu == "⏱️ Timesheets & Allocation":
        st.title("Timesheets & Labor Allocation")
        workers, clients = fetch_data('workers'), fetch_data('clients')
        
        if workers.empty or clients.empty: st.warning("Requires registered technicians and active portfolios.")
        else:
            with st.expander("📝 File New Timesheet", expanded=True):
                with st.form("labor_entry"):
                    c1, c2, c3 = st.columns(3)
                    log_date = c1.date_input("Date of Execution", date.today())
                    sel_client = c2.selectbox("Assigned Site", clients['client_name'])
                    sel_phase = c3.selectbox("Execution Phase", ["Incorporation (Gaines)", "Tirage de Câbles", "Appareillage (Finition)", "Tableau & Mise en Service", "Autre"])
                    
                    st.markdown("##### Allocate Technicians")
                    active_workers = st.multiselect("Select Active Roster:", workers['name'].tolist())
                    worker_inputs = {}
                    
                    if active_workers:
                        cols = st.columns(min(len(active_workers), 4))
                        for i, w_name in enumerate(active_workers):
                            w_tjm = workers[workers['name'] == w_name]['tjm'].values[0]
                            with cols[i % 4]: worker_inputs[w_name] = st.number_input(f"{w_name} (Rate: {w_tjm})", min_value=0.0, max_value=31.0, step=0.5)
                    
                    if st.form_submit_button("Commit Timesheets"):
                        if not active_workers: st.error("No technicians allocated.")
                        else:
                            logs_added = 0
                            conn = get_db_connection()
                            try:
                                with conn.cursor() as c:
                                    for w_name, days in worker_inputs.items():
                                        if days > 0:
                                            tjm = float(workers[workers['name'] == w_name]['tjm'].values[0])
                                            clean_days, clean_cost = float(days), float(days) * tjm
                                            c.execute('''INSERT INTO public.labor_logs (date, client_name, worker_name, days, cost, phase) 
                                                         VALUES (%s, %s, %s, %s, %s, %s)''', (log_date, sel_client, w_name, clean_days, clean_cost, sel_phase))
                                            logs_added += 1
                                conn.commit()
                                if logs_added > 0: st.success(f"{logs_added} allocation records committed.")
                            finally: conn.close()
            
            labor_df = fetch_data('labor_logs')
            if not labor_df.empty:
                clean_labor = labor_df[['date', 'client_name', 'phase', 'worker_name', 'days', 'cost']].rename(
                    columns={'date': 'Date', 'client_name': 'Site', 'phase': 'Phase', 'worker_name': 'Technician', 'days': 'Days Logged', 'cost': 'Cost (DH)'})
                st.dataframe(clean_labor.sort_values(by='Date', ascending=False).head(15), hide_index=True, use_container_width=True)

    # ==========================================
    # VIEW 7: PROCUREMENT & MATERIALS
    # ==========================================
    elif menu == "📦 Procurement & Materials":
        st.title("Procurement & Materials")
        clients = fetch_data('clients')
        if clients.empty: st.warning("Requires active client portfolio.")
        else:
            with st.expander("🛒 Log New Purchase Order", expanded=True):
                with st.form("log_expense", clear_on_submit=True):
                    c1, c2, c3 = st.columns(3)
                    exp_date = c1.date_input("Date of Purchase", date.today())
                    sel_client = c2.selectbox("Charge to Site", ["Stock / General"] + list(clients['client_name']))
                    sel_phase = c3.selectbox("Material Class", ["Incorporation (Gaines/Boitiers)", "Tirage de Câbles (Fils)", "Appareillage (Prises/Interrupteurs)", "Tableau (Disjoncteurs)", "Outillage"])
                    
                    item_desc = st.text_input("Invoice / Manifest Description")
                    amount = st.number_input("Total Disbursement (DH)", min_value=0.0, step=50.0)
                    
                    if st.form_submit_button("Commit Procurement Log"):
                        if item_desc and amount > 0:
                            conn = get_db_connection()
                            try:
                                with conn.cursor() as c:
                                    c.execute("INSERT INTO public.expenses (date, client_name, item, amount, phase) VALUES (%s, %s, %s, %s, %s)", 
                                              (exp_date, sel_client, item_desc, amount, sel_phase))
                                conn.commit()
                                st.success("Procurement logged successfully.")
                            finally: conn.close()
            
            exp_df = fetch_data('expenses')
            if not exp_df.empty:
                clean_exp = exp_df[['date', 'client_name', 'phase', 'item', 'amount']].rename(
                    columns={'date': 'Date', 'client_name': 'Site', 'phase': 'Classification', 'item': 'Description', 'amount': 'Cost (DH)'})
                st.dataframe(clean_exp.sort_values(by='Date', ascending=False).head(15), hide_index=True, use_container_width=True)

    # ==========================================
    # VIEW 8: MILESTONE TRACKING
    # ==========================================
    elif menu == "⚙️ Milestone Tracking":
        st.title("Technical Milestone Tracking")
        clients = fetch_data('clients')
        if clients.empty: st.warning("Requires active client portfolio.")
        else:
            with st.container(border=True):
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
                    
                    st.info(f"🏆 **Global Completion Variance:** {((p1 + p2 + p3 + p4) / 4):.1f}%")
                    
                    if st.form_submit_button("Update Site Milestones"):
                        conn = get_db_connection()
                        try:
                            with conn.cursor() as c:
                                c.execute('''INSERT INTO public.progress (client_name, phase1, phase2, phase3, phase4) VALUES (%s, %s, %s, %s, %s)
                                             ON CONFLICT (client_name) DO UPDATE SET phase1=EXCLUDED.phase1, phase2=EXCLUDED.phase2, phase3=EXCLUDED.phase3, phase4=EXCLUDED.phase4''', 
                                          (sel_client, p1, p2, p3, p4))
                            conn.commit()
                            st.success(f"Milestones updated for {sel_client}.")
                        finally: conn.close()
