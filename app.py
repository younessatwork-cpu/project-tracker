import streamlit as st
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from datetime import date

# ==========================================
# 1. ENTERPRISE DATABASE CONFIGURATION
# ==========================================

# Safely fetch and format the database URL
raw_url = st.secrets["DATABASE_URL"]

# Force PostgreSQL protocol
if raw_url.startswith("postgres://"):
    raw_url = raw_url.replace("postgres://", "postgresql://", 1)

# FORCE SESSION MODE (Port 5432) - This completely fixes the loop crashing error
if ":6543" in raw_url:
    raw_url = raw_url.replace(":6543", ":5432")

# FORCE SSL ENCRYPTION - Required by Supabase
if "?" not in raw_url:
    raw_url += "?sslmode=require"
elif "sslmode=" not in raw_url:
    raw_url += "&sslmode=require"

DB_URL = raw_url

# SQLAlchemy Engine for fast, read-only analytical queries
engine = create_engine(DB_URL)

# ==========================================
# 2. CORE DATABASE FUNCTIONS
# ==========================================

def get_db_connection():
    """Returns a fresh, clean connection to the database."""
    return psycopg2.connect(DB_URL)

def init_db():
    """Initializes the database schema with the new Client/Work Type structure."""
    query = '''
    CREATE TABLE IF NOT EXISTS public.workers (
        id SERIAL PRIMARY KEY, name TEXT UNIQUE, tjm REAL
    );
    CREATE TABLE IF NOT EXISTS public.clients (
        id SERIAL PRIMARY KEY, client_name TEXT UNIQUE, work_type TEXT, budget REAL, advance REAL
    );
    CREATE TABLE IF NOT EXISTS public.labor_logs (
        id SERIAL PRIMARY KEY, date TEXT, client_name TEXT, worker_name TEXT, days REAL, cost REAL
    );
    CREATE TABLE IF NOT EXISTS public.expenses (
        id SERIAL PRIMARY KEY, date TEXT, client_name TEXT, item TEXT, amount REAL
    );
    CREATE TABLE IF NOT EXISTS public.progress (
        client_name TEXT PRIMARY KEY, phase1 REAL, phase2 REAL, phase3 REAL, phase4 REAL
    );
    '''
    conn = get_db_connection()
    try:
        with conn.cursor() as c:
            c.execute(query)
        conn.commit()
    finally:
        conn.close()

# Initialize tables immediately
init_db()

# ==========================================
# 3. SECURITY GATEKEEPER
# ==========================================

def check_password():
    """Secure login mechanism."""
    if "password_correct" not in st.session_state:
        st.session_state["password_correct"] = False

    if not st.session_state["password_correct"]:
        st.markdown("<h1 style='text-align: center;'>⚡ Contractor OS</h1>", unsafe_allow_html=True)
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            pwd = st.text_input("Enter Master Password", type="password")
            if st.button("Secure Login", use_container_width=True):
                # MASTER PASSWORD HERE:
                if pwd == "Admin2026!": 
                    st.session_state["password_correct"] = True
                    st.rerun()
                else:
                    st.error("❌ Access Denied. Incorrect Password.")
        return False
    return True

# ==========================================
# 4. MAIN APPLICATION
# ==========================================

if check_password():
    st.set_page_config(page_title="Contractor OS", page_icon="⚡", layout="wide")
    
    # --- Sidebar Navigation ---
    st.sidebar.title("⚡ Menu")
    menu = st.sidebar.radio("Navigation", [
        "📊 Executive Dashboard", 
        "👷 Manage Workforce", 
        "🏗️ Manage Clients & Sites", 
        "💰 Log Client Payments", 
        "⏱️ Daily/Weekly Labor Entry", 
        "💸 Log Material Expenses",
        "📈 Update Site Progress"
    ])
    
    st.sidebar.markdown("---")
    if st.sidebar.button("🚪 Secure Logout"):
        st.session_state["password_correct"] = False
        st.rerun()

    # --- Helper Functions ---
    def fetch_data(table):
        """Safely fetches data using Pandas and SQLAlchemy."""
        try:
            return pd.read_sql(f'SELECT * FROM public.{table}', engine)
        except Exception:
            return pd.DataFrame()

    # ==========================================
    # VIEW: EXECUTIVE DASHBOARD
    # ==========================================
    if menu == "📊 Executive Dashboard":
        st.title("📊 Financial & Site Overview")
        st.markdown("---")
        
        clients_df = fetch_data('clients')
        labor_df = fetch_data('labor_logs')
        expenses_df = fetch_data('expenses')
        
        if not clients_df.empty:
            # Top Level Metrics
            total_budget = pd.to_numeric(clients_df['budget'], errors='coerce').sum()
            total_advance = pd.to_numeric(clients_df['advance'], errors='coerce').sum()
            total_labor = pd.to_numeric(labor_df['cost'], errors='coerce').sum() if not labor_df.empty else 0
            total_expenses = pd.to_numeric(expenses_df['amount'], errors='coerce').sum() if not expenses_df.empty else 0
            
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Total Portfolio Budget", f"{total_budget:,.2f} DH")
            c2.metric("Total Advances Received", f"{total_advance:,.2f} DH")
            c3.metric("Total Labor Costs", f"{total_labor:,.2f} DH")
            c4.metric("Total Material Expenses", f"{total_expenses:,.2f} DH")
            
            st.subheader("Client Margins & Active Progress")
            report_data = []
            
            for _, row in clients_df.iterrows():
                c_name = row['client_name']
                c_type = row['work_type']
                c_budget = float(row['budget']) if pd.notna(row['budget']) else 0.0
                
                c_labor = pd.to_numeric(labor_df[labor_df['client_name'] == c_name]['cost'], errors='coerce').sum() if not labor_df.empty else 0
                c_profit = c_budget - c_labor
                c_margin = (c_profit / c_budget * 100) if c_budget > 0 else 0
                
                # Fetch Progress
                prog_df = pd.read_sql(f"SELECT * FROM public.progress WHERE client_name='{c_name}'", engine)
                if not prog_df.empty:
                    v1, v2, v3, v4 = prog_df.iloc[0][['phase1', 'phase2', 'phase3', 'phase4']]
                    if max(v1, v2, v3, v4) <= 1.0 and sum((v1, v2, v3, v4)) > 0:
                        v1, v2, v3, v4 = v1*100, v2*100, v3*100, v4*100
                    completion = (v1 + v2 + v3 + v4) / 4
                else:
                    completion = 0.0
                
                report_data.append({
                    "Client": c_name,
                    "Type": c_type,
                    "Completion": f"{completion:.1f}%",
                    "Budget (DH)": c_budget,
                    "Labor Cost (DH)": float(c_labor),
                    "Profit Margin (DH)": float(c_profit),
                    "Margin (%)": round(c_margin, 2)
                })
                
            st.dataframe(pd.DataFrame(report_data), use_container_width=True, hide_index=True)
        else:
            st.info("No active clients. Go to 'Manage Clients & Sites' to get started.")

    # ==========================================
    # VIEW: MANAGE WORKFORCE
    # ==========================================
    elif menu == "👷 Manage Workforce":
        st.title("👷 Manage Workforce")
        with st.form("add_worker", clear_on_submit=True):
            st.subheader("Add New Electrician / Worker")
            c1, c2 = st.columns(2)
            w_name = c1.text_input("Full Name")
            w_tjm = c2.number_input("Daily Rate (TJM in DH)", min_value=0.0, step=10.0)
            
            if st.form_submit_button("➕ Add Worker to Database"):
                if w_name:
                    conn = get_db_connection()
                    try:
                        with conn.cursor() as c:
                            c.execute("INSERT INTO public.workers (name, tjm) VALUES (%s, %s)", (w_name.strip(), w_tjm))
                        conn.commit()
                        st.success(f"Successfully added {w_name}.")
                    except psycopg2.IntegrityError:
                        st.error("A worker with this name already exists.")
                    finally:
                        conn.close()
        st.markdown("### Active Roster")
        st.dataframe(fetch_data('workers')[['name', 'tjm']], hide_index=True, use_container_width=True)

    # ==========================================
    # VIEW: MANAGE CLIENTS & SITES
    # ==========================================
    elif menu == "🏗️ Manage Clients & Sites":
        st.title("🏗️ Client & Site Management")
        with st.form("add_client", clear_on_submit=True):
            st.subheader("Register New Client")
            c1, c2 = st.columns(2)
            c_name = c1.text_input("Client / Owner Name (e.g., M. Othmane)")
            c_type = c2.selectbox("Type of Work", ["Villa", "Apartment", "House", "Commercial Building", "Maintenance/Repair", "Other"])
            
            c3, c4 = st.columns(2)
            c_budget = c3.number_input("Total Contract Budget (DH)", min_value=0.0, step=1000.0)
            c_avance = c4.number_input("Initial Advance Received (DH)", min_value=0.0, step=1000.0)
            
            if st.form_submit_button("➕ Create Client Profile"):
                if c_name:
                    conn = get_db_connection()
                    try:
                        with conn.cursor() as c:
                            c.execute("INSERT INTO public.clients (client_name, work_type, budget, advance) VALUES (%s, %s, %s, %s)", 
                                      (c_name.strip(), c_type, c_budget, c_avance))
                        conn.commit()
                        st.success(f"Client profile for '{c_name}' ({c_type}) created securely.")
                    except psycopg2.IntegrityError:
                        st.error("This client name already exists.")
                    finally:
                        conn.close()
        st.markdown("### Client Database")
        st.dataframe(fetch_data('clients')[['client_name', 'work_type', 'budget', 'advance']], hide_index=True, use_container_width=True)

    # ==========================================
    # VIEW: LOG PAYMENTS
    # ==========================================
    elif menu == "💰 Log Client Payments":
        st.title("💰 Log New Payments")
        clients = fetch_data('clients')
        if clients.empty: 
            st.warning("Please add a client first.")
        else:
            with st.form("add_payment", clear_on_submit=True):
                st.subheader("Record Incoming Transfer or Cash")
                sel_client = st.selectbox("Select Client", clients['client_name'])
                new_money = st.number_input("Amount Received (DH)", min_value=0.0, step=500.0)
                
                if st.form_submit_button("✅ Process Payment"):
                    if new_money > 0:
                        conn = get_db_connection()
                        try:
                            with conn.cursor() as c:
                                c.execute("UPDATE public.clients SET advance = advance + %s WHERE client_name = %s", (new_money, sel_client))
                            conn.commit()
                            st.balloons()
                            st.success(f"Successfully processed {new_money:,.2f} DH for {sel_client}.")
                        finally:
                            conn.close()
                    else:
                        st.error("Payment amount must be greater than 0.")
            
            st.markdown("### Updated Advance Balances")
            st.dataframe(fetch_data('clients')[['client_name', 'work_type', 'budget', 'advance']], hide_index=True, use_container_width=True)

    # ==========================================
    # VIEW: LABOR ENTRY (BULLETPROOF LOOP)
    # ==========================================
    elif menu == "⏱️ Daily/Weekly Labor Entry":
        st.title("⏱️ Fast Labor Logging")
        workers, clients = fetch_data('workers'), fetch_data('clients')
        
        if workers.empty or clients.empty: 
            st.warning("Ensure you have at least one Worker and one Client registered.")
        else:
            with st.form("labor_entry"):
                c1, c2 = st.columns(2)
                log_date = c1.date_input("Date of Work", date.today())
                sel_client = c2.selectbox("Target Client / Site", clients['client_name'])
                
                st.markdown("### 👷 Select Workers on Site")
                active_workers = st.multiselect("Who worked during this period?", workers['name'].tolist())
                worker_inputs = {}
                
                if active_workers:
                    st.markdown("##### Assign Days Worked")
                    cols = st.columns(min(len(active_workers), 4))
                    for i, w_name in enumerate(active_workers):
                        w_tjm = workers[workers['name'] == w_name]['tjm'].values[0]
                        with cols[i % 4]:
                            worker_inputs[w_name] = st.number_input(f"{w_name} (TJM: {w_tjm})", min_value=0.0, max_value=31.0, step=0.5, value=0.0)
                
                if st.form_submit_button("💾 Save All Labor Records"):
                    if not active_workers:
                        st.error("Please select at least one worker.")
                    else:
                        # PROFESSIONAL TRANSACTION BLOCK
                        logs_added = 0
                        conn = get_db_connection()
                        try:
                            with conn.cursor() as c:
                                for w_name, days in worker_inputs.items():
                                if days > 0:
                                        # Force Pandas numbers into standard Python floats
                                        tjm = float(workers[workers['name'] == w_name]['tjm'].values[0])
                                        clean_days = float(days)
                                        clean_cost = float(clean_days * tjm)
                                        
                                        c.execute('''INSERT INTO public.labor_logs 
                                                     (date, client_name, worker_name, days, cost) 
                                                     VALUES (%s, %s, %s, %s, %s)''', 
                                                  (log_date, sel_client, w_name, clean_days, clean_cost))
                                        logs_added += 1
                            conn.commit() # Commit all inserts together securely
                            if logs_added > 0:
                                st.success(f"Successfully secured {logs_added} labor records for {sel_client}.")
                            else:
                                st.warning("No time logged (all selected workers were set to 0 days).")
                        except Exception as e:
                            conn.rollback()
                            st.error(f"Database error: {e}")
                        finally:
                            conn.close()
                            
            st.markdown("### Recent Log History")
            st.dataframe(pd.read_sql('SELECT date, client_name, worker_name, days, cost FROM public.labor_logs ORDER BY id DESC LIMIT 10', engine), hide_index=True, use_container_width=True)

    # ==========================================
    # VIEW: LOG EXPENSES
    # ==========================================
    elif menu == "💸 Log Material Expenses":
        st.title("💸 Materials & Logistics Expenses")
        clients = fetch_data('clients')
        if clients.empty: 
            st.warning("Please add a client first.")
        else:
            with st.form("log_expense", clear_on_submit=True):
                exp_date = st.date_input("Date of Purchase", date.today())
                sel_client = st.selectbox("Assign to Client", ["General/Stock"] + list(clients['client_name']))
                item_desc = st.text_input("Receipt / Item Description")
                amount = st.number_input("Total Cost (DH)", min_value=0.0, step=50.0)
                
                if st.form_submit_button("🧾 Record Expense"):
                    if item_desc and amount > 0:
                        conn = get_db_connection()
                        try:
                            with conn.cursor() as c:
                                c.execute("INSERT INTO public.expenses (date, client_name, item, amount) VALUES (%s, %s, %s, %s)", 
                                          (exp_date, sel_client, item_desc, amount))
                            conn.commit()
                            st.success("Expense recorded successfully.")
                        finally:
                            conn.close()
                    else:
                        st.error("Please provide a description and amount.")
            
            st.markdown("### Recent Expenses")
            st.dataframe(pd.read_sql('SELECT date, client_name, item, amount FROM public.expenses ORDER BY id DESC LIMIT 10', engine), hide_index=True, use_container_width=True)

    # ==========================================
    # VIEW: UPDATE PROGRESS
    # ==========================================
    elif menu == "📈 Update Site Progress":
        st.title("📈 Technical Site Progress")
        clients = fetch_data('clients')
        if clients.empty: 
            st.warning("Please add a client first.")
        else:
            sel_client = st.selectbox("Select Client / Site", clients['client_name'])
            prog_df = pd.read_sql(f"SELECT * FROM public.progress WHERE client_name='{sel_client}'", engine)
            
            v1, v2, v3, v4 = 0.0, 0.0, 0.0, 0.0
            if not prog_df.empty:
                v1, v2, v3, v4 = prog_df.iloc[0][['phase1', 'phase2', 'phase3', 'phase4']]
                if max(v1, v2, v3, v4) <= 1.0 and sum((v1, v2, v3, v4)) > 0:
                    v1, v2, v3, v4 = v1*100, v2*100, v3*100, v4*100

            with st.form("update_progress"):
                st.write(f"Adjust electrical phase completion for **{sel_client}**")
                p1 = st.slider("1. Installation et raccordement des gaines (%)", 0, 100, int(v1), 5)
                p2 = st.slider("2. Installation du tableau électrique (%)", 0, 100, int(v2), 5)
                p3 = st.slider("3. Pose des appareillages (%)", 0, 100, int(v3), 5)
                p4 = st.slider("4. Branchements et mise en service (%)", 0, 100, int(v4), 5)
                
                total_avg = (p1 + p2 + p3 + p4) / 4
                st.info(f"🏆 **Global Completion Target:** {total_avg:.1f}%")
                
                if st.form_submit_button("💾 Lock in Progress"):
                    conn = get_db_connection()
                    try:
                        with conn.cursor() as c:
                            c.execute('''INSERT INTO public.progress (client_name, phase1, phase2, phase3, phase4) 
                                         VALUES (%s, %s, %s, %s, %s)
                                         ON CONFLICT (client_name) 
                                         DO UPDATE SET phase1=EXCLUDED.phase1, phase2=EXCLUDED.phase2, 
                                                       phase3=EXCLUDED.phase3, phase4=EXCLUDED.phase4''', 
                                      (sel_client, p1, p2, p3, p4))
                        conn.commit()
                        st.success(f"Progress locked for {sel_client}.")
                    finally:
                        conn.close()
