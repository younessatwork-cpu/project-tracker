import streamlit as st
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from datetime import date

# --- 1. CLOUD DATABASE SETUP ---
DB_URL = st.secrets["DATABASE_URL"]

if DB_URL.startswith("postgres://"):
    DB_URL = DB_URL.replace("postgres://", "postgresql://", 1)

# FORCE SSL ENCRYPTION
if "?" not in DB_URL:
    DB_URL += "?sslmode=require"
elif "sslmode=" not in DB_URL:
    DB_URL += "&sslmode=require"

# Create an engine for reading data cleanly
engine = create_engine(DB_URL)

def get_db():
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = True
    return conn

# Create tables explicitly in the 'public' schema
def create_tables():
    conn = get_db()
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS public.workers (id SERIAL PRIMARY KEY, name TEXT UNIQUE, tjm REAL)''')
    c.execute('''CREATE TABLE IF NOT EXISTS public.projects (id SERIAL PRIMARY KEY, client_name TEXT UNIQUE, budget REAL, advance REAL)''')
    c.execute('''CREATE TABLE IF NOT EXISTS public.labor_logs (id SERIAL PRIMARY KEY, date TEXT, project_name TEXT, worker_name TEXT, days REAL, cost REAL)''')
    c.execute('''CREATE TABLE IF NOT EXISTS public.expenses (id SERIAL PRIMARY KEY, date TEXT, project_name TEXT, item TEXT, amount REAL)''')
    c.execute('''CREATE TABLE IF NOT EXISTS public.progress (project_name TEXT PRIMARY KEY, phase1 REAL, phase2 REAL, phase3 REAL, phase4 REAL)''')
    conn.close()

create_tables()

# --- 2. THE GATEKEEPER (LOGIN SCREEN) ---
def check_password():
    if "password_correct" not in st.session_state:
        st.session_state["password_correct"] = False

    if not st.session_state["password_correct"]:
        st.markdown("<h1 style='text-align: center;'>🔒 Contractor OS Login</h1>", unsafe_allow_html=True)
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            st.info("Please enter the master password to access the system.")
            pwd = st.text_input("Password", type="password")
            
            if st.button("Login"):
                if pwd == "Admin2026!": 
                    st.session_state["password_correct"] = True
                    st.rerun()
                else:
                    st.error("❌ Incorrect Password.")
        return False
    return True

# --- 3. RUN THE APP ---
if check_password():
    st.set_page_config(page_title="Contractor OS", layout="wide")
    st.sidebar.title("⚙️ System Menu")
    
    if st.sidebar.button("🚪 Logout"):
        st.session_state["password_correct"] = False
        st.rerun()
        
    menu = st.sidebar.radio("Navigate", 
                            ["📊 Master Dashboard", 
                             "🔍 Project Analytics", 
                             "👷 Manage Team", 
                             "🏗️ Manage Projects", 
                             "💰 Log Payments", 
                             "⏱️ Fast Labor Entry", 
                             "💸 Log Expenses",
                             "📈 Update Progress"])

    # --- Helper Functions (Explicitly querying the public schema) ---
    def get_all_workers(): return pd.read_sql('SELECT * FROM public.workers', engine)
    def get_all_projects(): return pd.read_sql('SELECT * FROM public.projects', engine)

    # --- VIEW 1: MASTER DASHBOARD ---
    if menu == "📊 Master Dashboard":
        st.title("📊 Financial & Project Dashboard")
        st.markdown("---")
        
        projects_df = get_all_projects()
        labor_df = pd.read_sql('SELECT * FROM public.labor_logs', engine)
        expenses_df = pd.read_sql('SELECT * FROM public.expenses', engine)
        
        if not projects_df.empty:
            total_budget = pd.to_numeric(projects_df['budget'], errors='coerce').sum()
            total_advance = pd.to_numeric(projects_df['advance'], errors='coerce').sum()
            total_labor = pd.to_numeric(labor_df['cost'], errors='coerce').sum() if not labor_df.empty else 0
            total_expenses = pd.to_numeric(expenses_df['amount'], errors='coerce').sum() if not expenses_df.empty else 0
            
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Total Portfolio Budget", f"{float(total_budget):,.2f} DH")
            c2.metric("Total Advances Collected", f"{float(total_advance):,.2f} DH")
            c3.metric("Total Labor Spent", f"{float(total_labor):,.2f} DH")
            c4.metric("Total Materials/Expenses", f"{float(total_expenses):,.2f} DH")
            
            st.subheader("Active Project Margins & Progress")
            
            report_data = []
            for index, row in projects_df.iterrows():
                p_name = row['client_name']
                p_budget = float(row['budget']) if pd.notna(row['budget']) else 0.0
                
                p_labor = pd.to_numeric(labor_df[labor_df['project_name'] == p_name]['cost'], errors='coerce').sum() if not labor_df.empty else 0
                profit = p_budget - p_labor
                margin = (profit / p_budget * 100) if p_budget > 0 else 0
                
                prog_df = pd.read_sql(f"SELECT * FROM public.progress WHERE project_name='{p_name}'", engine)
                if not prog_df.empty:
                    v1, v2, v3, v4 = prog_df['phase1'].values[0], prog_df['phase2'].values[0], prog_df['phase3'].values[0], prog_df['phase4'].values[0]
                    if max(v1, v2, v3, v4) <= 1.0 and sum((v1, v2, v3, v4)) > 0:
                        v1, v2, v3, v4 = v1*100, v2*100, v3*100, v4*100
                    completion = (v1 + v2 + v3 + v4) / 4
                else:
                    completion = 0.0
                
                report_data.append({
                    "Project": p_name,
                    "Completion": f"{completion:.1f}%",
                    "Budget": p_budget,
                    "Total Labor Cost": float(p_labor),
                    "Current Profit": float(profit),
                    "Margin (%)": round(margin, 2)
                })
                
            st.dataframe(pd.DataFrame(report_data), use_container_width=True)
        else:
            st.info("No projects yet. Go to 'Manage Projects' to start.")

    # --- VIEW 2: PROJECT ANALYTICS ---
    elif menu == "🔍 Project Analytics":
        st.title("🔍 Project Labor Analytics")
        labor_df = pd.read_sql('SELECT * FROM public.labor_logs', engine)
        if not labor_df.empty:
            summary_df = labor_df.groupby('project_name').agg(
                Total_Man_Days=('days', 'sum'), Total_Labor_Cost=('cost', 'sum'), Unique_Workers=('worker_name', 'nunique')
            ).reset_index()
            st.dataframe(summary_df, use_container_width=True)
            
            selected_project = st.selectbox("Select a Project to see worker breakdown:", summary_df['project_name'])
            if selected_project:
                proj_data = labor_df[labor_df['project_name'] == selected_project]
                worker_breakdown = proj_data.groupby('worker_name').agg(Total_Days=('days', 'sum'), Total_Cost=('cost', 'sum')).reset_index()
                c1, c2 = st.columns([1, 2])
                with c1: st.dataframe(worker_breakdown, hide_index=True, use_container_width=True)
                with c2: st.bar_chart(worker_breakdown.set_index('worker_name')['Total_Days'])
        else:
            st.info("No labor data logged yet.")

    # --- VIEW 3: MANAGE TEAM ---
    elif menu == "👷 Manage Team":
        st.title("👷 Manage Workforce")
        with st.form("add_worker"):
            w_name = st.text_input("Worker Name")
            w_tjm = st.number_input("Daily Rate (TJM in DH)", min_value=0.0, step=10.0)
            if st.form_submit_button("Add Worker") and w_name:
                conn = get_db()
                c = conn.cursor()
                try:
                    c.execute("INSERT INTO public.workers (name, tjm) VALUES (%s, %s)", (w_name, w_tjm))
                    st.success(f"Added {w_name}!")
                except psycopg2.IntegrityError:
                    st.error("Worker already exists!")
                finally: conn.close()
        st.dataframe(get_all_workers(), hide_index=True)

    # --- VIEW 4: MANAGE PROJECTS ---
    elif menu == "🏗️ Manage Projects":
        st.title("🏗️ Manage Projects")
        with st.form("add_project"):
            p_name = st.text_input("Client / Project Name")
            p_budget = st.number_input("Total Budget (DH)", min_value=0.0, step=1000.0)
            p_avance = st.number_input("Advance Received (DH)", min_value=0.0, step=1000.0)
            if st.form_submit_button("Create Project") and p_name:
                conn = get_db()
                c = conn.cursor()
                try:
                    c.execute("INSERT INTO public.projects (client_name, budget, advance) VALUES (%s, %s, %s)", (p_name, p_budget, p_avance))
                    st.success(f"Project '{p_name}' created!")
                except psycopg2.IntegrityError:
                    st.error("Project already exists!")
                finally: conn.close()
        st.dataframe(get_all_projects(), hide_index=True)

    # --- VIEW 5: LOG PAYMENTS ---
    elif menu == "💰 Log Payments":
        st.title("💰 Log New Payments")
        st.write("Did a client just hand you cash or a transfer? Log it here instantly.")
        projects = get_all_projects()
        
        if projects.empty: 
            st.warning("You need to create a project first.")
        else:
            with st.form("add_payment"):
                st.subheader("💵 Add Money Received")
                sel_project = st.selectbox("1. Which project is paying you?", projects['client_name'])
                new_money = st.number_input("2. How much money did you receive right now? (DH)", min_value=0.0, step=500.0, value=0.0)
                
                if st.form_submit_button("✅ Save Payment to Database"):
                    if new_money > 0:
                        conn = get_db()
                        c = conn.cursor()
                        c.execute("UPDATE public.projects SET advance = advance + %s WHERE client_name = %s", (new_money, sel_project))
                        conn.close()
                        st.balloons()
                        st.success(f"Awesome! Successfully added {new_money:,.2f} DH to {sel_project}.")
                    else:
                        st.error("Please enter an amount greater than 0.")
            
            st.markdown("---")
            st.subheader("Current Total Advances by Project")
            updated_projects = get_all_projects()
            st.dataframe(updated_projects[['client_name', 'budget', 'advance']], hide_index=True, use_container_width=True)

    # --- VIEW 6: FAST LABOR ENTRY ---
    elif menu == "⏱️ Fast Labor Entry":
        st.title("⏱️ Fast Labor Entry")
        workers, projects = get_all_workers(), get_all_projects()
        if workers.empty or projects.empty: 
            st.warning("Add Workers and Projects first.")
        else:
            with st.form("fast_labor_entry"):
                c1, c2 = st.columns(2)
                log_date = c1.date_input("Week Starting (or Date of Work)", date.today())
                sel_project = c2.selectbox("Select Project", projects['client_name'])
                
                active_workers = st.multiselect("Click to select workers for this project:", workers['name'].tolist())
                worker_inputs = {}
                
                if active_workers:
                    cols = st.columns(min(len(active_workers), 4) if len(active_workers) > 0 else 1)
                    for i, w_name in enumerate(active_workers):
                        w_tjm = workers[workers['name'] == w_name]['tjm'].values[0]
                        with cols[i % 4]:
                            worker_inputs[w_name] = st.number_input(f"{w_name} (TJM: {w_tjm})", min_value=0.0, max_value=31.0, step=0.5, value=0.0)
                
                if st.form_submit_button("💾 Save Labor Logs"):
                    if not active_workers:
                        st.error("Select at least one worker.")
                    else:
                        conn = get_db()
                        c = conn.cursor()
                        logs_added = 0
                        for w_name, days_worked in worker_inputs.items():
                            if days_worked > 0:
                                tjm = workers[workers['name'] == w_name]['tjm'].values[0]
                                c.execute("INSERT INTO public.labor_logs (date, project_name, worker_name, days, cost) VALUES (%s, %s, %s, %s, %s)",
                                          (log_date, sel_project, w_name, days_worked, days_worked * tjm))
                                logs_added += 1
                        conn.close()
                        if logs_added > 0: st.success(f"Logged {logs_added} workers for {sel_project}!")
                        else: st.warning("No logs saved (all were 0 days).")
            st.subheader("Recent Logs")
            st.dataframe(pd.read_sql('SELECT date, project_name, worker_name, days, cost FROM public.labor_logs ORDER BY id DESC LIMIT 10', engine), hide_index=True)

    # --- VIEW 7: LOG EXPENSES ---
    elif menu == "💸 Log Expenses":
        st.title("💸 Log Material Expenses")
        projects = get_all_projects()
        if projects.empty: st.warning("Add Projects first.")
        else:
            with st.form("log_expense"):
                exp_date = st.date_input("Date of Purchase", date.today())
                sel_project = st.selectbox("Select Project", ["General/Stock"] + list(projects['client_name']))
                item_desc = st.text_input("Item Description")
                amount = st.number_input("Total Cost (DH)", min_value=0.0, step=50.0)
                if st.form_submit_button("Log Expense") and item_desc and amount > 0:
                    conn = get_db()
                    c = conn.cursor()
                    c.execute("INSERT INTO public.expenses (date, project_name, item, amount) VALUES (%s, %s, %s, %s)", (exp_date, sel_project, item_desc, amount))
                    conn.close()
                    st.success("Expense logged.")
            st.dataframe(pd.read_sql('SELECT * FROM public.expenses ORDER BY id DESC LIMIT 10', engine), hide_index=True)

    # --- VIEW 8: UPDATE PROGRESS ---
    elif menu == "📈 Update Progress":
        st.title("📈 Technical Progress Tracker")
        projects = get_all_projects()
        if projects.empty: st.warning("Add Projects first.")
        else:
            sel_project = st.selectbox("Select Project to Update", projects['client_name'])
            prog_df = pd.read_sql(f"SELECT * FROM public.progress WHERE project_name='{sel_project}'", engine)
            
            v1, v2, v3, v4 = 0.0, 0.0, 0.0, 0.0
            if not prog_df.empty:
                v1, v2, v3, v4 = prog_df['phase1'].values[0], prog_df['phase2'].values[0], prog_df['phase3'].values[0], prog_df['phase4'].values[0]
                if max(v1, v2, v3, v4) <= 1.0 and sum((v1, v2, v3, v4)) > 0:
                    v1, v2, v3, v4 = v1*100, v2*100, v3*100, v4*100

            with st.form("update_progress"):
                p1 = st.slider("1. Installation et raccordement (%)", 0, 100, int(v1), 5)
                p2 = st.slider("2. Installation du tableau électrique (%)", 0, 100, int(v2), 5)
                p3 = st.slider("3. Pose des appareillages (%)", 0, 100, int(v3), 5)
                p4 = st.slider("4. Branchements et mise en service (%)", 0, 100, int(v4), 5)
                
                total_avg = (p1 + p2 + p3 + p4) / 4
                st.info(f"🏆 **Overall Project Completion:** {total_avg:.1f}%")
                
                if st.form_submit_button("Save Progress"):
                    conn = get_db()
                    c = conn.cursor()
                    c.execute('''INSERT INTO public.progress (project_name, phase1, phase2, phase3, phase4) 
                                 VALUES (%s, %s, %s, %s, %s)
                                 ON CONFLICT (project_name) 
                                 DO UPDATE SET phase1=EXCLUDED.phase1, phase2=EXCLUDED.phase2, 
                                               phase3=EXCLUDED.phase3, phase4=EXCLUDED.phase4''', 
                              (sel_project, p1, p2, p3, p4))
                    conn.close()
                    st.success("Progress saved!")
