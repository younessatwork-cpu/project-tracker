import streamlit as st
import pandas as pd
import sqlite3
from datetime import date

# --- 1. DATABASE SETUP ---
conn = sqlite3.connect('business_data.db', check_same_thread=False)
c = conn.cursor()

def create_tables():
    c.execute('''CREATE TABLE IF NOT EXISTS workers 
                 (id INTEGER PRIMARY KEY, name TEXT UNIQUE, tjm REAL)''')
    c.execute('''CREATE TABLE IF NOT EXISTS projects 
                 (id INTEGER PRIMARY KEY, client_name TEXT UNIQUE, budget REAL, advance REAL)''')
    c.execute('''CREATE TABLE IF NOT EXISTS labor_logs 
                 (id INTEGER PRIMARY KEY, date TEXT, project_name TEXT, worker_name TEXT, days REAL, cost REAL)''')
    c.execute('''CREATE TABLE IF NOT EXISTS expenses 
                 (id INTEGER PRIMARY KEY, date TEXT, project_name TEXT, item TEXT, amount REAL)''')
    c.execute('''CREATE TABLE IF NOT EXISTS progress 
                 (project_name TEXT PRIMARY KEY, phase1 REAL, phase2 REAL, phase3 REAL, phase4 REAL)''')
    conn.commit()

create_tables()

# --- Page Config & Navigation ---
st.set_page_config(page_title="Contractor OS", layout="wide")
st.sidebar.title("⚙️ System Menu")
menu = st.sidebar.radio("Navigate", 
                        ["📊 Master Dashboard", 
                         "🔍 Project Analytics",  # <-- NEW MENU ITEM
                         "👷 Manage Team", 
                         "🏗️ Manage Projects", 
                         "⏱️ Weekly Labor Entry", # <-- UPGRADED MENU ITEM
                         "💸 Log Expenses",
                         "📈 Update Progress",
                         "✏️ Edit / Delete Data"])

# --- Helper Functions ---
def get_all_workers(): return pd.read_sql('SELECT * FROM workers', conn)
def get_all_projects(): return pd.read_sql('SELECT * FROM projects', conn)

# --- VIEW 1: MASTER DASHBOARD ---
if menu == "📊 Master Dashboard":
    st.title("📊 Financial & Project Dashboard")
    st.markdown("---")
    
    projects_df = get_all_projects()
    labor_df = pd.read_sql('SELECT * FROM labor_logs', conn)
    expenses_df = pd.read_sql('SELECT * FROM expenses', conn)
    
    if not projects_df.empty:
        # Force all columns to be strictly numeric before doing any math
        total_budget = pd.to_numeric(projects_df['budget'], errors='coerce').sum()
        total_advance = pd.to_numeric(projects_df['advance'], errors='coerce').sum()
        total_labor = pd.to_numeric(labor_df['cost'], errors='coerce').sum() if not labor_df.empty else 0
        total_expenses = pd.to_numeric(expenses_df['amount'], errors='coerce').sum() if not expenses_df.empty else 0
        
        c1, c2, c3, c4 = st.columns(4)
        # We also wrap the final variables in float() just to be absolutely certain
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
            
            prog_df = pd.read_sql(f"SELECT * FROM progress WHERE project_name='{p_name}'", conn)
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

# --- VIEW 2: PROJECT ANALYTICS (NEW) ---
elif menu == "🔍 Project Analytics":
    st.title("🔍 Project Labor Analytics")
    st.write("Track exactly how many workers and total man-days are going into each project.")
    st.markdown("---")
    
    labor_df = pd.read_sql('SELECT * FROM labor_logs', conn)
    
    if not labor_df.empty:
        # Group data by project
        summary_df = labor_df.groupby('project_name').agg(
            Total_Man_Days=('days', 'sum'),
            Total_Labor_Cost=('cost', 'sum'),
            Unique_Workers=('worker_name', 'nunique')
        ).reset_index()
        
        st.subheader("High-Level Project Resource Summary")
        st.dataframe(summary_df, use_container_width=True)
        
        st.markdown("---")
        st.subheader("Deep Dive: Who worked where?")
        
        # Select a project to see exactly who worked on it
        selected_project = st.selectbox("Select a Project to see worker breakdown:", summary_df['project_name'])
        
        if selected_project:
            proj_data = labor_df[labor_df['project_name'] == selected_project]
            worker_breakdown = proj_data.groupby('worker_name').agg(
                Total_Days=('days', 'sum'),
                Total_Cost=('cost', 'sum')
            ).reset_index()
            
            c1, c2 = st.columns([1, 2])
            with c1:
                st.dataframe(worker_breakdown, hide_index=True, use_container_width=True)
            with c2:
                st.bar_chart(worker_breakdown.set_index('worker_name')['Total_Days'])
    else:
        st.info("No labor data logged yet.")

# --- VIEW 3: MANAGE TEAM ---
elif menu == "👷 Manage Team":
    st.title("👷 Manage Workforce")
    with st.form("add_worker"):
        w_name = st.text_input("Worker Name")
        w_tjm = st.number_input("Daily Rate (TJM in DH)", min_value=0.0, step=10.0)
        if st.form_submit_button("Add Worker") and w_name:
            try:
                c.execute("INSERT INTO workers (name, tjm) VALUES (?, ?)", (w_name, w_tjm))
                conn.commit()
                st.success(f"Added {w_name}!")
            except sqlite3.IntegrityError:
                st.error("Worker already exists!")
    st.dataframe(get_all_workers(), hide_index=True)

# --- VIEW 4: MANAGE PROJECTS ---
elif menu == "🏗️ Manage Projects":
    st.title("🏗️ Manage Projects")
    with st.form("add_project"):
        p_name = st.text_input("Client / Project Name")
        p_budget = st.number_input("Total Budget (DH)", min_value=0.0, step=1000.0)
        p_avance = st.number_input("Advance Received (DH)", min_value=0.0, step=1000.0)
        if st.form_submit_button("Create Project") and p_name:
            try:
                c.execute("INSERT INTO projects (client_name, budget, advance) VALUES (?, ?, ?)", (p_name, p_budget, p_avance))
                conn.commit()
                st.success(f"Project '{p_name}' created!")
            except sqlite3.IntegrityError:
                st.error("Project already exists!")
    st.dataframe(get_all_projects(), hide_index=True)

# --- VIEW 5: FAST LABOR ENTRY (UPGRADED) ---
elif menu == "⏱️ Weekly Labor Entry":
    st.title("⏱️ Fast Labor Entry")
    st.write("1. Select the project. 2. Pick who worked. 3. Enter their days.")
    
    workers, projects = get_all_workers(), get_all_projects()
    if workers.empty or projects.empty: 
        st.warning("Add Workers and Projects first.")
    else:
        with st.form("fast_labor_entry"):
            c1, c2 = st.columns(2)
            log_date = c1.date_input("Week Starting (or Date of Work)", date.today())
            sel_project = c2.selectbox("Select Project", projects['client_name'])
            
            st.markdown("### Who was on site?")
            # Select the active workers
            active_workers = st.multiselect("Click to select workers for this project:", workers['name'].tolist())
            
            worker_inputs = {}
            
            # Only show input boxes if you selected someone
            if active_workers:
                st.markdown("### Enter Days Worked")
                # Dynamically create columns based on how many people you selected (max 4 wide)
                cols = st.columns(min(len(active_workers), 4) if len(active_workers) > 0 else 1)
                
                for i, w_name in enumerate(active_workers):
                    w_tjm = workers[workers['name'] == w_name]['tjm'].values[0]
                    with cols[i % 4]:
                        # FIX: Default value is now exactly 0.0. Minimum value is 0.0.
                        worker_inputs[w_name] = st.number_input(f"{w_name} (TJM: {w_tjm})", min_value=0.0, max_value=31.0, step=0.5, value=0.0)
            
            # The submit button
            submit_button = st.form_submit_button("💾 Save Labor Logs")
            
            if submit_button:
                if not active_workers:
                    st.error("Please select at least one worker from the list above before saving.")
                else:
                    logs_added = 0
                    for w_name, days_worked in worker_inputs.items():
                        # FIX: Only save to database if days_worked is greater than 0
                        if days_worked > 0:
                            tjm = workers[workers['name'] == w_name]['tjm'].values[0]
                            total_cost = days_worked * tjm
                            c.execute("INSERT INTO labor_logs (date, project_name, worker_name, days, cost) VALUES (?, ?, ?, ?, ?)",
                                      (log_date, sel_project, w_name, days_worked, total_cost))
                            logs_added += 1
                            
                    if logs_added > 0:
                        conn.commit()
                        st.success(f"Successfully logged {logs_added} active workers for {sel_project}!")
                    else:
                        st.warning("No logs were saved because all selected workers had 0 days worked.")
                
        st.subheader("Recent Labor Logs")
        st.dataframe(pd.read_sql('SELECT date, project_name, worker_name, days, cost FROM labor_logs ORDER BY id DESC LIMIT 10', conn), hide_index=True)
# --- VIEW 6: LOG EXPENSES ---
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
                c.execute("INSERT INTO expenses (date, project_name, item, amount) VALUES (?, ?, ?, ?)", (exp_date, sel_project, item_desc, amount))
                conn.commit()
                st.success("Expense logged.")
        st.dataframe(pd.read_sql('SELECT * FROM expenses ORDER BY id DESC LIMIT 10', conn), hide_index=True)

# --- VIEW 7: UPDATE PROGRESS ---
elif menu == "📈 Update Progress":
    st.title("📈 Technical Progress Tracker")
    projects = get_all_projects()
    if projects.empty: st.warning("Add Projects first.")
    else:
        sel_project = st.selectbox("Select Project to Update", projects['client_name'])
        prog_df = pd.read_sql(f"SELECT * FROM progress WHERE project_name='{sel_project}'", conn)
        
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
                c.execute('''INSERT OR REPLACE INTO progress (project_name, phase1, phase2, phase3, phase4) 
                             VALUES (?, ?, ?, ?, ?)''', (sel_project, p1, p2, p3, p4))
                conn.commit()
                st.success("Progress saved!")

# --- VIEW 8: THE DATA MANAGER ---
elif menu == "✏️ Edit / Delete Data":
    st.title("✏️ Database Manager")
    table_to_edit = st.selectbox("Select Table to Edit:", ["labor_logs", "expenses", "projects", "workers"])
    df = pd.read_sql(f"SELECT * FROM {table_to_edit}", conn)
    
    with st.form("editor_form"):
        edited_df = st.data_editor(df, num_rows="dynamic", use_container_width=True)
        if st.form_submit_button("💾 Save All Changes to Database"):
            edited_df.to_sql(table_to_edit, conn, if_exists='replace', index=False)
            st.success(f"Successfully updated the '{table_to_edit}' table!")