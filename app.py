import streamlit as st
import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
from datetime import date, timedelta
import base64
import os
import time
import urllib.parse
import hashlib
import re
import json
import io
from fpdf import FPDF
from streamlit_option_menu import option_menu

# ══════════════════════════════════════════════════════════════
# CONFIG & PAGE SETUP
# ══════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="Newlightemara OS",
    page_icon="💡",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    [data-testid="stAppViewContainer"] { background: #f8fafc; }
    [data-testid="stSidebar"] { background: #111827 !important; }
    [data-testid="stSidebar"] * { color: #e5e7eb !important; }
    [data-testid="stSidebarContent"] { padding: 0 !important; }
    input[type="number"] { -moz-appearance: textfield; }
    input[type="number"]::-webkit-inner-spin-button,
    input[type="number"]::-webkit-outer-spin-button { -webkit-appearance: none; margin: 0; }
    [data-testid="stMetric"] {
        background: #ffffff;
        border: 1px solid #e2e8f0;
        border-radius: 10px;
        padding: 16px 20px !important;
        box-shadow: 0 1px 3px rgba(0,0,0,0.06);
    }
    [data-testid="stMetricLabel"] { font-size: 12px !important; color: #64748b !important; font-weight: 600 !important; }
    [data-testid="stMetricValue"] { font-size: 24px !important; font-weight: 800 !important; }
    .stButton > button {
        border-radius: 8px !important;
        font-weight: 600 !important;
        font-size: 13px !important;
        transition: all .15s !important;
    }
    .stButton > button:hover { transform: translateY(-1px); box-shadow: 0 4px 12px rgba(0,0,0,0.1); }
    .stTabs [data-baseweb="tab-list"] { background: #f1f5f9; border-radius: 10px; padding: 4px; gap: 4px; }
    .stTabs [data-baseweb="tab"] { border-radius: 7px !important; font-weight: 600 !important; font-size: 13px !important; }
    .stTabs [aria-selected="true"] { background: #ffffff !important; color: #1a56db !important; box-shadow: 0 1px 3px rgba(0,0,0,0.1) !important; }
    [data-testid="stDataFrame"] { border-radius: 10px; overflow: hidden; border: 1px solid #e2e8f0; }
    [data-testid="stExpander"] { border: 1px solid #e2e8f0 !important; border-radius: 10px !important; background: #fff !important; }
    [data-testid="stExpanderDetails"] { background: #fff !important; }
    [data-testid="stForm"] { background: #ffffff; border: 1px solid #e2e8f0; border-radius: 12px; padding: 20px !important; }
    .stSelectbox > div > div, .stTextInput > div > div { border-radius: 7px !important; }
    hr { border-color: #e2e8f0 !important; }
    .stAlert { border-radius: 10px !important; }
    h1 { font-size: 22px !important; font-weight: 800 !important; color: #0f172a !important; }
    h2 { font-size: 17px !important; font-weight: 700 !important; color: #1e293b !important; }
    h3 { font-size: 14px !important; font-weight: 700 !important; color: #334155 !important; }
    .sidebar-brand {
        padding: 20px 16px 14px;
        border-bottom: 1px solid rgba(255,255,255,.08);
        margin-bottom: 8px;
    }
    .badge {
        display: inline-block; padding: 3px 10px; border-radius: 20px;
        font-size: 11px; font-weight: 700; letter-spacing: .3px;
    }
    .badge-green  { background: #dcfce7; color: #15803d; }
    .badge-red    { background: #fee2e2; color: #b91c1c; }
    .badge-blue   { background: #dbeafe; color: #1d4ed8; }
    .badge-amber  { background: #fef3c7; color: #b45309; }
    .badge-purple { background: #ede9fe; color: #5b21b6; }
    .badge-gray   { background: #f3f4f6; color: #4b5563; }
    .kpi-card {
        background: #fff; border: 1px solid #e2e8f0; border-radius: 10px;
        padding: 16px 20px; border-left: 4px solid var(--accent, #1a56db);
    }
    .phase-Incorporation { background: #ede9fe; color: #5b21b6; }
    .phase-Tirage        { background: #dbeafe; color: #1e40af; }
    .phase-Appareillage  { background: #fef3c7; color: #92400e; }
    .phase-Tableau       { background: #dcfce7; color: #065f46; }
</style>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════
# CONSTANTS
# ══════════════════════════════════════════════════════════════
PHASES = ["Incorporation", "Tirage", "Appareillage", "Tableau"]
WORK_TYPES = ["Full Electrical", "Tableau", "Tirage", "Appareillage", "VDI", "Domotique"]
PAYMENT_METHODS = ["Virement bancaire", "Chèque", "Espèces", "Autre"]
UNITS = ["pcs", "m", "kg", "boîte", "rouleau"]
CATEGORIES = ["Câblage", "Protection", "Appareillage", "Conduit", "Tableau", "Autre"]

PHASE_COLORS = {
    "Incorporation": ("badge-purple", "#7c3aed"),
    "Tirage":        ("badge-blue",   "#1a56db"),
    "Appareillage":  ("badge-amber",  "#d97706"),
    "Tableau":       ("badge-green",  "#16a34a"),
}

# ══════════════════════════════════════════════════════════════
# DATABASE
# ══════════════════════════════════════════════════════════════
def _build_db_url() -> str:
    """Build database URL from secrets with SSL enforcement."""
    try:
        raw = st.secrets["DATABASE_URL"]
    except KeyError:
        st.error("DATABASE_URL not found in secrets. Please configure it in Streamlit Cloud settings.")
        st.stop()
    raw = re.sub(r"^postgres://", "postgresql://", raw)
    raw = raw.replace(":6543", ":5432")
    if "?" not in raw:
        raw += "?sslmode=require"
    elif "sslmode=" not in raw:
        raw += "&sslmode=require"
    return raw


@st.cache_resource
def get_engine():
    """Get SQLAlchemy engine with connection pooling."""
    return create_engine(_build_db_url(), pool_pre_ping=True, pool_size=5, max_overflow=10)


def get_conn():
    """Get fresh psycopg2 connection."""
    return psycopg2.connect(_build_db_url())


def run_query(sql: str, params=None) -> pd.DataFrame:
    """Execute SELECT query and return DataFrame."""
    try:
        with get_engine().connect() as conn:
            return pd.read_sql(text(sql), conn, params=params)
    except Exception as e:
        st.error(f"Database query error: {e}")
        return pd.DataFrame()


def execute(sql: str, params=None):
    """Execute write query with transaction support."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        st.error(f"Database error: {e}")
        return False
    finally:
        conn.close()


def hash_password(pw: str) -> str:
    """Hash password using SHA-256 with salt."""
    salt = st.secrets.get("PASSWORD_SALT", "Newlightemara2026")
    return hashlib.sha256((pw + salt).encode()).hexdigest()


def init_db():
    """Initialize database schema with all tables and seed admin."""
    ddl = [
        """CREATE TABLE IF NOT EXISTS public.workers (
            id SERIAL PRIMARY KEY,
            name TEXT UNIQUE NOT NULL,
            tjm REAL NOT NULL DEFAULT 0,
            specialty TEXT DEFAULT 'Général',
            active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
        """CREATE TABLE IF NOT EXISTS public.clients (
            id SERIAL PRIMARY KEY,
            client_name TEXT UNIQUE NOT NULL,
            work_type TEXT DEFAULT '',
            budget REAL DEFAULT 0,
            advance REAL DEFAULT 0,
            total_points REAL DEFAULT 0,
            status TEXT DEFAULT 'active',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            archived BOOLEAN DEFAULT FALSE
        )""",
        """CREATE TABLE IF NOT EXISTS public.labor_logs (
            id SERIAL PRIMARY KEY,
            date TEXT NOT NULL,
            client_name TEXT NOT NULL,
            worker_name TEXT NOT NULL,
            days REAL NOT NULL,
            cost REAL NOT NULL,
            phase TEXT DEFAULT 'Général',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
        """CREATE TABLE IF NOT EXISTS public.expenses (
            id SERIAL PRIMARY KEY,
            date TEXT NOT NULL,
            client_name TEXT NOT NULL,
            item TEXT NOT NULL,
            amount REAL NOT NULL,
            phase TEXT DEFAULT 'Général',
            supplier TEXT DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
        """CREATE TABLE IF NOT EXISTS public.progress (
            client_name TEXT PRIMARY KEY,
            phase1 REAL DEFAULT 0,
            phase2 REAL DEFAULT 0,
            phase3 REAL DEFAULT 0,
            phase4 REAL DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
        """CREATE TABLE IF NOT EXISTS public.site_photos (
            id SERIAL PRIMARY KEY,
            upload_date TEXT NOT NULL,
            client_name TEXT NOT NULL,
            phase TEXT DEFAULT 'Général',
            photo_data TEXT NOT NULL,
            notes TEXT DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
        """CREATE TABLE IF NOT EXISTS public.inventory (
            id SERIAL PRIMARY KEY,
            item_name TEXT UNIQUE NOT NULL,
            category TEXT DEFAULT '',
            quantity REAL DEFAULT 0,
            unit TEXT DEFAULT 'pcs',
            min_stock REAL DEFAULT 20,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
        """CREATE TABLE IF NOT EXISTS public.inventory_logs (
            id SERIAL PRIMARY KEY,
            date TEXT NOT NULL,
            item_name TEXT NOT NULL,
            change_amount REAL NOT NULL,
            direction TEXT DEFAULT 'in',
            site_allocated TEXT DEFAULT '',
            notes TEXT DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
        """CREATE TABLE IF NOT EXISTS public.system_users (
            username TEXT PRIMARY KEY,
            password TEXT NOT NULL,
            role TEXT NOT NULL,
            reference TEXT DEFAULT 'Master',
            active BOOLEAN DEFAULT TRUE,
            last_login TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
        """CREATE TABLE IF NOT EXISTS public.payments (
            id SERIAL PRIMARY KEY,
            date TEXT NOT NULL,
            client_name TEXT NOT NULL,
            amount REAL NOT NULL,
            method TEXT DEFAULT 'Virement',
            notes TEXT DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
        """CREATE TABLE IF NOT EXISTS public.audit_log (
            id SERIAL PRIMARY KEY,
            action_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            username TEXT,
            action TEXT,
            table_name TEXT,
            record_id TEXT,
            details TEXT
        )""",
    ]

    seed = """
        INSERT INTO public.system_users (username, password, role, reference)
        VALUES ('admin', %s, 'Admin', 'Master')
        ON CONFLICT (username) DO NOTHING
    """

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            for stmt in ddl:
                cur.execute(stmt)
            cur.execute(seed, (hash_password("Admin2026!"),))
        conn.commit()
    except Exception as e:
        conn.rollback()
        st.error(f"Database initialization failed: {e}")
    finally:
        conn.close()


init_db()


# ══════════════════════════════════════════════════════════════
# AUDIT LOGGING
# ══════════════════════════════════════════════════════════════
def log_audit(action: str, table_name: str = "", record_id: str = "", details: str = ""):
    """Log action to audit trail."""
    user = st.session_state.get("user", "anonymous")
    execute(
        "INSERT INTO public.audit_log (username, action, table_name, record_id, details) VALUES (%s, %s, %s, %s, %s)",
        (user, action, table_name, str(record_id), details),
    )


# ══════════════════════════════════════════════════════════════
# SESSION & AUTH
# ══════════════════════════════════════════════════════════════
_defaults = {"auth": False, "role": None, "user": None, "ref": None, "login_time": None}
for k, v in _defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v


def logout():
    """Clear session and redirect to login."""
    log_audit("LOGOUT")
    for k, v in _defaults.items():
        st.session_state[k] = v
    st.rerun()


# ══════════════════════════════════════════════════════════════
# LOGIN PAGE
# ══════════════════════════════════════════════════════════════
if not st.session_state["auth"]:
    col1, col2, col3 = st.columns([1, 1.4, 1])
    with col2:
        st.markdown("<br><br>", unsafe_allow_html=True)
        logo = "logo.png"
        if os.path.exists(logo):
            st.image(logo, width=120)
        else:
            st.markdown("## 💡 Newlightemara OS")

        st.markdown("#### Sign in to your workspace")
        st.markdown("---")

        with st.form("login_form", clear_on_submit=False):
            username = st.text_input("Username", placeholder="Enter username")
            password = st.text_input("Password", type="password", placeholder="••••••••")
            submitted = st.form_submit_button("Sign In →", use_container_width=True, type="primary")

        if submitted:
            if not username or not password:
                st.error("Please enter both username and password.")
            else:
                users = run_query(
                    "SELECT * FROM public.system_users WHERE username = :u AND password = :p AND active = TRUE",
                    {"u": username, "p": hash_password(password)},
                )
                if users.empty:
                    users = run_query(
                        "SELECT * FROM public.system_users WHERE username = :u AND password = :p AND active = TRUE",
                        {"u": username, "p": password},
                    )
                if not users.empty:
                    row = users.iloc[0]
                    st.session_state.update(
                        auth=True, role=row["role"],
                        user=row["username"], ref=row["reference"],
                        login_time=time.time(),
                    )
                    execute("UPDATE public.system_users SET last_login = CURRENT_TIMESTAMP WHERE username = %s", (username,))
                    log_audit("LOGIN", "system_users", username)
                    st.rerun()
                else:
                    st.error("❌ Invalid credentials or account inactive.")
                    log_audit("LOGIN_FAILED", details=f"username: {username}")
    st.stop()


# ══════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════
ROLE = st.session_state["role"]
REF  = st.session_state["ref"]
USER = st.session_state["user"]

fmt_dh  = lambda n: f"{n:,.0f} DH" if n is not None else "0 DH"
pct     = lambda a, b: round(a / b * 100, 1) if b else 0.0


def badge(text: str, style: str = "badge-blue") -> str:
    return f'<span class="badge {style}">{text}</span>'


def phase_badge(phase: str) -> str:
    cls = PHASE_COLORS.get(phase, ("badge-blue", "#000"))[0]
    return f'<span class="badge {cls}">{phase}</span>'


def rain_money():
    st.markdown("""
    <script>
    (function(){
      const emojis=['💸','💵','💰','🤑'];
      for(let i=0;i<25;i++){
        const d=document.createElement('div');
        d.textContent=emojis[Math.floor(Math.random()*emojis.length)];
        d.style.cssText=`position:fixed;font-size:${20+Math.random()*16}px;left:${Math.random()*100}vw;top:-5%;z-index:9999;pointer-events:none;animation:fall 2.5s ease-in forwards;animation-delay:${Math.random()*1.5}s`;
        document.body.appendChild(d);
        setTimeout(()=>d.remove(),4000);
      }
      if(!document.getElementById('fall-style')){
        const s=document.createElement('style');
        s.id='fall-style';
        s.textContent='@keyframes fall{to{top:110%;transform:rotate(720deg);opacity:0}}';
        document.head.appendChild(s);
      }
    })();
    </script>
    """, unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════
# SIDEBAR NAVIGATION
# ══════════════════════════════════════════════════════════════
with st.sidebar:
    logo = "logo.png"
    if os.path.exists(logo):
        st.image(logo, width=140)
    else:
        st.markdown(
            '<div class="sidebar-brand"><h2 style="color:#fff;font-size:18px;font-weight:800">💡 Newlightemara</h2>'
            '<p style="color:#9ca3af;font-size:12px">Field Operations OS</p></div>',
            unsafe_allow_html=True,
        )

    st.markdown(
        f'<div style="padding:8px 16px;margin-bottom:4px">'
        f'<span style="font-size:11px;color:#9ca3af">Logged in as </span>'
        f'<strong style="color:#e5e7eb;font-size:12px">{USER}</strong> '
        f'<span style="font-size:10px;background:#1f2937;color:#6b7280;padding:2px 6px;border-radius:4px;margin-left:4px">{ROLE}</span>'
        f'</div>',
        unsafe_allow_html=True,
    )

    if ROLE == "Admin":
        menu_opts = ["Dashboard", "Smart Estimator", "Client Portfolios", "Timesheets",
                     "Payroll", "Efficiency Matrix", "Procurement", "Milestones",
                     "Site Photos", "Warehouse", "Invoicing", "Dispatch", "Settings"]
        menu_icos = ["bar-chart-fill", "calculator-fill", "building-fill", "clock-fill",
                     "wallet2", "speedometer2", "cart-fill", "check-circle-fill",
                     "camera-fill", "box-fill", "receipt", "send-fill", "shield-fill"]
    elif ROLE == "Technician":
        menu_opts = ["Timesheets", "Site Photos"]
        menu_icos = ["clock-fill", "camera-fill"]
    else:
        menu_opts = ["VIP Portal"]
        menu_icos = ["star-fill"]

    menu = option_menu(
        None, menu_opts, icons=menu_icos,
        menu_icon="cast", default_index=0,
        styles={
            "container": {"padding": "4px 8px", "background": "transparent"},
            "nav-link": {
                "font-size": "13px", "font-weight": "500",
                "color": "#9ca3af", "border-radius": "7px",
                "margin": "2px 0", "padding": "8px 12px",
            },
            "nav-link-selected": {
                "background": "rgba(26,86,219,0.25)",
                "color": "#93c5fd", "font-weight": "700",
            },
            "icon": {"font-size": "14px"},
        },
    )

    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("↩ Logout", use_container_width=True):
        logout()


# ══════════════════════════════════════════════════════════════
# PAGE: DASHBOARD
# ══════════════════════════════════════════════════════════════
if menu == "Dashboard":
    st.title("📊 Executive Command Center")

    clients  = run_query("SELECT * FROM public.clients WHERE archived = FALSE")
    labor    = run_query("SELECT * FROM public.labor_logs")
    expenses = run_query("SELECT * FROM public.expenses")
    payments = run_query("SELECT * FROM public.payments")

    if clients.empty:
        st.info("No data yet. Start by adding clients in **Client Portfolios**.")
        st.stop()

    total_budget  = clients["budget"].sum()
    total_advance = clients["advance"].sum()
    total_labor   = labor["cost"].sum() if not labor.empty else 0
    total_mat     = expenses["amount"].sum() if not expenses.empty else 0
    total_cost    = total_labor + total_mat
    gross_profit  = total_advance - total_cost
    active_sites  = (clients["status"] == "active").sum() if "status" in clients.columns else len(clients)
    completed_sites = (clients["status"] == "completed").sum() if "status" in clients.columns else 0

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Active Sites",  int(active_sites))
    c2.metric("Completed",     int(completed_sites))
    c3.metric("Cash Collected",   fmt_dh(total_advance),
              f"{pct(total_advance, total_budget):.0f}% of total")
    c4.metric("Labor Cost",       fmt_dh(total_labor))
    c5.metric("Materials",        fmt_dh(total_mat))
    c6.metric("Gross Profit",     fmt_dh(gross_profit),
              delta=f"{pct(gross_profit, total_advance):.1f}% margin" if total_advance else None)

    st.markdown("---")

    col_left, col_right = st.columns([3, 2])

    with col_left:
        st.subheader("Site Performance")
        rows = []
        for _, r in clients.iterrows():
            lc = labor[labor["client_name"] == r["client_name"]]["cost"].sum() if not labor.empty else 0
            mc = expenses[expenses["client_name"] == r["client_name"]]["amount"].sum() if not expenses.empty else 0
            spent  = lc + mc
            margin = pct(r["advance"] - spent, r["advance"]) if r["advance"] else 0.0
            remaining = r["budget"] - r["advance"]
            rows.append({
                "Site":         r["client_name"],
                "Budget":       fmt_dh(r["budget"]),
                "Collected":    fmt_dh(r["advance"]),
                "Spent":        fmt_dh(spent),
                "Remaining":    fmt_dh(remaining),
                "Margin %":     f"{margin:.1f}%",
                "Status":       r.get("status", "active"),
            })
        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True, hide_index=True)

        # Export functionality
        if st.button("📥 Export CSV", use_container_width=True):
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            st.download_button("Download CSV", csv_buffer.getvalue(), "sites_report.csv", "text/csv")

    with col_right:
        st.subheader("Spend Breakdown")
        if not labor.empty:
            phase_spend = labor.groupby("phase")["cost"].sum().reset_index()
            phase_spend.columns = ["Phase", "Labor (DH)"]
            st.dataframe(phase_spend, use_container_width=True, hide_index=True)

        st.subheader("Recent Activity")
        if not labor.empty:
            recent = labor.sort_values("date", ascending=False).head(6)[
                ["date", "worker_name", "client_name", "phase", "days", "cost"]
            ].rename(columns={
                "date": "Date", "worker_name": "Worker",
                "client_name": "Site", "phase": "Phase",
                "days": "Days", "cost": "Cost (DH)",
            })
            st.dataframe(recent, use_container_width=True, hide_index=True)

        # Payment history
        if not payments.empty:
            st.subheader("Recent Payments")
            recent_pay = payments.sort_values("date", ascending=False).head(5)[["date", "client_name", "amount", "method"]]
            recent_pay.columns = ["Date", "Client", "Amount", "Method"]
            recent_pay["Amount"] = recent_pay["Amount"].apply(fmt_dh)
            st.dataframe(recent_pay, use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════
# PAGE: SMART ESTIMATOR
# ══════════════════════════════════════════════════════════════
elif menu == "Smart Estimator":
    st.title("🧠 Smart Estimator")

    clients  = run_query("SELECT * FROM public.clients WHERE archived = FALSE")
    labor    = run_query("SELECT * FROM public.labor_logs")
    expenses = run_query("SELECT * FROM public.expenses")

    col_form, col_result = st.columns(2)

    with col_form:
        with st.form("estimator_form"):
            st.subheader("Project Parameters")
            project_name = st.text_input("Project Name", placeholder="e.g. Villa Hassan II")

            c1, c2 = st.columns(2)
            points = c1.number_input("Estimated Points", min_value=1, value=100, step=10)
            surface = c2.number_input("Surface m² (optional)", min_value=0, value=0, step=10)

            margin_pct = st.slider("Target Margin %", min_value=10, max_value=60, value=30, step=5)

            st.markdown("**Work Types Included**")
            work_types = {}
            cols = st.columns(3)
            for i, wt in enumerate(["Incorporation", "Tirage", "Appareillage", "Tableau", "VDI", "Domotique"]):
                work_types[wt] = cols[i % 3].checkbox(wt, value=i < 4)

            submitted = st.form_submit_button("🧠 Generate Quote", use_container_width=True, type="primary")

        # Historical reference
        st.subheader("Historical Cost Reference")
        if not clients.empty:
            ref_rows = []
            for _, r in clients.iterrows():
                lc = labor[labor["client_name"] == r["client_name"]]["cost"].sum() if not labor.empty else 0
                mc = expenses[expenses["client_name"] == r["client_name"]]["amount"].sum() if not expenses.empty else 0
                dh_pt = (lc + mc) / r["total_points"] if r["total_points"] > 0 else 0
                ref_rows.append({
                    "Site": r["client_name"],
                    "Budget": fmt_dh(r["budget"]),
                    "Points": int(r["total_points"]),
                    "DH/pt": fmt_dh(dh_pt),
                })
            st.dataframe(pd.DataFrame(ref_rows), use_container_width=True, hide_index=True)

    with col_result:
        if submitted:
            if not project_name or points <= 0:
                st.error("Project name and valid points are required.")
            else:
                total_pts  = clients["total_points"].sum() if not clients.empty else 0
                total_cost_hist = (
                    (labor["cost"].sum() if not labor.empty else 0) +
                    (expenses["amount"].sum() if not expenses.empty else 0)
                )
                cost_per_pt = total_cost_hist / total_pts if total_pts > 0 else 150
                base_cost   = points * cost_per_pt
                quote       = base_cost / (1 - margin_pct / 100)
                gross_profit = quote - base_cost

                st.subheader(f"Quote: {project_name}")
                st.markdown(
                    f'<div style="text-align:center;background:#f0f9ff;border:2px solid #bfdbfe;border-radius:12px;padding:20px;margin:12px 0">'
                    f'<div style="font-size:12px;color:#64748b;font-weight:600;text-transform:uppercase;letter-spacing:.5px">Recommended Price</div>'
                    f'<div style="font-size:36px;font-weight:900;color:#1a56db;margin:8px 0">{fmt_dh(quote)}</div>'
                    f'<span class="badge badge-green">Margin: {margin_pct}%</span>'
                    f'</div>',
                    unsafe_allow_html=True,
                )

                details = {
                    "Estimated Cost":    fmt_dh(base_cost),
                    "Gross Profit":      fmt_dh(gross_profit),
                    "Labor estimate":    fmt_dh(base_cost * 0.45),
                    "Materials estimate":fmt_dh(base_cost * 0.55),
                    "Cost per Point":    fmt_dh(cost_per_pt),
                }
                if surface > 0:
                    details["Price per m²"] = fmt_dh(quote / surface)

                df_details = pd.DataFrame(details.items(), columns=["Item", "Amount"])
                st.dataframe(df_details, use_container_width=True, hide_index=True)

                # Store in session for PDF export
                st.session_state["last_quote"] = {
                    "project": project_name,
                    "quote": quote,
                    "details": details,
                    "points": points,
                    "margin": margin_pct,
                }

        # PDF export (outside form)
        if "last_quote" in st.session_state:
            if st.button("📄 Export Quote PDF", type="primary", use_container_width=True):
                q = st.session_state["last_quote"]
                pdf = FPDF()
                pdf.add_page()
                pdf.set_font("Arial", "B", 18)
                pdf.cell(0, 12, "DEVIS - NEWLIGHTEMARA", ln=True, align="C")
                pdf.set_font("Arial", "", 12)
                pdf.cell(0, 8, f"Projet: {q['project']}", ln=True)
                pdf.cell(0, 8, f"Points: {q['points']} | Marge: {q['margin']}%", ln=True)
                pdf.ln(4)
                pdf.set_font("Arial", "B", 14)
                pdf.cell(0, 10, f"Prix recommande: {fmt_dh(q['quote'])}", ln=True)
                pdf.set_font("Arial", "", 11)
                for k, v in q["details"].items():
                    pdf.cell(0, 7, f"  {k}: {v}", ln=True)
                fname = f"devis_{q['project'].replace(' ','_')}.pdf"
                pdf.output(fname)
                with open(fname, "rb") as f:
                    st.download_button("⬇ Download PDF", f.read(), fname, mime="application/pdf")


# ══════════════════════════════════════════════════════════════
# PAGE: CLIENT PORTFOLIOS
# ══════════════════════════════════════════════════════════════
elif menu == "Client Portfolios":
    st.title("🏗 Client Portfolios")

    clients = run_query("SELECT * FROM public.clients WHERE archived = FALSE ORDER BY id")

    tab_list, tab_add, tab_payment, tab_archive = st.tabs(["📋 Sites List", "➕ New Site", "💰 Log Payment", "🗑 Archive"])

    with tab_list:
        if clients.empty:
            st.info("No sites yet. Use the **New Site** tab to add your first project.")
        else:
            for _, r in clients.iterrows():
                remaining = r["budget"] - r["advance"]
                col_name, col_budget, col_collected, col_remaining, col_status, col_btn = st.columns([3,2,2,2,1.5,1.5])
                col_name.markdown(f"**{r['client_name']}**  \n<small style='color:#64748b'>{r.get('work_type','')} · {int(r['total_points'])} pts</small>", unsafe_allow_html=True)
                col_budget.metric("Budget", fmt_dh(r["budget"]))
                col_collected.metric("Collected", fmt_dh(r["advance"]))
                col_remaining.metric("Remaining", fmt_dh(remaining), delta=f"{pct(r['advance'],r['budget']):.0f}% paid" if r["budget"] else None)
                status = r.get("status", "active")
                col_status.markdown(badge("✓ Done" if status == "completed" else "● Active",
                                          "badge-green" if status == "completed" else "badge-blue"), unsafe_allow_html=True)

                btn_cols = col_btn.columns(2)
                if btn_cols[0].button("✓", key=f"done_{r['id']}", disabled=(status == "completed"), help="Mark as completed"):
                    execute("UPDATE public.clients SET status='completed' WHERE id=%s", (r["id"],))
                    log_audit("UPDATE", "clients", r["id"], "status -> completed")
                    st.rerun()
                if btn_cols[1].button("🗑", key=f"del_{r['id']}", help="Archive site"):
                    execute("UPDATE public.clients SET archived = TRUE WHERE id=%s", (r["id"],))
                    log_audit("ARCHIVE", "clients", r["id"])
                    st.rerun()
                st.divider()

    with tab_add:
        with st.form("new_client_form"):
            st.subheader("New Construction Site")
            c1, c2 = st.columns(2)
            nm   = c1.text_input("Client / Site Name *")
            wt   = c2.selectbox("Work Type", WORK_TYPES)
            bg   = c1.number_input("Budget (DH) *", min_value=0.0, value=0.0, step=1000.0)
            pts  = c2.number_input("Total Points", min_value=0.0, value=0.0, step=10.0)
            adv  = c1.number_input("Initial Advance (DH)", min_value=0.0, value=0.0, step=1000.0)
            if st.form_submit_button("✅ Create Site", type="primary"):
                if not nm or bg <= 0:
                    st.error("Name and budget are required.")
                elif not run_query("SELECT 1 FROM public.clients WHERE client_name=%s", (nm,)).empty:
                    st.error(f'Site "{nm}" already exists.')
                else:
                    ok = execute(
                        "INSERT INTO public.clients (client_name,work_type,budget,advance,total_points,status) VALUES (%s,%s,%s,%s,%s,'active')",
                        (nm, wt, bg, adv, pts),
                    )
                    if ok:
                        execute("INSERT INTO public.progress (client_name,phase1,phase2,phase3,phase4) VALUES (%s,0,0,0,0) ON CONFLICT DO NOTHING", (nm,))
                        log_audit("CREATE", "clients", details=f"Created site: {nm}")
                        st.success(f'Site "{nm}" created successfully.')
                        st.rerun()

    with tab_payment:
        if clients.empty:
            st.info("No clients available.")
        else:
            with st.form("payment_form"):
                st.subheader("Collect Payment")
                client_opts = clients["client_name"].tolist()
                sel = st.selectbox("Client", client_opts)
                selected_client = clients[clients["client_name"] == sel].iloc[0]
                outstanding = selected_client["budget"] - selected_client["advance"]
                st.info(f"Outstanding balance: **{fmt_dh(outstanding)}**")

                c1, c2 = st.columns(2)
                amt    = c1.number_input("Amount Received (DH)", min_value=0.01, value=0.0, step=100.0)
                method = c2.selectbox("Payment Method", PAYMENT_METHODS)
                notes  = st.text_input("Notes (optional)")

                if st.form_submit_button("💰 Confirm Payment", type="primary"):
                    if amt <= 0:
                        st.error("Enter a valid amount.")
                    elif amt > outstanding + 0.01:
                        st.error(f"Amount exceeds outstanding balance of {fmt_dh(outstanding)}.")
                    else:
                        ok = execute("UPDATE public.clients SET advance = advance + %s WHERE client_name=%s", (amt, sel))
                        if ok:
                            execute(
                                "INSERT INTO public.payments (date,client_name,amount,method,notes) VALUES (%s,%s,%s,%s,%s)",
                                (str(date.today()), sel, amt, method, notes),
                            )
                            log_audit("PAYMENT", "clients", selected_client["id"], f"Amount: {amt}")
                            rain_money()
                            st.success(f"✅ {fmt_dh(amt)} collected from {sel}!")
                            st.rerun()

    with tab_archive:
        st.subheader("Archived Sites")
        archived = run_query("SELECT * FROM public.clients WHERE archived = TRUE ORDER BY id")
        if archived.empty:
            st.info("No archived sites.")
        else:
            for _, r in archived.iterrows():
                col1, col2 = st.columns([4, 1])
                col1.markdown(f"**{r['client_name']}** — {r.get('work_type','')} — Budget: {fmt_dh(r['budget'])}")
                if col2.button("Restore", key=f"restore_{r['id']}"):
                    execute("UPDATE public.clients SET archived = FALSE WHERE id=%s", (r["id"],))
                    log_audit("RESTORE", "clients", r["id"])
                    st.rerun()


# ══════════════════════════════════════════════════════════════
# PAGE: TIMESHEETS
# ══════════════════════════════════════════════════════════════
elif menu == "Timesheets":
    st.title("🕐 Daily Timesheets")

    workers = run_query("SELECT * FROM public.workers WHERE active = TRUE ORDER BY name")
    clients = run_query("SELECT client_name FROM public.clients WHERE status='active' AND archived = FALSE")

    if workers.empty:
        st.warning("No workers registered yet. Add technicians in **Settings**.")
        st.stop()

    col_form, col_log = st.columns([1, 1])

    with col_form:
        with st.form("timesheet_form"):
            st.subheader("Log Working Days")
            d     = st.date_input("Date", value=date.today())
            site  = st.selectbox("Site", clients["client_name"].tolist() if not clients.empty else ["—"])
            phase = st.selectbox("Phase", PHASES)

            # Per-worker day inputs
            st.markdown("**Technicians**")
            worker_days = {}
            for _, w in workers.iterrows():
                cols = st.columns([3, 1])
                checked = cols[0].checkbox(f"{w['name']} ({fmt_dh(w['tjm'])}/j)", key=f"wchk_{w['id']}")
                days_val = cols[1].number_input("Days", min_value=0.0, max_value=30.0, step=0.5,
                                                 value=0.0, key=f"wdays_{w['id']}",
                                                 label_visibility="collapsed")
                if checked and days_val > 0:
                    worker_days[w["name"]] = (days_val, w["tjm"])

            submitted = st.form_submit_button("✅ Submit Logs", type="primary", use_container_width=True)

        if submitted:
            if not worker_days:
                st.error("Select at least one technician with days > 0.")
            else:
                logged = 0
                for worker_name, (days_worked, tjm) in worker_days.items():
                    if days_worked > 0:
                        execute(
                            "INSERT INTO public.labor_logs (date,client_name,worker_name,days,cost,phase) VALUES (%s,%s,%s,%s,%s,%s)",
                            (str(d), site, worker_name, days_worked, days_worked * tjm, phase),
                        )
                        logged += 1
                if logged:
                    log_audit("CREATE", "labor_logs", details=f"Logged {logged} entries for {site}")
                    st.success(f"✅ {logged} log(s) saved.")
                    st.rerun()

    with col_log:
        st.subheader("Recent Logs")
        logs = run_query(
            "SELECT id, date, worker_name, client_name, phase, days, cost FROM public.labor_logs ORDER BY id DESC LIMIT 30"
        )
        if not logs.empty:
            # Add delete buttons
            for _, row in logs.iterrows():
                cols = st.columns([5, 1])
                cols[0].markdown(
                    f"{row['date']} | **{row['worker_name']}** | {row['client_name']} | "
                    f"{row['phase']} | {row['days']}j | {fmt_dh(row['cost'])}"
                )
                if cols[1].button("🗑", key=f"del_log_{row['id']}", help="Delete entry"):
                    execute("DELETE FROM public.labor_logs WHERE id = %s", (row["id"],))
                    log_audit("DELETE", "labor_logs", row["id"])
                    st.rerun()
        else:
            st.info("No timesheet entries yet.")


# ══════════════════════════════════════════════════════════════
# PAGE: PAYROLL
# ══════════════════════════════════════════════════════════════
elif menu == "Payroll":
    st.title("💳 Payroll")

    col1, col2, col3 = st.columns(3)
    start_date = col1.date_input("From", value=date.today() - timedelta(days=30))
    end_date   = col2.date_input("To",   value=date.today())
    workers    = run_query("SELECT name FROM public.workers WHERE active = TRUE ORDER BY name")
    worker_filter = col3.selectbox("Worker", ["All Workers"] + (workers["name"].tolist() if not workers.empty else []))

    logs = run_query("SELECT * FROM public.labor_logs WHERE date >= %s AND date <= %s",
                     (str(start_date), str(end_date)))

    if not logs.empty and worker_filter != "All Workers":
        logs = logs[logs["worker_name"] == worker_filter]

    if logs.empty:
        st.info("No labor records found for this period.")
    else:
        total_pay  = logs["cost"].sum()
        total_days = logs["days"].sum()

        c1, c2, c3 = st.columns(3)
        c1.metric("Total Payroll",  fmt_dh(total_pay))
        c2.metric("Total Days",     f"{total_days:.1f} days")
        c3.metric("Workers Active", logs["worker_name"].nunique())

        st.markdown("---")

        pay_table = (
            logs.groupby("worker_name")
            .agg(Days=("days", "sum"), Total=("cost", "sum"))
            .reset_index()
            .sort_values("Total", ascending=False)
        )
        pay_table["Total"] = pay_table["Total"].apply(fmt_dh)
        pay_table["Days"]  = pay_table["Days"].apply(lambda x: f"{x:.1f}")
        pay_table.columns  = ["Worker", "Days Worked", "Amount Due"]
        st.dataframe(pay_table, use_container_width=True, hide_index=True)

        # Export payroll
        if st.button("📥 Export Payroll CSV"):
            csv_buffer = io.StringIO()
            pay_table.to_csv(csv_buffer, index=False)
            st.download_button("Download", csv_buffer.getvalue(), f"payroll_{start_date}_{end_date}.csv", "text/csv")

        st.markdown("---")
        st.subheader("Detail")
        detail = logs[["date", "worker_name", "client_name", "phase", "days", "cost"]].copy()
        detail["cost"] = detail["cost"].apply(fmt_dh)
        detail.columns = ["Date", "Worker", "Site", "Phase", "Days", "Cost"]
        st.dataframe(detail.sort_values("Date", ascending=False), use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════
# PAGE: EFFICIENCY MATRIX
# ══════════════════════════════════════════════════════════════
elif menu == "Efficiency Matrix":
    st.title("⚡ Efficiency Matrix")

    labor = run_query("SELECT * FROM public.labor_logs")

    if labor.empty:
        st.info("No labor data to analyze yet.")
    else:
        tab_worker, tab_site, tab_trend = st.tabs(["By Worker", "By Site", "Cost Trend"])

        with tab_worker:
            pivot = pd.pivot_table(
                labor, values="days", index="worker_name",
                columns="phase", aggfunc="sum", fill_value=0,
            )
            pivot["Total Days"] = pivot.sum(axis=1)
            cost_map = labor.groupby("worker_name")["cost"].sum()
            pivot["Total Cost (DH)"] = cost_map.apply(fmt_dh)
            st.dataframe(pivot, use_container_width=True)

        with tab_site:
            pivot2 = pd.pivot_table(
                labor, values="days", index="client_name",
                columns="phase", aggfunc="sum", fill_value=0,
            )
            pivot2["Total Days"] = pivot2.sum(axis=1)
            st.dataframe(pivot2, use_container_width=True)

        with tab_trend:
            labor["date"] = pd.to_datetime(labor["date"])
            daily = labor.groupby("date").agg(Cost=("cost","sum"), Days=("days","sum")).reset_index()
            daily["date"] = daily["date"].dt.strftime("%Y-%m-%d")
            st.line_chart(daily.set_index("date")["Cost"], height=250, color="#1a56db")
            st.caption("Daily labor cost over time")


# ══════════════════════════════════════════════════════════════
# PAGE: PROCUREMENT
# ══════════════════════════════════════════════════════════════
elif menu == "Procurement":
    st.title("🛒 Procurement & Expenses")

    clients = run_query("SELECT client_name FROM public.clients WHERE archived = FALSE ORDER BY client_name")

    col_form, col_summary = st.columns([1, 1])

    with col_form:
        with st.form("expense_form"):
            st.subheader("Log Material Expense")
            c1, c2 = st.columns(2)
            site     = c1.selectbox("Site", clients["client_name"].tolist() if not clients.empty else ["—"])
            phase    = c2.selectbox("Phase", PHASES)
            item     = st.text_input("Item Description *", placeholder="e.g. Câbles 2.5mm² (500m)")
            c1b, c2b = st.columns(2)
            amt      = c1b.number_input("Amount (DH) *", min_value=0.01, value=0.0, step=100.0)
            supplier = c2b.text_input("Supplier", placeholder="Optional")

            if st.form_submit_button("+ Log Expense", type="primary"):
                if not item or amt <= 0:
                    st.error("Item description and valid amount are required.")
                else:
                    ok = execute(
                        "INSERT INTO public.expenses (date,client_name,item,amount,phase,supplier) VALUES (%s,%s,%s,%s,%s,%s)",
                        (str(date.today()), site, item, amt, phase, supplier),
                    )
                    if ok:
                        log_audit("CREATE", "expenses", details=f"{item} - {amt} DH")
                        st.success("Expense logged.")
                        st.rerun()

    with col_summary:
        st.subheader("Spend by Phase")
        expenses = run_query("SELECT * FROM public.expenses")
        if not expenses.empty:
            by_phase = expenses.groupby("phase")["amount"].sum().reset_index()
            total_exp = by_phase["amount"].sum()
            for _, row in by_phase.iterrows():
                pct_val = pct(row["amount"], total_exp)
                st.markdown(
                    f'{phase_badge(row["phase"])} &nbsp; **{fmt_dh(row["amount"])}** &nbsp;'
                    f'<span style="color:#64748b;font-size:12px">({pct_val:.0f}%)</span>',
                    unsafe_allow_html=True,
                )
                st.progress(pct_val / 100)

    st.markdown("---")
    st.subheader("Expense Log")
    expenses = run_query("SELECT id, date, client_name, phase, item, amount, supplier FROM public.expenses ORDER BY id DESC")
    if not expenses.empty:
        for _, row in expenses.iterrows():
            cols = st.columns([6, 1])
            cols[0].markdown(
                f"{row['date']} | **{row['item']}** | {row['client_name']} | "
                f"{row['phase']} | {fmt_dh(row['amount'])} | {row['supplier']}"
            )
            if cols[1].button("🗑", key=f"del_exp_{row['id']}"):
                execute("DELETE FROM public.expenses WHERE id = %s", (row["id"],))
                log_audit("DELETE", "expenses", row["id"])
                st.rerun()
    else:
        st.info("No expenses logged yet.")


# ══════════════════════════════════════════════════════════════
# PAGE: MILESTONES
# ══════════════════════════════════════════════════════════════
elif menu == "Milestones":
    st.title("✅ Technical Progress")

    clients = run_query("SELECT client_name FROM public.clients WHERE archived = FALSE ORDER BY client_name")
    if clients.empty:
        st.info("No clients yet.")
        st.stop()

    site = st.selectbox("Select Site", clients["client_name"])
    curr = run_query("SELECT * FROM public.progress WHERE client_name=%s", (site,))
    v = curr.iloc[0] if not curr.empty else {"phase1": 0, "phase2": 0, "phase3": 0, "phase4": 0}

    phase_names  = ["Incorporation", "Tirage", "Appareillage", "Tableau"]
    phase_colors = ["#7c3aed", "#1a56db", "#d97706", "#16a34a"]

    col_sliders, col_visual = st.columns([1, 1])

    with col_sliders:
        with st.form("progress_form"):
            st.subheader("Update Progress")
            vals = []
            for i, (pn, pc) in enumerate(zip(phase_names, phase_colors)):
                val = st.slider(pn, 0, 100, int(v.get(f"phase{i+1}", 0) or 0))
                vals.append(val)
            if st.form_submit_button("💾 Save Progress", type="primary", use_container_width=True):
                execute(
                    """INSERT INTO public.progress (client_name,phase1,phase2,phase3,phase4)
                       VALUES (%s,%s,%s,%s,%s)
                       ON CONFLICT (client_name) DO UPDATE
                       SET phase1=EXCLUDED.phase1,phase2=EXCLUDED.phase2,
                           phase3=EXCLUDED.phase3,phase4=EXCLUDED.phase4,
                           updated_at=CURRENT_TIMESTAMP""",
                    (site, *vals),
                )
                log_audit("UPDATE", "progress", details=f"{site} phases updated")
                st.success("Progress updated.")
                st.rerun()

    with col_visual:
        st.subheader(f"Overview: {site}")
        overall = round(sum(vals) / 4) if "vals" in dir() else 0

        # Circular progress via SVG
        radius, cx, cy = 60, 75, 75
        circumference = 2 * 3.14159 * radius
        dash = circumference * overall / 100
        st.markdown(
            f"""<div style="display:flex;justify-content:center;margin:16px 0">
            <svg width="150" height="150" viewBox="0 0 150 150">
              <circle cx="{cx}" cy="{cy}" r="{radius}" fill="none" stroke="#e2e8f0" stroke-width="10"/>
              <circle cx="{cx}" cy="{cy}" r="{radius}" fill="none"
                stroke="{'#16a34a' if overall==100 else '#1a56db'}" stroke-width="10"
                stroke-dasharray="{dash:.1f} {circumference:.1f}"
                stroke-linecap="round"
                transform="rotate(-90 {cx} {cy})"/>
              <text x="{cx}" y="{cy+6}" text-anchor="middle" font-size="22" font-weight="800" fill="#0f172a">{overall}%</text>
              <text x="{cx}" y="{cy+22}" text-anchor="middle" font-size="10" fill="#64748b">overall</text>
            </svg></div>""",
            unsafe_allow_html=True,
        )

        for i, (pn, pc, val) in enumerate(zip(phase_names, phase_colors, vals)):
            st.markdown(f"**{pn}** &nbsp; `{val}%`")
            st.progress(val / 100)


# ══════════════════════════════════════════════════════════════
# PAGE: SITE PHOTOS
# ══════════════════════════════════════════════════════════════
elif menu == "Site Photos":
    st.title("📷 As-Built Photo Archive")

    clients = run_query("SELECT client_name FROM public.clients WHERE archived = FALSE ORDER BY client_name")

    with st.form("photo_form"):
        c1, c2, c3 = st.columns(3)
        site  = c1.selectbox("Site", clients["client_name"].tolist() if not clients.empty else ["—"])
        phase = c2.selectbox("Phase", PHASES)
        notes = c3.text_input("Notes")
        files = st.file_uploader("Upload Photos", accept_multiple_files=True,
                                  type=["jpg","jpeg","png","webp"])
        if st.form_submit_button("📤 Upload", type="primary"):
            if not files:
                st.error("Select at least one photo.")
            else:
                for f in files:
                    b64 = base64.b64encode(f.getvalue()).decode()
                    execute(
                        "INSERT INTO public.site_photos (upload_date,client_name,phase,photo_data,notes) VALUES (%s,%s,%s,%s,%s)",
                        (str(date.today()), site, phase, b64, notes),
                    )
                log_audit("CREATE", "site_photos", details=f"Uploaded {len(files)} photos for {site}")
                st.success(f"{len(files)} photo(s) uploaded.")
                st.rerun()

    st.markdown("---")
    st.subheader("Gallery")
    filter_site  = st.selectbox("Filter by site",  ["All"] + (clients["client_name"].tolist() if not clients.empty else []), key="g_site")
    filter_phase = st.selectbox("Filter by phase", ["All"] + PHASES, key="g_phase")

    photos = run_query("SELECT * FROM public.site_photos ORDER BY id DESC")
    if not photos.empty:
        if filter_site  != "All": photos = photos[photos["client_name"] == filter_site]
        if filter_phase != "All": photos = photos[photos["phase"] == filter_phase]

    if photos.empty:
        st.info("No photos yet.")
    else:
        cols = st.columns(3)
        for idx, (_, row) in enumerate(photos.iterrows()):
            with cols[idx % 3]:
                try:
                    img_bytes = base64.b64decode(row["photo_data"])
                    st.image(img_bytes, use_column_width=True)
                    st.caption(f"{row['client_name']} · {row['phase']} · {row['upload_date']}")
                    if row.get("notes"):
                        st.markdown(f"<small>{row['notes']}</small>", unsafe_allow_html=True)
                    if st.button("🗑", key=f"del_photo_{row['id']}"):
                        execute("DELETE FROM public.site_photos WHERE id = %s", (row["id"],))
                        log_audit("DELETE", "site_photos", row["id"])
                        st.rerun()
                except Exception:
                    st.warning(f"Could not render photo #{row['id']}")


# ══════════════════════════════════════════════════════════════
# PAGE: WAREHOUSE
# ══════════════════════════════════════════════════════════════
elif menu == "Warehouse":
    st.title("📦 Stock Control")

    inventory = run_query("SELECT * FROM public.inventory ORDER BY item_name")

    # Low-stock alerts
    if not inventory.empty:
        low = inventory[inventory["quantity"] < inventory["min_stock"]]
        if not low.empty:
            st.error(f"⚠️ **{len(low)} item(s) critically low in stock:** " +
                     ", ".join(low["item_name"].tolist()))
        elif not inventory[inventory["quantity"] < 50].empty:
            st.warning("Some items are running low on stock.")

    col_in, col_out = st.columns(2)

    with col_in:
        with st.form("checkin_form"):
            st.subheader("📥 Check In")
            c1, c2 = st.columns(2)
            item_in  = c1.text_input("Item Name")
            qty_in   = c2.number_input("Quantity", min_value=0.1, value=0.0, step=1.0)
            c1b, c2b = st.columns(2)
            unit_in  = c1b.selectbox("Unit", UNITS)
            cat_in   = c2b.selectbox("Category", CATEGORIES)
            min_stock = st.number_input("Minimum Stock Alert", min_value=1.0, value=20.0, step=5.0)
            if st.form_submit_button("+ Receive Stock", type="primary", use_container_width=True):
                if not item_in or qty_in <= 0:
                    st.error("Item and valid quantity required.")
                else:
                    execute(
                        """INSERT INTO public.inventory (item_name,category,quantity,unit,min_stock)
                           VALUES (%s,%s,%s,%s,%s)
                           ON CONFLICT (item_name) DO UPDATE
                           SET quantity = inventory.quantity + EXCLUDED.quantity,
                               min_stock = EXCLUDED.min_stock""",
                        (item_in, cat_in, qty_in, unit_in, min_stock),
                    )
                    execute(
                        "INSERT INTO public.inventory_logs (date,item_name,change_amount,direction) VALUES (%s,%s,%s,'in')",
                        (str(date.today()), item_in, qty_in),
                    )
                    log_audit("CREATE", "inventory", details=f"Received {qty_in} {unit_in} of {item_in}")
                    st.success(f"Stocked {qty_in} {unit_in} of {item_in}.")
                    st.rerun()

    with col_out:
        with st.form("checkout_form"):
            st.subheader("📤 Check Out")
            items_list = inventory["item_name"].tolist() if not inventory.empty else []
            c1, c2 = st.columns(2)
            item_out  = c1.selectbox("Item", items_list if items_list else ["—"])
            qty_out   = c2.number_input("Quantity", min_value=0.1, value=0.0, step=1.0, key="qout")
            clients   = run_query("SELECT client_name FROM public.clients WHERE archived = FALSE ORDER BY client_name")
            site_alloc = st.selectbox("Allocate to Site", ["—"] + (clients["client_name"].tolist() if not clients.empty else []))
            if st.form_submit_button("- Issue Stock", type="primary", use_container_width=True):
                if not items_list or qty_out <= 0:
                    st.error("Item and valid quantity required.")
                else:
                    current = inventory[inventory["item_name"] == item_out]["quantity"].values[0]
                    if qty_out > current:
                        st.error(f"Only {current} available in stock.")
                    else:
                        execute(
                            "UPDATE public.inventory SET quantity = quantity - %s WHERE item_name=%s",
                            (qty_out, item_out),
                        )
                        execute(
                            "INSERT INTO public.inventory_logs (date,item_name,change_amount,direction,site_allocated) VALUES (%s,%s,%s,'out',%s)",
                            (str(date.today()), item_out, qty_out, site_alloc),
                        )
                        log_audit("UPDATE", "inventory", details=f"Issued {qty_out} of {item_out} to {site_alloc}")
                        st.success(f"Issued {qty_out} of {item_out}.")
                        st.rerun()

    st.markdown("---")
    st.subheader("Current Inventory")
    if not inventory.empty:
        def status_tag(q, min_q):
            if q < min_q:   return "🔴 Critical"
            if q < min_q * 2:   return "🟡 Low"
            return "🟢 OK"
        inventory["Status"] = inventory.apply(lambda r: status_tag(r["quantity"], r.get("min_stock", 20)), axis=1)
        display_df = inventory[["item_name","category","quantity","unit","Status"]].rename(
            columns={"item_name":"Item","category":"Category","quantity":"Qty","unit":"Unit"}
        )
        st.dataframe(display_df, use_container_width=True, hide_index=True)

        # Stock movement log
        st.subheader("Stock Movement Log")
        logs = run_query("SELECT * FROM public.inventory_logs ORDER BY id DESC LIMIT 20")
        if not logs.empty:
            st.dataframe(logs[["date","item_name","change_amount","direction","site_allocated"]].rename(
                columns={"date":"Date","item_name":"Item","change_amount":"Qty","direction":"Type","site_allocated":"Site"}
            ), use_container_width=True, hide_index=True)
    else:
        st.info("Inventory is empty.")


# ══════════════════════════════════════════════════════════════
# PAGE: INVOICING
# ══════════════════════════════════════════════════════════════
elif menu == "Invoicing":
    st.title("🧾 Billing Engine")

    clients = run_query("SELECT * FROM public.clients WHERE archived = FALSE ORDER BY client_name")
    if clients.empty:
        st.info("No clients yet.")
        st.stop()

    sel = st.selectbox("Select Client", clients["client_name"])
    client = clients[clients["client_name"] == sel].iloc[0]

    labor    = run_query("SELECT * FROM public.labor_logs WHERE client_name=%s", (sel,))
    expenses = run_query("SELECT * FROM public.expenses WHERE client_name=%s", (sel,))
    payments = run_query("SELECT * FROM public.payments WHERE client_name=%s ORDER BY date DESC", (sel,))

    total_labor = labor["cost"].sum() if not labor.empty else 0
    total_mat   = expenses["amount"].sum() if not expenses.empty else 0
    total_cost  = total_labor + total_mat
    remaining   = float(client["budget"]) - float(client["advance"])

    # Invoice header metrics
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Contract Value", fmt_dh(client["budget"]))
    c2.metric("Total Paid",     fmt_dh(client["advance"]))
    c3.metric("Total Spend",    fmt_dh(total_cost))
    c4.metric("Outstanding",    fmt_dh(remaining), delta=f"{pct(client['advance'],client['budget']):.0f}% collected")

    st.markdown("---")
    col_inv, col_hist = st.columns([3, 2])

    with col_inv:
        # Render invoice-style breakdown
        inv_id = f"INV-{int(client['id']):04d}"
        st.markdown(
            f"""<div style="background:#fff;border:1px solid #e2e8f0;border-radius:12px;padding:24px">
            <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:20px">
              <div>
                <div style="font-size:20px;font-weight:900;color:#0f172a">💡 NEWLIGHTEMARA</div>
                <div style="font-size:12px;color:#64748b">Expert Électricité & Automatisme</div>
              </div>
              <div style="text-align:right">
                <div style="font-size:18px;font-weight:800;color:#1a56db">FACTURE</div>
                <div style="font-size:12px;color:#64748b">#{inv_id}</div>
                <div style="font-size:12px;color:#64748b">{date.today().strftime('%d/%m/%Y')}</div>
              </div>
            </div>
            <div style="font-weight:700;font-size:15px;margin-bottom:4px">{sel}</div>
            <div style="font-size:12px;color:#64748b;margin-bottom:16px">Type: {client.get('work_type','')}</div>
            <hr style="border-color:#e2e8f0">
            </div>""",
            unsafe_allow_html=True,
        )

        tab_labor, tab_mat, tab_pay = st.tabs(["Labor Lines", "Material Lines", "Payment History"])
        with tab_labor:
            if not labor.empty:
                show = labor[["date","worker_name","phase","days","cost"]].copy()
                show["cost"] = show["cost"].apply(fmt_dh)
                show.columns = ["Date","Worker","Phase","Days","Amount"]
                st.dataframe(show, use_container_width=True, hide_index=True)
            else:
                st.info("No labor entries.")
        with tab_mat:
            if not expenses.empty:
                show = expenses[["date","item","phase","amount"]].copy()
                show["amount"] = show["amount"].apply(fmt_dh)
                show.columns = ["Date","Item","Phase","Amount"]
                st.dataframe(show, use_container_width=True, hide_index=True)
            else:
                st.info("No material entries.")
        with tab_pay:
            if not payments.empty:
                show = payments[["date","amount","method","notes"]].copy()
                show["amount"] = show["amount"].apply(fmt_dh)
                show.columns = ["Date","Amount","Method","Notes"]
                st.dataframe(show, use_container_width=True, hide_index=True)
            else:
                st.info("No payments recorded.")

        # Totals
        st.markdown(
            f"""<div style="max-width:280px;margin-left:auto;margin-top:16px">
            <div style="display:flex;justify-content:space-between;padding:6px 0;font-size:13px;border-top:1px solid #e2e8f0">
              <span>Main d'oeuvre</span><strong>{fmt_dh(total_labor)}</strong></div>
            <div style="display:flex;justify-content:space-between;padding:6px 0;font-size:13px">
              <span>Matériaux</span><strong>{fmt_dh(total_mat)}</strong></div>
            <div style="display:flex;justify-content:space-between;padding:6px 0;font-size:13px;color:#16a34a">
              <span>Acomptes versés</span><strong>-{fmt_dh(client['advance'])}</strong></div>
            <div style="display:flex;justify-content:space-between;padding:12px;background:#eff6ff;border:2px solid #bfdbfe;border-radius:8px;margin-top:8px">
              <span style="font-weight:800;color:#1e40af">RESTE À PAYER</span>
              <strong style="font-size:16px;color:#1e40af">{fmt_dh(remaining)}</strong>
            </div></div>""",
            unsafe_allow_html=True,
        )

        # PDF Export - Fixed: separated button from download
        col_a, col_b = st.columns(2)
        if col_a.button("📄 Generate PDF Invoice", type="primary", use_container_width=True):
            pdf = FPDF()
            pdf.add_page()
            pdf.set_font("Arial","B",18)
            pdf.cell(0,12,"FACTURE - NEWLIGHTEMARA",ln=True,align="C")
            pdf.set_font("Arial","",12)
            pdf.cell(0,8,f"Client: {sel}",ln=True)
            pdf.cell(0,8,f"Ref: {inv_id}  |  Date: {date.today()}",ln=True)
            pdf.ln(4)
            if not labor.empty:
                pdf.set_font("Arial","B",11)
                pdf.cell(0,8,"Main d'oeuvre:",ln=True)
                pdf.set_font("Arial","",10)
                for _, row in labor.iterrows():
                    pdf.cell(0,6,f"  {row['worker_name']} - {row['phase']} - {row['days']}j - {fmt_dh(row['cost'])}",ln=True)
            if not expenses.empty:
                pdf.set_font("Arial","B",11)
                pdf.cell(0,8,"Materiaux:",ln=True)
                pdf.set_font("Arial","",10)
                for _, row in expenses.iterrows():
                    pdf.cell(0,6,f"  {row['item']} - {fmt_dh(row['amount'])}",ln=True)
            pdf.ln(4)
            pdf.set_font("Arial","B",13)
            pdf.cell(0,10,f"RESTE A PAYER: {fmt_dh(remaining)}",ln=True)
            fname = f"facture_{sel.replace(' ','_')}.pdf"
            pdf.output(fname)

            with open(fname,"rb") as f:
                pdf_bytes = f.read()

            col_b.download_button(
                label="⬇ Download PDF",
                data=pdf_bytes,
                file_name=fname,
                mime="application/pdf",
                use_container_width=True
            )
            log_audit("GENERATE_INVOICE", "clients", client["id"], f"Invoice generated for {sel}")

    with col_hist:
        st.subheader("Quick Payment")
        with st.form("quick_pay_form"):
            pay_amt    = st.number_input("Amount (DH)", min_value=0.01, value=0.0, step=100.0)
            pay_method = st.selectbox("Method", PAYMENT_METHODS)
            pay_notes  = st.text_input("Notes")
            if st.form_submit_button("💰 Collect", type="primary", use_container_width=True):
                if pay_amt <= 0:
                    st.error("Enter a valid amount.")
                elif pay_amt > remaining + 0.01:
                    st.error(f"Exceeds outstanding balance of {fmt_dh(remaining)}.")
                else:
                    execute("UPDATE public.clients SET advance=advance+%s WHERE client_name=%s", (pay_amt, sel))
                    execute(
                        "INSERT INTO public.payments (date,client_name,amount,method,notes) VALUES (%s,%s,%s,%s,%s)",
                        (str(date.today()), sel, pay_amt, pay_method, pay_notes),
                    )
                    log_audit("PAYMENT", "clients", client["id"], f"Quick payment: {pay_amt}")
                    rain_money()
                    st.success(f"✅ {fmt_dh(pay_amt)} collected!")
                    st.rerun()


# ══════════════════════════════════════════════════════════════
# PAGE: DISPATCH
# ══════════════════════════════════════════════════════════════
elif menu == "Dispatch":
    st.title("📤 Team Dispatch")

    workers = run_query("SELECT * FROM public.workers WHERE active = TRUE ORDER BY name")
    clients = run_query("SELECT client_name FROM public.clients WHERE status='active' AND archived = FALSE ORDER BY client_name")

    if workers.empty or clients.empty:
        st.warning("Add workers and active sites first.")
        st.stop()

    col_form, col_msg = st.columns(2)

    with col_form:
        with st.form("dispatch_form"):
            st.subheader("Create Assignment")
            worker = st.selectbox("Technician", workers["name"])
            site   = st.selectbox("Site", clients["client_name"])
            d      = st.date_input("Date", value=date.today() + timedelta(days=1))
            phase  = st.selectbox("Phase", PHASES)
            notes  = st.text_area("Special Instructions", placeholder="Bring the 16mm² cables, meet foreman at gate...", height=80)
            channel = st.radio("Send via", ["WhatsApp", "SMS"], horizontal=True)
            submitted = st.form_submit_button("🚀 Generate Message", type="primary", use_container_width=True)

    with col_msg:
        if submitted:
            worker_first = worker.split(" ")[0]
            msg = (
                f"🔧 *NEWLIGHTEMARA*\n\n"
                f"Bonjour {worker_first},\n\n"
                f"Votre mission pour demain:\n"
                f"📍 *Chantier:* {site}\n"
                f"🗓 *Date:* {d.strftime('%A %d %B %Y')}\n"
                f"⚡ *Phase:* {phase}"
            )
            if notes:
                msg += f"\n📝 *Notes:* {notes}"
            msg += "\n\nBonne journée! 💡 Newlightemara"

            st.subheader("Message Preview")
            st.code(msg, language=None)

            encoded = urllib.parse.quote(msg)
            if channel == "WhatsApp":
                url = f"https://wa.me/?text={encoded}"
                st.link_button("📱 Open WhatsApp", url, use_container_width=True, type="primary")
            else:
                url = f"sms:?body={encoded}"
                st.link_button("💬 Open SMS", url, use_container_width=True)

            log_audit("DISPATCH", details=f"Dispatched {worker} to {site}")

    st.markdown("---")
    st.subheader("Worker Overview")
    cols = st.columns(min(len(workers), 6))
    for i, (_, w) in enumerate(workers.iterrows()):
        last = run_query(
            "SELECT client_name, phase, date FROM public.labor_logs WHERE worker_name=%s ORDER BY id DESC LIMIT 1",
            (w["name"],),
        )
        with cols[i % len(cols)]:
            st.markdown(
                f'<div style="background:#fff;border:1px solid #e2e8f0;border-radius:10px;padding:14px;text-align:center">'
                f'<div style="font-weight:700;font-size:13px">{w["name"].split()[0]}</div>'
                f'<div style="font-size:11px;color:#64748b;margin:4px 0">{w.get("specialty","")}</div>'
                f'<div style="margin-top:8px">' +
                (badge(last.iloc[0]["client_name"], "badge-blue") if not last.empty else badge("Unassigned", "badge-amber")) +
                f'</div></div>',
                unsafe_allow_html=True,
            )


# ══════════════════════════════════════════════════════════════
# PAGE: SETTINGS
# ══════════════════════════════════════════════════════════════
elif menu == "Settings":
    st.title("⚙️ System Administration")

    tab_users, tab_workers, tab_security, tab_audit = st.tabs(["Users", "Workers", "Security", "Audit Log"])

    with tab_users:
        col_form, col_list = st.columns([1, 1])
        with col_form:
            with st.form("add_user_form"):
                st.subheader("Add User Account")
                u_name = st.text_input("Username")
                u_pass = st.text_input("Password", type="password", help="Minimum 8 characters")
                u_role = st.selectbox("Role", ROLES)
                u_ref  = st.text_input("Reference", help="Worker name for Technician, client name for Client role")
                if st.form_submit_button("Create Account", type="primary"):
                    if not u_name or not u_pass:
                        st.error("Username and password are required.")
                    elif len(u_pass) < 8:
                        st.error("Password must be at least 8 characters.")
                    else:
                        ok = execute(
                            "INSERT INTO public.system_users (username,password,role,reference) VALUES (%s,%s,%s,%s)",
                            (u_name, hash_password(u_pass), u_role, u_ref or "Master"),
                        )
                        if ok:
                            log_audit("CREATE_USER", "system_users", u_name)
                            st.success(f'Account "{u_name}" created.')
                            st.rerun()

        with col_list:
            st.subheader("Existing Users")
            users = run_query("SELECT username, role, reference, active, last_login FROM public.system_users ORDER BY username")
            if not users.empty:
                st.dataframe(users.rename(columns={"username":"Username","role":"Role","reference":"Reference","active":"Active","last_login":"Last Login"}),
                             use_container_width=True, hide_index=True)

                # Toggle user active status
                st.subheader("Manage Users")
                for _, u in users.iterrows():
                    col1, col2 = st.columns([3, 1])
                    col1.markdown(f"**{u['username']}** ({u['role']})")
                    if col2.button("Toggle", key=f"toggle_{u['username']}"):
                        new_status = not u["active"]
                        execute("UPDATE public.system_users SET active = %s WHERE username = %s", (new_status, u["username"]))
                        log_audit("TOGGLE_USER", "system_users", u["username"], f"Active: {new_status}")
                        st.rerun()

    with tab_workers:
        col_form, col_list = st.columns([1, 1])
        with col_form:
            with st.form("add_worker_form"):
                st.subheader("Add Technician")
                w_name = st.text_input("Full Name")
                c1, c2 = st.columns(2)
                w_tjm  = c1.number_input("Daily Rate (DH/j)", min_value=1.0, value=500.0, step=50.0)
                w_spec = c2.selectbox("Specialty", PHASES)
                if st.form_submit_button("Add Technician", type="primary"):
                    if not w_name or w_tjm <= 0:
                        st.error("Name and daily rate required.")
                    else:
                        ok = execute(
                            "INSERT INTO public.workers (name,tjm,specialty) VALUES (%s,%s,%s) ON CONFLICT (name) DO UPDATE SET tjm=EXCLUDED.tjm, specialty=EXCLUDED.specialty",
                            (w_name, w_tjm, w_spec),
                        )
                        if ok:
                            log_audit("CREATE", "workers", details=f"Added worker: {w_name}")
                            st.success(f'"{w_name}" added.')
                            st.rerun()

        with col_list:
            st.subheader("Worker Roster")
            workers = run_query("SELECT id, name, specialty, tjm, active FROM public.workers ORDER BY name")
            if not workers.empty:
                for _, w in workers.iterrows():
                    col1, col2, col3 = st.columns([3, 1, 1])
                    col1.markdown(f"**{w['name']}** — {w['specialty']} — {fmt_dh(w['tjm'])}/j")
                    col2.markdown(badge("Active" if w["active"] else "Inactive", "badge-green" if w["active"] else "badge-gray"), unsafe_allow_html=True)
                    if col3.button("Toggle", key=f"toggle_worker_{w['id']}"):
                        new_status = not w["active"]
                        execute("UPDATE public.workers SET active = %s WHERE id = %s", (new_status, w["id"]))
                        log_audit("TOGGLE_WORKER", "workers", w["id"], f"Active: {new_status}")
                        st.rerun()
            else:
                st.info("No workers registered yet.")

    with tab_security:
        st.subheader("Change Password")
        with st.form("change_pass_form"):
            old_pass  = st.text_input("Current Password", type="password")
            new_pass  = st.text_input("New Password", type="password")
            conf_pass = st.text_input("Confirm New Password", type="password")
            if st.form_submit_button("Update Password"):
                if not all([old_pass, new_pass, conf_pass]):
                    st.error("All fields required.")
                elif new_pass != conf_pass:
                    st.error("New passwords do not match.")
                elif len(new_pass) < 8:
                    st.error("Password must be at least 8 characters.")
                else:
                    result = run_query(
                        "SELECT 1 FROM public.system_users WHERE username=%s AND password=%s",
                        (USER, hash_password(old_pass)),
                    )
                    if result.empty:
                        st.error("Current password is incorrect.")
                    else:
                        execute(
                            "UPDATE public.system_users SET password=%s WHERE username=%s",
                            (hash_password(new_pass), USER),
                        )
                        log_audit("PASSWORD_CHANGE", "system_users", USER)
                        st.success("Password updated. Please log in again.")
                        time.sleep(1)
                        logout()

    with tab_audit:
        st.subheader("Audit Trail")
        audit = run_query("SELECT * FROM public.audit_log ORDER BY action_time DESC LIMIT 100")
        if not audit.empty:
            st.dataframe(audit[["action_time", "username", "action", "table_name", "details"]].rename(
                columns={"action_time":"Time","username":"User","action":"Action","table_name":"Table","details":"Details"}
            ), use_container_width=True, hide_index=True)

            if st.button("📥 Export Audit Log"):
                csv_buffer = io.StringIO()
                audit.to_csv(csv_buffer, index=False)
                st.download_button("Download CSV", csv_buffer.getvalue(), "audit_log.csv", "text/csv")
        else:
            st.info("No audit entries yet.")


# ══════════════════════════════════════════════════════════════
# PAGE: VIP PORTAL (Client role)
# ══════════════════════════════════════════════════════════════
elif menu == "VIP Portal":
    st.title(f"🌟 Project Portal")

    clients = run_query("SELECT * FROM public.clients WHERE client_name=%s", (REF,))
    if clients.empty:
        st.error(f'No project found for "{REF}". Contact your project manager.')
        st.stop()

    client = clients.iloc[0]
    remaining = float(client["budget"]) - float(client["advance"])

    c1, c2, c3 = st.columns(3)
    c1.metric("Contract Value", fmt_dh(client["budget"]))
    c2.metric("Total Paid",     fmt_dh(client["advance"]))
    c3.metric("Remaining",      fmt_dh(remaining))

    st.markdown("---")
    st.subheader("Construction Progress")

    progress = run_query("SELECT * FROM public.progress WHERE client_name=%s", (REF,))
    if not progress.empty:
        p = progress.iloc[0]
        phase_names = ["Incorporation", "Tirage", "Appareillage", "Tableau"]
        for i, name in enumerate(phase_names):
            val = int(p.get(f"phase{i+1}", 0) or 0)
            st.markdown(f"**{name}** — `{val}%`")
            st.progress(val / 100)

    st.markdown("---")
    st.subheader("Recent Site Activity")
    labor = run_query(
        "SELECT date, phase, days FROM public.labor_logs WHERE client_name=%s ORDER BY id DESC LIMIT 10",
        (REF,),
    )
    if not labor.empty:
        labor.columns = ["Date", "Phase", "Days"]
        st.dataframe(labor, use_container_width=True, hide_index=True)
    else:
        st.info("No activity recorded yet.")

    photos = run_query(
        "SELECT * FROM public.site_photos WHERE client_name=%s ORDER BY id DESC LIMIT 6",
        (REF,),
    )
    if not photos.empty:
        st.markdown("---")
        st.subheader("Site Photos")
        cols = st.columns(3)
        for idx, (_, row) in enumerate(photos.iterrows()):
            try:
                with cols[idx % 3]:
                    st.image(base64.b64decode(row["photo_data"]), use_column_width=True)
                    st.caption(f"{row['phase']} · {row['upload_date']}")
            except Exception:
                pass
