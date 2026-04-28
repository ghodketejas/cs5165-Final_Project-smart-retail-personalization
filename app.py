from flask import Flask, render_template, request, redirect, url_for, flash, session
import pandas as pd
import os
import urllib.parse
import pyodbc
import time
from functools import wraps
from sqlalchemy import create_engine, text
from werkzeug.security import check_password_hash, generate_password_hash
from typing import Optional

from sklearn.linear_model import LinearRegression
from sklearn.ensemble import GradientBoostingClassifier

app = Flask(__name__)
# Azure App Service: set FLASK_SECRET_KEY in Application settings (or SECRET_KEY).
app.secret_key = os.environ.get("FLASK_SECRET_KEY") or os.environ.get("SECRET_KEY") or "dev-unsafe-key-change-locally"

DATA_FOLDER = "data"

HOUSEHOLDS_FILE = os.path.join(DATA_FOLDER, "400_households.csv")
TRANSACTIONS_FILE = os.path.join(DATA_FOLDER, "400_transactions.csv")
PRODUCTS_FILE = os.path.join(DATA_FOLDER, "400_products.csv")

SQL_SERVER = os.getenv("AZURE_SQL_SERVER")
SQL_DATABASE = os.getenv("AZURE_SQL_DATABASE")
SQL_USERNAME = os.getenv("AZURE_SQL_USERNAME")
SQL_PASSWORD = os.getenv("AZURE_SQL_PASSWORD")


def _require_sql_settings():
    required = {
        "AZURE_SQL_SERVER": SQL_SERVER,
        "AZURE_SQL_DATABASE": SQL_DATABASE,
        "AZURE_SQL_USERNAME": SQL_USERNAME,
        "AZURE_SQL_PASSWORD": SQL_PASSWORD,
    }
    missing = [k for k, v in required.items() if v is None or (isinstance(v, str) and not v.strip())]
    if missing:
        raise RuntimeError(
            "Missing or empty environment variables: "
            + ", ".join(missing)
            + ". In Azure: App Service → Environment variables, add the AZURE_SQL_* values, then Save and restart the app."
        )


def _sql_error_user_hint(exc) -> str:
    """App Service → SQL often fails with 10060 until the SQL firewall allows the app."""
    s = str(exc)
    if any(
        x in s
        for x in (
            "10060",
            "08S01",
            "08001",
            "TCP Provider",
            "ETIMEDOUT",
            "timed out",
        )
    ):
        return (
            " | Hint: The app cannot reach the database (network / firewall). "
            "In Azure Portal open your *SQL server* (logical server) → *Networking* & Security / *Networking* → "
            "set *Public network access* to *Enabled* if you are not using a private endpoint, then either "
            "turn ON *Allow Azure services and resources to access this server* (simplest for class projects) "
            "OR add a firewall rule for each *Outbound* IP of your App Service (App Service → *Overview* → *Outbound IP addresses*). "
            "Wait ~1–2 minutes after saving. Also confirm *AZURE_SQL_SERVER* is the full host: yourserver.database.windows.net"
        )
    return ""


def _hash_password(plain: str) -> str:
    # pbkdf2:sha256 keeps string length well under common NVARCHAR(256) column limits
    return generate_password_hash(plain, method="pbkdf2:sha256", salt_length=16)


def _safe_next_path(candidate) -> Optional[str]:
    if not candidate or not isinstance(candidate, str):
        return None
    c = candidate.strip()
    if c.startswith("/") and not c.startswith("//") and ".." not in c:
        return c
    return None


def register_user_in_db(username: str, email: str, password: str):
    """
    Returns (ok: bool, error_message: str | None)
    """
    username = (username or "").strip()
    email = (email or "").strip()
    if len(username) < 3 or len(username) > 64:
        return False, "Username must be between 3 and 64 characters."
    if "@" not in email or len(email) > 256:
        return False, "Please enter a valid email."
    if len(password) < 6:
        return False, "Password must be at least 6 characters."
    if len(password) > 200:
        return False, "Password is too long."

    conn = get_sql_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM AppUsers WHERE username = ?", (username,))
        if cur.fetchone():
            return False, "That username is already taken."
        cur.execute("SELECT 1 FROM AppUsers WHERE LOWER(email) = LOWER(?)", (email,))
        if cur.fetchone():
            return False, "That email is already registered."
        ph = _hash_password(password)
        cur.execute(
            "INSERT INTO AppUsers (username, email, pass_hash) VALUES (?, ?, ?)",
            (username, email, ph),
        )
        conn.commit()
        return True, None
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        em = str(e)
        if "UNIQUE" in em.upper() or "2627" in em:
            return False, "That username or email is already in use."
        return False, f"Could not register: {e}"
    finally:
        conn.close()


def verify_user_login(username: str, password: str):
    """Returns user_id (int) if ok, else None."""
    username = (username or "").strip()
    if not username or not password:
        return None
    conn = get_sql_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT user_id, pass_hash FROM AppUsers WHERE username = ?",
            (username,),
        )
        row = cur.fetchone()
        if not row:
            return None
        uid, ph = int(row[0]), row[1]
        if ph and check_password_hash(ph, password):
            return uid
        return None
    finally:
        conn.close()


def login_required(view_func):
    @wraps(view_func)
    def wrapped(*args, **kwargs):
        if session.get("user_id") is None:
            nxt = request.path
            if request.query_string:
                nxt = nxt + "?" + request.query_string.decode()
            return redirect(url_for("login", next=nxt))
        return view_func(*args, **kwargs)

    return wrapped


def get_sql_connection():
    _require_sql_settings()
    server = str(SQL_SERVER).strip()
    database = str(SQL_DATABASE).strip()
    username = str(SQL_USERNAME).strip()
    password = str(SQL_PASSWORD)  # allow symbols; do not strip
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=120;"
    )
    # Retry transient login/network hiccups (common on first cold connection in Azure).
    max_attempts = 3
    base_delay_s = 1.0
    for attempt in range(1, max_attempts + 1):
        try:
            return pyodbc.connect(conn_str)
        except pyodbc.Error as e:
            msg = str(e)
            transient = any(
                t in msg
                for t in (
                    "HYT00",
                    "HYT01",
                    "08001",
                    "08S01",
                    "10060",
                    "timed out",
                    "timeout",
                )
            )
            if attempt == max_attempts or not transient:
                raise
            time.sleep(base_delay_s * attempt)


_sqlalchemy_engine = None


def get_sqlalchemy_engine():
    """
    SQLAlchemy engine for pandas read_sql (avoids UserWarning about raw pyodbc).
    pyodbc is still used for auth helpers that need a raw DBAPI cursor.
    """
    global _sqlalchemy_engine
    if _sqlalchemy_engine is not None:
        return _sqlalchemy_engine
    _require_sql_settings()
    server = str(SQL_SERVER).strip()
    database = str(SQL_DATABASE).strip()
    username = str(SQL_USERNAME).strip()
    password = str(SQL_PASSWORD)
    odbc_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={server};DATABASE={database};UID={username};PWD={password};"
        "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=120;"
    )
    encoded = urllib.parse.quote_plus(odbc_str)
    _sqlalchemy_engine = create_engine(
        f"mssql+pyodbc:///?odbc_connect={encoded}",
        pool_pre_ping=True,
        pool_recycle=3600,
    )
    return _sqlalchemy_engine


def get_data_pull_from_sql(household_id):
    query = """
        SELECT TOP 100
            t.HSHD_NUM AS hshd_num,
            t.BASKET_NUM AS basket_num,
            t.PURCHASE_DATE AS date,
            t.PRODUCT_NUM AS product_num,
            p.DEPARTMENT AS department,
            p.COMMODITY AS commodity,
            t.SPEND AS spend,
            t.UNITS AS units,
            t.STORE_R AS store_region,
            t.WEEK_NUM AS week_num,
            t.YEAR AS year,
            h.LOYALTY_FLAG AS loyalty_flag,
            h.AGE_RANGE AS age_range,
            h.MARITAL_STATUS AS marital_status,
            h.INCOME_RANGE AS income_range,
            h.HOMEOWNER_DESC AS homeowner_desc,
            h.HSHD_COMPOSITION AS hshd_composition,
            h.HH_SIZE AS hshd_size,
            h.CHILDREN AS children
        FROM Transactions t
        JOIN Households h ON t.HSHD_NUM = h.HSHD_NUM
        JOIN Products p ON t.PRODUCT_NUM = p.PRODUCT_NUM
        WHERE t.HSHD_NUM = :hid
        ORDER BY
            t.HSHD_NUM,
            t.BASKET_NUM,
            t.PURCHASE_DATE,
            t.PRODUCT_NUM,
            p.DEPARTMENT,
            p.COMMODITY;
    """

    engine = get_sqlalchemy_engine()
    df = pd.read_sql(text(query), engine, params={"hid": int(household_id)})
    df.columns = [str(c).lower() for c in df.columns]
    return df


# Join used for analytics aggregates (same inner-join semantics as prior full merge).
_SQL_ANALYTICS_JOIN = """
FROM Transactions t
INNER JOIN Products p ON t.PRODUCT_NUM = p.PRODUCT_NUM
INNER JOIN Households h ON t.HSHD_NUM = h.HSHD_NUM
"""

# Basket pair co-occurrence is O(lines × basket width); cap lines transferred from SQL.
_PAIR_SAMPLE_LINE_LIMIT = 250_000

# If basket-level aggregate row count is huge, subsample for Gradient Boosting fit time.
_ML_GB_MAX_BASKETS = 80_000


def _read_sql_lower(engine, sql: str, params=None) -> pd.DataFrame:
    df = pd.read_sql(text(sql), engine, params=params or {})
    if not df.empty:
        df.columns = [str(c).lower() for c in df.columns]
    return df


def fetch_dashboard_metrics_from_sql():
    """
    Dashboard KPIs and chart series using SQL GROUP BY (no full fact-table load into pandas).
    """
    engine = get_sqlalchemy_engine()
    total = _read_sql_lower(
        engine,
        f"SELECT ISNULL(SUM(CAST(t.SPEND AS FLOAT)), 0) AS v {_SQL_ANALYTICS_JOIN}",
    )
    total_spend = float(total["v"].iloc[0]) if not total.empty else 0.0

    nb = _read_sql_lower(
        engine,
        f"SELECT COUNT(DISTINCT t.BASKET_NUM) AS v {_SQL_ANALYTICS_JOIN}",
    )
    total_transactions = int(nb["v"].iloc[0]) if not nb.empty else 0

    dept_df = _read_sql_lower(
        engine,
        f"""
            SELECT TOP 5 p.DEPARTMENT AS department, SUM(CAST(t.SPEND AS FLOAT)) AS spend
            {_SQL_ANALYTICS_JOIN}
            GROUP BY p.DEPARTMENT
            ORDER BY SUM(t.SPEND) DESC
            """,
    )
    week_df = _read_sql_lower(
        engine,
        f"""
            SELECT t.WEEK_NUM AS week_num, SUM(CAST(t.SPEND AS FLOAT)) AS spend
            {_SQL_ANALYTICS_JOIN}
            GROUP BY t.WEEK_NUM
            ORDER BY t.WEEK_NUM
            """,
    )
    income_df = _read_sql_lower(
        engine,
        f"""
            SELECT TOP 12
                COALESCE(NULLIF(LTRIM(RTRIM(CAST(h.INCOME_RANGE AS NVARCHAR(256)))), ''), N'Unknown') AS cat,
                SUM(CAST(t.SPEND AS FLOAT)) AS spend
            {_SQL_ANALYTICS_JOIN}
            GROUP BY COALESCE(NULLIF(LTRIM(RTRIM(CAST(h.INCOME_RANGE AS NVARCHAR(256)))), ''), N'Unknown')
            ORDER BY SUM(t.SPEND) DESC
            """,
    )
    children_df = _read_sql_lower(
        engine,
        f"""
            SELECT TOP 10
                COALESCE(NULLIF(LTRIM(RTRIM(CAST(h.CHILDREN AS NVARCHAR(128)))), ''), N'Unknown') AS cat,
                SUM(CAST(t.SPEND AS FLOAT)) AS spend
            {_SQL_ANALYTICS_JOIN}
            GROUP BY COALESCE(NULLIF(LTRIM(RTRIM(CAST(h.CHILDREN AS NVARCHAR(128)))), ''), N'Unknown')
            ORDER BY SUM(t.SPEND) DESC
            """,
    )
    brand_df = _read_sql_lower(
        engine,
        f"""
            SELECT TOP 14
                COALESCE(NULLIF(LTRIM(RTRIM(CAST(p.BRAND_TY AS NVARCHAR(128)))), ''), N'Unknown') AS cat,
                SUM(CAST(t.SPEND AS FLOAT)) AS spend
            {_SQL_ANALYTICS_JOIN}
            GROUP BY COALESCE(NULLIF(LTRIM(RTRIM(CAST(p.BRAND_TY AS NVARCHAR(128)))), ''), N'Unknown')
            ORDER BY SUM(t.SPEND) DESC
            """,
    )
    organic_df = _read_sql_lower(
        engine,
        f"""
            SELECT TOP 8
                COALESCE(NULLIF(LTRIM(RTRIM(CAST(p.NATURAL_ORGANIC_FLAG AS NVARCHAR(128)))), ''), N'Unknown') AS cat,
                SUM(CAST(t.SPEND AS FLOAT)) AS spend
            {_SQL_ANALYTICS_JOIN}
            GROUP BY COALESCE(NULLIF(LTRIM(RTRIM(CAST(p.NATURAL_ORGANIC_FLAG AS NVARCHAR(128)))), ''), N'Unknown')
            ORDER BY SUM(t.SPEND) DESC
            """,
    )

    def _lv(df, c_cat="cat", c_spend="spend"):
        if df is None or df.empty:
            return [], []
        return [str(x) for x in df[c_cat]], [float(x) for x in df[c_spend]]

    dept_labels, dept_values = _lv(dept_df, "department", "spend")
    week_labels, week_values = _lv(week_df, "week_num", "spend")
    demo_income_labels, demo_income_values = _lv(income_df)
    demo_children_labels, demo_children_values = _lv(children_df)
    brand_ty_labels, brand_ty_values = _lv(brand_df)
    organic_labels, organic_values = _lv(organic_df)

    return {
        "total_spend": f"{total_spend:,.2f}",
        "total_transactions": total_transactions,
        "dept_labels": dept_labels,
        "dept_values": dept_values,
        "week_labels": week_labels,
        "week_values": week_values,
        "demo_income_labels": demo_income_labels,
        "demo_income_values": demo_income_values,
        "demo_children_labels": demo_children_labels,
        "demo_children_values": demo_children_values,
        "brand_ty_labels": brand_ty_labels,
        "brand_ty_values": brand_ty_values,
        "organic_labels": organic_labels,
        "organic_values": organic_values,
        "has_data": total_transactions > 0 or total_spend > 0,
    }


def _basket_pair_results_from_sample(lines_df: pd.DataFrame):
    """Build top commodity pair list from basket_num + commodity lines (possibly sampled)."""
    if lines_df is None or lines_df.empty:
        return []
    lines_df = lines_df.copy()
    lines_df.columns = [str(c).lower() for c in lines_df.columns]
    basket_pairs = (
        lines_df.groupby(["basket_num", "commodity"]).size().reset_index(name="count")
    )
    basket_items = basket_pairs.groupby("basket_num")["commodity"].apply(list)
    pair_counts = {}
    for items in basket_items:
        unique_items = list(set(items))
        for i in range(len(unique_items)):
            for j in range(i + 1, len(unique_items)):
                pair = tuple(sorted([unique_items[i], unique_items[j]]))
                pair_counts[pair] = pair_counts.get(pair, 0) + 1
    top_pairs = sorted(pair_counts.items(), key=lambda x: x[1], reverse=True)[:10]
    return [
        {"item_1": pair[0][0], "item_2": pair[0][1], "times_bought_together": pair[1]}
        for pair in top_pairs
    ]


def fetch_ml_insights_from_sql():
    """
    ML Insights using SQL aggregates + a capped line sample for pair mining.
    Avoids loading the full joined transaction fact table into memory.
    """
    engine = get_sqlalchemy_engine()
    max_w = _read_sql_lower(
        engine,
        f"SELECT MAX(t.WEEK_NUM) AS max_week {_SQL_ANALYTICS_JOIN}",
    )
    if max_w.empty or pd.isna(max_w["max_week"].iloc[0]):
        return None
    latest_week = float(max_w["max_week"].iloc[0])

    customer_summary = _read_sql_lower(
        engine,
        f"""
            SELECT
                t.HSHD_NUM AS hshd_num,
                SUM(CAST(t.SPEND AS FLOAT)) AS total_spend,
                SUM(CAST(t.UNITS AS FLOAT)) AS total_units,
                COUNT(DISTINCT t.BASKET_NUM) AS total_baskets,
                COUNT(DISTINCT t.WEEK_NUM) AS active_weeks
            {_SQL_ANALYTICS_JOIN}
            GROUP BY t.HSHD_NUM
            """,
    )
    if customer_summary.empty:
        return None

    basket_agg = _read_sql_lower(
        engine,
        f"""
            SELECT
                t.BASKET_NUM AS basket_num,
                COUNT_BIG(*) AS n_lines,
                COUNT(DISTINCT p.COMMODITY) AS n_commodity,
                COUNT(DISTINCT p.DEPARTMENT) AS n_department,
                SUM(CAST(t.SPEND AS FLOAT)) AS basket_spend
            {_SQL_ANALYTICS_JOIN}
            GROUP BY t.BASKET_NUM
            """,
    )

    churn_data = _read_sql_lower(
        engine,
        f"""
            SELECT
                t.HSHD_NUM AS hshd_num,
                MAX(t.WEEK_NUM) AS last_purchase_week,
                SUM(CAST(t.SPEND AS FLOAT)) AS total_spend,
                COUNT(DISTINCT t.BASKET_NUM) AS total_baskets
            {_SQL_ANALYTICS_JOIN}
            GROUP BY t.HSHD_NUM
            """,
    )

    pair_lines = _read_sql_lower(
        engine,
        f"""
            SELECT TOP ({_PAIR_SAMPLE_LINE_LIMIT}) t.BASKET_NUM AS basket_num, p.COMMODITY AS commodity
            {_SQL_ANALYTICS_JOIN}
            ORDER BY t.BASKET_NUM, t.PRODUCT_NUM
            """,
    )

    return {
        "latest_week": latest_week,
        "customer_summary": customer_summary,
        "basket_agg": basket_agg,
        "churn_data": churn_data,
        "pair_lines": pair_lines,
    }


DATA_PULL_SORT_KEYS = [
    "hshd_num",
    "basket_num",
    "date",
    "product_num",
    "department",
    "commodity",
]


def _sort_data_pull_results(df, sort_by, ascending):
    if df is None or df.empty:
        return df
    df = df.copy()
    df.columns = [str(c).lower() for c in df.columns]
    key = sort_by if sort_by in DATA_PULL_SORT_KEYS else "hshd_num"
    if key == "date" and key in df.columns:
        col = pd.to_datetime(df["date"], errors="coerce")
        df = df.assign(_sort_date=col).sort_values(
            by="_sort_date", ascending=ascending, na_position="last"
        )
        return df.drop(columns=["_sort_date"])
    if key in df.columns:
        return df.sort_values(by=key, ascending=ascending, na_position="last")
    return df


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/ml-overview")
def ml_overview():
    """Short write-up for deliverable: LR, RF, GB, and CLV model choice (public)."""
    return render_template("ml_overview.html")


@app.route("/register", methods=["GET", "POST"])
def register():
    if session.get("user_id") is not None:
        return redirect(url_for("data_pull"))
    if request.method == "POST":
        un = request.form.get("username", "")
        em = request.form.get("email", "")
        pw = request.form.get("password", "")
        try:
            ok, err = register_user_in_db(un, em, pw)
        except Exception as e:
            flash(f"Registration failed: {e}{_sql_error_user_hint(e)}")
            return render_template("register.html")
        if ok:
            flash("Account created. Please sign in.")
            return redirect(url_for("login"))
        flash(err or "Registration failed.")
    return render_template("register.html")


@app.route("/login", methods=["GET", "POST"])
def login():
    if session.get("user_id") is not None:
        nxt = _safe_next_path(request.args.get("next") or request.form.get("next"))
        if nxt:
            return redirect(nxt)
        return redirect(url_for("data_pull"))
    if request.method == "POST":
        un = request.form.get("username", "")
        pw = request.form.get("password", "")
        try:
            uid = verify_user_login(un, pw)
        except Exception as e:
            flash(f"Sign-in failed: {e}{_sql_error_user_hint(e)}")
            next_url = request.form.get("next") or request.args.get("next") or ""
            return render_template("login.html", next_url=next_url)
        if uid is not None:
            session["user_id"] = uid
            session["username"] = un.strip()
            flash("You are signed in.")
            nxt = _safe_next_path(request.form.get("next") or request.args.get("next"))
            if nxt:
                return redirect(nxt)
            return redirect(url_for("data_pull"))
        flash("Invalid username or password.")
    next_url = request.form.get("next") or request.args.get("next") or ""
    return render_template("login.html", next_url=next_url)


@app.route("/logout", methods=["GET", "POST"])
def logout():
    session.clear()
    flash("You have been signed out.")
    return redirect(url_for("index"))


@app.route("/data-pull", methods=["GET", "POST"])
@login_required
def data_pull():
    household_id = request.values.get("hshd_num", 10)
    sort_by = request.values.get("sort_by", "hshd_num")
    sort_dir = request.values.get("sort_dir", "asc")
    if sort_by not in DATA_PULL_SORT_KEYS:
        sort_by = "hshd_num"
    if sort_dir not in ("asc", "desc"):
        sort_dir = "asc"
    ascending = sort_dir == "asc"

    try:
        results = get_data_pull_from_sql(household_id)
        results = _sort_data_pull_results(results, sort_by, ascending)

    except Exception as e:
        flash(f"Azure SQL error: {e}{_sql_error_user_hint(e)}")
        results = pd.DataFrame()

    no_results = results.empty

    return render_template(
        "data_pull.html",
        household_id=household_id,
        tables=results.to_html(classes="table table-striped table-bordered", index=False) if not no_results else "",
        no_results=no_results,
        sort_by=sort_by,
        sort_dir=sort_dir,
    )


@app.route("/upload", methods=["GET", "POST"])
@login_required
def upload():
    if request.method == "POST":
        try:
            households_file = request.files["households"]
            transactions_file = request.files["transactions"]
            products_file = request.files["products"]

            households_file.save(os.path.join(DATA_FOLDER, "400_households.csv"))
            transactions_file.save(os.path.join(DATA_FOLDER, "400_transactions.csv"))
            products_file.save(os.path.join(DATA_FOLDER, "400_products.csv"))

            flash("Files uploaded successfully. Data updated!")

            return redirect(url_for("data_pull"))

        except Exception as e:
            flash(f"Upload failed: {e}")

    return render_template("upload.html")


@app.route("/dashboard")
@login_required
def dashboard():
    try:
        m = fetch_dashboard_metrics_from_sql()
        has_data = m.pop("has_data", False)
        if not has_data:
            return render_template(
                "dashboard.html",
                total_spend="0.00",
                total_transactions=0,
                dept_labels=[],
                dept_values=[],
                week_labels=[],
                week_values=[],
                demo_income_labels=[],
                demo_income_values=[],
                demo_children_labels=[],
                demo_children_values=[],
                brand_ty_labels=[],
                brand_ty_values=[],
                organic_labels=[],
                organic_values=[],
            )

        return render_template("dashboard.html", **m)

    except Exception as e:
        return f"Dashboard error: {e}{_sql_error_user_hint(e)}"


@app.route("/ml-insights")
@login_required
def ml_insights():
    try:
        data = fetch_ml_insights_from_sql()
        if data is None:
            return render_template(
                "ml_insights.html",
                top_clv=[],
                basket_results=[],
                basket_gb_rows=[],
                high_risk_customers=[],
                high_risk_count=0,
                low_risk_count=0,
            )

        customer_summary = data["customer_summary"]
        customer_summary = customer_summary[customer_summary["total_baskets"] > 0].copy()
        if customer_summary.empty:
            return render_template(
                "ml_insights.html",
                top_clv=[],
                basket_results=[],
                basket_gb_rows=[],
                high_risk_customers=[],
                high_risk_count=0,
                low_risk_count=0,
            )

        # -----------------------------
        # 1. CLV Prediction
        # -----------------------------
        customer_summary["avg_basket_value"] = (
            customer_summary["total_spend"] / customer_summary["total_baskets"]
        ).round(2)

        customer_summary["estimated_future_clv"] = customer_summary["total_spend"] * 1.25

        X = customer_summary[
            ["total_spend", "total_units", "total_baskets", "active_weeks", "avg_basket_value"]
        ]
        y = customer_summary["estimated_future_clv"]

        clv_model = LinearRegression()
        clv_model.fit(X, y)
        customer_summary["predicted_clv"] = clv_model.predict(X).round(2)

        top_clv = customer_summary.sort_values(
            by="predicted_clv",
            ascending=False
        ).head(10)

        # -----------------------------
        # 2. Basket Analysis (pairs from capped line sample; GB on SQL basket aggregates)
        # -----------------------------
        basket_results = _basket_pair_results_from_sample(data["pair_lines"])

        basket_gb_rows = []
        try:
            bf = data["basket_agg"].copy()
            if len(bf) > _ML_GB_MAX_BASKETS:
                bf = bf.sample(n=_ML_GB_MAX_BASKETS, random_state=42)
            if len(bf) >= 40 and bf["basket_spend"].nunique() > 1:
                med_spend = bf["basket_spend"].median()
                bf["high_value_basket"] = (bf["basket_spend"] >= med_spend).astype(int)
                if bf["high_value_basket"].nunique() > 1:
                    Xb = bf[["n_lines", "n_commodity", "n_department"]]
                    yb = bf["high_value_basket"]
                    gb = GradientBoostingClassifier(
                        max_depth=3,
                        n_estimators=48,
                        learning_rate=0.1,
                        random_state=42,
                    )
                    gb.fit(Xb, yb)
                    labels = [
                        "Lines in basket",
                        "Distinct commodities",
                        "Distinct departments",
                    ]
                    for lab, imp in zip(labels, gb.feature_importances_):
                        basket_gb_rows.append(
                            {"feature": lab, "importance_pct": round(float(imp) * 100, 2)}
                        )
        except Exception:
            basket_gb_rows = []

        # -----------------------------
        # 3. Churn Risk
        # -----------------------------
        latest_week = data["latest_week"]
        churn_data = data["churn_data"].copy()
        churn_data["weeks_since_last_purchase"] = (
            latest_week - churn_data["last_purchase_week"]
        )
        churn_data["churn_risk"] = churn_data["weeks_since_last_purchase"].apply(
            lambda x: "High Risk" if x >= 8 else "Low Risk"
        )

        high_risk_count = int((churn_data["churn_risk"] == "High Risk").sum())
        low_risk_count = int((churn_data["churn_risk"] == "Low Risk").sum())

        high_risk_customers = churn_data.sort_values(
            by="weeks_since_last_purchase",
            ascending=False
        ).head(10)

        return render_template(
            "ml_insights.html",
            top_clv=top_clv.to_dict(orient="records"),
            basket_results=basket_results,
            basket_gb_rows=basket_gb_rows,
            high_risk_customers=high_risk_customers.to_dict(orient="records"),
            high_risk_count=high_risk_count,
            low_risk_count=low_risk_count,
        )

    except Exception as e:
        return f"ML Insights error: {e}{_sql_error_user_hint(e)}"


if __name__ == "__main__":
    app.run(debug=True)