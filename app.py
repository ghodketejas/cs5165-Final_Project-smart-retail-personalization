from flask import Flask, render_template, request, redirect, url_for, flash, session
import pandas as pd
import os
import pyodbc
from functools import wraps
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
    return pyodbc.connect(conn_str)


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
        WHERE t.HSHD_NUM = ?
        ORDER BY
            t.HSHD_NUM,
            t.BASKET_NUM,
            t.PURCHASE_DATE,
            t.PRODUCT_NUM,
            p.DEPARTMENT,
            p.COMMODITY;
    """

    conn = get_sql_connection()
    df = pd.read_sql(query, conn, params=[int(household_id)])
    conn.close()

    return df


def get_merged_for_analytics_from_sql():
    """
    Full joined line-level data from Azure SQL, equivalent to the old
    transactions + households + products merge used by Dashboard and ML Insights.
    """
    query = """
        SELECT
            t.HSHD_NUM AS hshd_num,
            t.BASKET_NUM AS basket_num,
            p.COMMODITY AS commodity,
            t.SPEND AS spend,
            t.UNITS AS units,
            t.WEEK_NUM AS week_num,
            p.DEPARTMENT AS department,
            t.STORE_R AS store_region,
            h.HH_SIZE AS hh_size,
            h.CHILDREN AS children,
            h.INCOME_RANGE AS income_range,
            p.BRAND_TY AS brand_ty,
            p.NATURAL_ORGANIC_FLAG AS natural_organic_flag
        FROM Transactions t
        INNER JOIN Products p ON t.PRODUCT_NUM = p.PRODUCT_NUM
        INNER JOIN Households h ON t.HSHD_NUM = h.HSHD_NUM
    """
    conn = get_sql_connection()
    df = pd.read_sql(query, conn)
    conn.close()
    df.columns = [str(c).lower() for c in df.columns]
    return df


def _spend_by_category_for_chart(
    df: pd.DataFrame, col: str, *, fill_label: str = "Unknown", top_n: int = 14
):
    """Aggregate spend by a categorical column for Chart.js (labels + values)."""
    if df is None or df.empty or col not in df.columns:
        return [], []
    s = df.copy()
    s[col] = s[col].fillna(fill_label).astype(str).str.strip()
    s.loc[s[col] == "", col] = fill_label
    g = (
        s.groupby(col, as_index=True)["spend"]
        .sum()
        .sort_values(ascending=False)
        .head(top_n)
    )
    return [str(i) for i in g.index], [float(x) for x in g.values]


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
        merged = get_merged_for_analytics_from_sql()

        if merged.empty:
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

        # Total Spend
        total_spend = f"{merged['spend'].sum():,.2f}"

        # Total Transactions
        total_transactions = int(merged["basket_num"].nunique())

        # Top Departments
        top_departments = (
            merged.groupby("department")["spend"]
            .sum()
            .sort_values(ascending=False)
            .head(5)
        )

        dept_labels = [str(x) for x in top_departments.index]
        dept_values = [float(x) for x in top_departments.values]

        # Spend Over Time (by week)
        spend_time = (
            merged.groupby("week_num")["spend"]
            .sum()
            .sort_index()
        )

        week_labels = [str(x) for x in spend_time.index]
        week_values = [float(x) for x in spend_time.values]

        demo_income_labels, demo_income_values = _spend_by_category_for_chart(
            merged, "income_range", top_n=12
        )
        demo_children_labels, demo_children_values = _spend_by_category_for_chart(
            merged, "children", top_n=10
        )
        brand_ty_labels, brand_ty_values = _spend_by_category_for_chart(
            merged, "brand_ty", top_n=14
        )
        organic_labels, organic_values = _spend_by_category_for_chart(
            merged, "natural_organic_flag", top_n=8
        )

        return render_template(
            "dashboard.html",
            total_spend=total_spend,
            total_transactions=total_transactions,
            dept_labels=dept_labels,
            dept_values=dept_values,
            week_labels=week_labels,
            week_values=week_values,
            demo_income_labels=demo_income_labels,
            demo_income_values=demo_income_values,
            demo_children_labels=demo_children_labels,
            demo_children_values=demo_children_values,
            brand_ty_labels=brand_ty_labels,
            brand_ty_values=brand_ty_values,
            organic_labels=organic_labels,
            organic_values=organic_values,
        )

    except Exception as e:
        return f"Dashboard error: {e}{_sql_error_user_hint(e)}"


@app.route("/ml-insights")
@login_required
def ml_insights():
    try:
        merged = get_merged_for_analytics_from_sql()

        if merged.empty:
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
        customer_summary = merged.groupby("hshd_num").agg(
            total_spend=("spend", "sum"),
            total_units=("units", "sum"),
            total_baskets=("basket_num", "nunique"),
            active_weeks=("week_num", "nunique")
        ).reset_index()

        customer_summary["avg_basket_value"] = (
            customer_summary["total_spend"] / customer_summary["total_baskets"]
        ).round(2)

        # Create a target variable for CLV.
        # For this project, future CLV is estimated as 25% more than current total spend.
        customer_summary["estimated_future_clv"] = customer_summary["total_spend"] * 1.25

        # Features used by the Linear Regression model
        X = customer_summary[
            ["total_spend", "total_units", "total_baskets", "active_weeks", "avg_basket_value"]
        ]

        # Target variable
        y = customer_summary["estimated_future_clv"]

        # Train Linear Regression model
        clv_model = LinearRegression()
        clv_model.fit(X, y)

        # Predict CLV
        customer_summary["predicted_clv"] = clv_model.predict(X).round(2)

        top_clv = customer_summary.sort_values(
            by="predicted_clv",
            ascending=False
        ).head(10)

        # -----------------------------
        # 2. Basket Analysis
        # -----------------------------
        basket_pairs = (
            merged.groupby(["basket_num", "commodity"])
            .size()
            .reset_index(name="count")
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

        basket_results = [
            {
                "item_1": pair[0][0],
                "item_2": pair[0][1],
                "times_bought_together": pair[1]
            }
            for pair in top_pairs
        ]

        # Gradient Boosting on basket-level features (deliverable: ML for basket analysis)
        basket_gb_rows = []
        try:
            bf = merged.groupby("basket_num", as_index=False).agg(
                n_lines=("commodity", "count"),
                n_commodity=("commodity", "nunique"),
                n_department=("department", "nunique"),
                basket_spend=("spend", "sum"),
            )
            if len(bf) >= 40 and bf["basket_spend"].nunique() > 1:
                med_spend = bf["basket_spend"].median()
                bf["high_value_basket"] = (bf["basket_spend"] >= med_spend).astype(int)
                if bf["high_value_basket"].nunique() > 1:
                    Xb = bf[["n_lines", "n_commodity", "n_department"]]
                    yb = bf["high_value_basket"]
                    gb = GradientBoostingClassifier(
                        max_depth=3,
                        n_estimators=80,
                        learning_rate=0.08,
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
        latest_week = merged["week_num"].max()

        churn_data = merged.groupby("hshd_num").agg(
            last_purchase_week=("week_num", "max"),
            total_spend=("spend", "sum"),
            total_baskets=("basket_num", "nunique")
        ).reset_index()

        churn_data["weeks_since_last_purchase"] = (
            latest_week - churn_data["last_purchase_week"]
        )

        churn_data["churn_risk"] = churn_data["weeks_since_last_purchase"].apply(
            lambda x: "High Risk" if x >= 8 else "Low Risk"
        )

        high_risk_count = churn_data[churn_data["churn_risk"] == "High Risk"].shape[0]
        low_risk_count = churn_data[churn_data["churn_risk"] == "Low Risk"].shape[0]

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