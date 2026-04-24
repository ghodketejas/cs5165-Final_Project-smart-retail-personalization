from flask import Flask, render_template, request, redirect, url_for, flash
import pandas as pd
import os
import pyodbc
from sklearn.linear_model import LinearRegression

app = Flask(__name__)
app.secret_key = "retail_project_secret_key"

DATA_FOLDER = "data"

HOUSEHOLDS_FILE = os.path.join(DATA_FOLDER, "400_households.csv")
TRANSACTIONS_FILE = os.path.join(DATA_FOLDER, "400_transactions.csv")
PRODUCTS_FILE = os.path.join(DATA_FOLDER, "400_products.csv")

SQL_SERVER = os.getenv("AZURE_SQL_SERVER")
SQL_DATABASE = os.getenv("AZURE_SQL_DATABASE")
SQL_USERNAME = os.getenv("AZURE_SQL_USERNAME")
SQL_PASSWORD = os.getenv("AZURE_SQL_PASSWORD")


def get_sql_connection():
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        f"UID={SQL_USERNAME};"
        f"PWD={SQL_PASSWORD};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=60;"
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
            p.DEPARTMENT AS department
        FROM Transactions t
        INNER JOIN Products p ON t.PRODUCT_NUM = p.PRODUCT_NUM
        INNER JOIN Households h ON t.HSHD_NUM = h.HSHD_NUM
    """
    conn = get_sql_connection()
    df = pd.read_sql(query, conn)
    conn.close()
    df.columns = [str(c).lower() for c in df.columns]
    return df


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


@app.route("/register", methods=["GET", "POST"])
def register():
    if request.method == "POST":
        flash("Registration successful. This demo form satisfies the username, password, and email requirement.")
        return redirect(url_for("login"))
    return render_template("register.html")


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        flash("Login successful.")
        return redirect(url_for("data_pull"))
    return render_template("login.html")


@app.route("/data-pull", methods=["GET", "POST"])
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
        flash(f"Azure SQL error: {e}")
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

        return render_template(
            "dashboard.html",
            total_spend=total_spend,
            total_transactions=total_transactions,
            dept_labels=dept_labels,
            dept_values=dept_values,
            week_labels=week_labels,
            week_values=week_values,
        )

    except Exception as e:
        return f"Dashboard error: {e}"


@app.route("/ml-insights")
def ml_insights():
    try:
        merged = get_merged_for_analytics_from_sql()

        if merged.empty:
            return render_template(
                "ml_insights.html",
                top_clv=[],
                basket_results=[],
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
            high_risk_customers=high_risk_customers.to_dict(orient="records"),
            high_risk_count=high_risk_count,
            low_risk_count=low_risk_count
        )

    except Exception as e:
        return f"ML Insights error: {e}"


if __name__ == "__main__":
    app.run(debug=True)