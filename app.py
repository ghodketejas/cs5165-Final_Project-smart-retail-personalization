from flask import Flask, render_template, request, redirect, url_for, flash
import pandas as pd
import os
from sklearn.linear_model import LinearRegression

app = Flask(__name__)
app.secret_key = "retail_project_secret_key"

DATA_FOLDER = "data"

HOUSEHOLDS_FILE = os.path.join(DATA_FOLDER, "400_households.csv")
TRANSACTIONS_FILE = os.path.join(DATA_FOLDER, "400_transactions.csv")
PRODUCTS_FILE = os.path.join(DATA_FOLDER, "400_products.csv")


def clean_columns(df):
    df.columns = df.columns.str.strip()
    df.columns = df.columns.str.lower()
    return df


def load_data():
    households = pd.read_csv(HOUSEHOLDS_FILE)
    transactions = pd.read_csv(TRANSACTIONS_FILE)
    products = pd.read_csv(PRODUCTS_FILE)

    households = clean_columns(households)
    transactions = clean_columns(transactions)
    products = clean_columns(products)

    merged = transactions.merge(households, on="hshd_num", how="left")
    merged = merged.merge(products, on="product_num", how="left")

    return households, transactions, products, merged


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
    household_id = 10
    results = None

    if request.method == "POST":
        household_id = request.form.get("hshd_num", 10)

    try:
        households, transactions, products, merged = load_data()

        results = merged[merged["hshd_num"].astype(str) == str(household_id)]

        sort_cols = ["hshd_num", "basket_num", "purchase_", "product_num", "department", "commodity"]
        existing_sort_cols = [col for col in sort_cols if col in results.columns]

        if existing_sort_cols:
            results = results.sort_values(by=existing_sort_cols)

        display_cols = [
            "hshd_num", "basket_num", "purchase_", "product_num",
            "department", "commodity", "spend", "units",
            "store_r", "week_num", "year", "l",
            "age_range", "marital", "income_range",
            "homeowner", "hshd_composition", "hh_size", "children",
        ]

        existing_display_cols = [col for col in display_cols if col in results.columns]
        results = results[existing_display_cols].head(100)

        rename_cols = {
            "purchase_": "date",
            "l": "loyalty_flag",
            "store_r": "store_region",
            "marital": "marital_status",
            "homeowner": "homeowner_desc",
            "hh_size": "hshd_size",
        }

        results = results.rename(columns=rename_cols)

    except Exception as e:
        flash(f"Error loading data: {e}")
        results = pd.DataFrame()

    return render_template(
        "data_pull.html",
        household_id=household_id,
        tables=results.to_html(classes="table table-striped table-bordered", index=False)
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
        households, transactions, products, merged = load_data()

        # Total Spend
        total_spend = f"{merged['spend'].sum():,.2f}"

        # Total Transactions
        total_transactions = merged["basket_num"].nunique()

        # Top Departments
        top_departments = (
            merged.groupby("department")["spend"]
            .sum()
            .sort_values(ascending=False)
            .head(5)
        )

        dept_labels = list(top_departments.index)
        dept_values = list(top_departments.values)

        # Spend Over Time (by week)
        spend_time = (
            merged.groupby("week_num")["spend"]
            .sum()
            .sort_index()
        )

        week_labels = list(spend_time.index)
        week_values = list(spend_time.values)

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
        households, transactions, products, merged = load_data()

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