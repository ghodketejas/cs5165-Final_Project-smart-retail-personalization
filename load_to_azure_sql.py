import pandas as pd
import pyodbc
import os

SERVER = "smartretailservergroup46.database.windows.net"
DATABASE = "smartretaildb"
USERNAME = "retailadmin"
PASSWORD = os.getenv("AZURE_SQL_PASSWORD")

HOUSEHOLDS_FILE = "data/400_households.csv"
PRODUCTS_FILE = "data/400_products.csv"
TRANSACTIONS_FILE = "data/400_transactions.csv"

conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={SERVER};"
    f"DATABASE={DATABASE};"
    f"UID={USERNAME};"
    f"PWD={PASSWORD};"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connection Timeout=120;"
)

conn = pyodbc.connect(conn_str)
cursor = conn.cursor()
cursor.fast_executemany = True


def clean_columns(df):
    df.columns = df.columns.str.strip().str.upper()
    return df


print("Loading households...")
households = clean_columns(pd.read_csv(HOUSEHOLDS_FILE))
households = households.rename(columns={"L": "LOYALTY_FLAG"})
households["HSHD_NUM"] = pd.to_numeric(households["HSHD_NUM"], errors="coerce")
households = households.dropna(subset=["HSHD_NUM"])
households["HSHD_NUM"] = households["HSHD_NUM"].astype(int)

cursor.execute("DELETE FROM Households")

household_rows = []

for _, row in households.iterrows():
    household_rows.append((
        int(row["HSHD_NUM"]),
        str(row["LOYALTY_FLAG"]).strip() if pd.notnull(row["LOYALTY_FLAG"]) else None,
        str(row["AGE_RANGE"]).strip() if pd.notnull(row["AGE_RANGE"]) else None,
        str(row["MARITAL"]).strip() if pd.notnull(row["MARITAL"]) else None,
        str(row["INCOME_RANGE"]).strip() if pd.notnull(row["INCOME_RANGE"]) else None,
        str(row["HOMEOWNER"]).strip() if pd.notnull(row["HOMEOWNER"]) else None,
        str(row["HSHD_COMPOSITION"]).strip() if pd.notnull(row["HSHD_COMPOSITION"]) else None,
        str(row["HH_SIZE"]).strip() if pd.notnull(row["HH_SIZE"]) else None,
        str(row["CHILDREN"]).strip() if pd.notnull(row["CHILDREN"]) else None,
    ))

cursor.executemany(
    """
    INSERT INTO Households
    (HSHD_NUM, LOYALTY_FLAG, AGE_RANGE, MARITAL_STATUS, INCOME_RANGE,
     HOMEOWNER_DESC, HSHD_COMPOSITION, HH_SIZE, CHILDREN)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
    household_rows,
)
conn.commit()
print(f"Inserted {len(household_rows)} household rows.")


print("Loading products...")
products = clean_columns(pd.read_csv(PRODUCTS_FILE))

cursor.execute("DELETE FROM Products")

product_rows = products[
    [
        "PRODUCT_NUM",
        "DEPARTMENT",
        "COMMODITY",
        "BRAND_TY",
        "NATURAL_ORGANIC_FLAG",
    ]
].where(pd.notnull(products), None).values.tolist()

cursor.executemany(
    """
    INSERT INTO Products
    (PRODUCT_NUM, DEPARTMENT, COMMODITY, BRAND_TY, NATURAL_ORGANIC_FLAG)
    VALUES (?, ?, ?, ?, ?)
    """,
    product_rows,
)
conn.commit()
print(f"Inserted {len(product_rows)} product rows.")


print("Loading transactions in chunks...")
cursor.execute("DELETE FROM Transactions")
conn.commit()

chunk_size = 10000
total_inserted = 0

for chunk in pd.read_csv(TRANSACTIONS_FILE, chunksize=chunk_size):
    chunk = clean_columns(chunk)
    chunk = chunk.rename(columns={"PURCHASE_": "PURCHASE_DATE"})

    chunk["PURCHASE_DATE"] = pd.to_datetime(
        chunk["PURCHASE_DATE"], errors="coerce"
    ).dt.date

    transaction_rows = chunk[
        [
            "BASKET_NUM",
            "HSHD_NUM",
            "PURCHASE_DATE",
            "PRODUCT_NUM",
            "SPEND",
            "UNITS",
            "STORE_R",
            "WEEK_NUM",
            "YEAR",
        ]
    ].where(pd.notnull(chunk), None).values.tolist()

    cursor.executemany(
        """
        INSERT INTO Transactions
        (BASKET_NUM, HSHD_NUM, PURCHASE_DATE, PRODUCT_NUM,
         SPEND, UNITS, STORE_R, WEEK_NUM, YEAR)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        transaction_rows,
    )

    conn.commit()
    total_inserted += len(transaction_rows)
    print(f"Inserted {total_inserted} transaction rows so far...")

cursor.close()
conn.close()

print("Azure SQL data loading completed successfully.")