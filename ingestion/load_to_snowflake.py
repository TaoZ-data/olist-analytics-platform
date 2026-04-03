import os
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

def get_snowflake_connection():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )

def create_table_and_load(conn, table_name, df):
    cursor = conn.cursor()

    # Build CREATE TABLE statement from dataframe columns
    col_defs = ", ".join([f'"{col.upper()}" VARCHAR' for col in df.columns])
    create_sql = f'CREATE OR REPLACE TABLE OLIST_DB.RAW.{table_name} ({col_defs});'
    cursor.execute(create_sql)
    print(f"  Table {table_name} created.")

    # Insert rows in batches
    cols = ", ".join([f'"{col.upper()}"' for col in df.columns])
    placeholders = ", ".join(["%s"] * len(df.columns))
    insert_sql = f"INSERT INTO OLIST_DB.RAW.{table_name} ({cols}) VALUES ({placeholders})"

    batch_size = 5000
    total = len(df)
    for i in range(0, total, batch_size):
        batch = df.iloc[i:i+batch_size].values.tolist()
        batch = [[None if pd.isna(v) else str(v) for v in row] for row in batch]
        cursor.executemany(insert_sql, batch)
        print(f"  Loaded {min(i+batch_size, total):,} / {total:,} rows...", end="\r")

    print(f"  Done: {total:,} rows loaded into {table_name}.")
    cursor.close()

def main():
    files = {
        "RAW_CUSTOMERS":            "olist_customers_dataset.csv",
        "RAW_ORDERS":               "olist_orders_dataset.csv",
        "RAW_ORDER_ITEMS":          "olist_order_items_dataset.csv",
        "RAW_ORDER_PAYMENTS":       "olist_order_payments_dataset.csv",
        "RAW_ORDER_REVIEWS":        "olist_order_reviews_dataset.csv",
        "RAW_PRODUCTS":             "olist_products_dataset.csv",
        "RAW_SELLERS":              "olist_sellers_dataset.csv",
        "RAW_GEOLOCATION":          "olist_geolocation_dataset.csv",
        "RAW_CATEGORY_TRANSLATION": "product_category_name_translation.csv",
    }

    print("Connecting to Snowflake...")
    conn = get_snowflake_connection()
    print("Connected!\n")

    for table_name, filename in files.items():
        filepath = os.path.join("data", filename)
        print(f"Loading {filename}...")
        df = pd.read_csv(filepath)
        create_table_and_load(conn, table_name, df)
        print()

    conn.close()
    print("All tables loaded successfully!")

if __name__ == "__main__":
    main()