import pandas as pd
import os

data_path = "data/"

files = {
    "customers": "olist_customers_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "payments": "olist_order_payments_dataset.csv",
    "reviews": "olist_order_reviews_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "category_translation": "product_category_name_translation.csv",
}

print("=" * 50)
print("OLIST DATASET SUMMARY")
print("=" * 50)

total_rows = 0
for name, filename in files.items():
    filepath = os.path.join(data_path, filename)
    df = pd.read_csv(filepath)
    total_rows += len(df)
    print(f"{name:<25} {len(df):>8,} rows  |  {len(df.columns)} columns")

print("=" * 50)
print(f"{'TOTAL':<25} {total_rows:>8,} rows")
print("=" * 50)

