import pandas as pd
from pathlib import Path

raw_path = Path("D:/python projects/Mini Retail Sales Pipeline/data/raw")

customers_path = raw_path / "customers.csv"
orders_path = raw_path / "orders.csv"
products_path = raw_path / "products.csv"

customers = pd.read_csv(customers_path)
orders = pd.read_csv(orders_path)
products = pd.read_csv(products_path)


# creating a function which will clean customers data


# a cleanning function for customers
def clean_customers(customers):
    customers = customers.copy()

    customers["city"] = customers["city"].fillna("Unknown")
    customers = customers.drop_duplicates(subset=["customer_id"], keep="first")
    customers = customers.dropna(subset=["customer_name"])

    return customers


# a cleanning function for products
def clean_products(products):
    products = products.copy()

    products = products.dropna(subset=["product_id"])
    products["category"] = products["category"].fillna("Uncategorized")
    products["unit_cost"] = pd.to_numeric(products["unit_cost"], errors="coerce")
    products["unit_price"] = pd.to_numeric(products["unit_price"], errors="coerce")
    products = products.dropna(subset=["unit_cost", "unit_price"])

    return products


# a cleanning function for orders
def clean_orders(orders):
    orders = orders.copy()

    orders = orders.drop_duplicates(subset=["order_id"], keep="first")

    orders["quantity"] = pd.to_numeric(orders["quantity"], errors="coerce")
    orders = orders.dropna(subset=["quantity"])
    orders = orders[orders["quantity"] > 0]

    orders["order_date"] = pd.to_datetime(orders["order_date"], errors="coerce")
    orders = orders.dropna(subset=["order_date"])

    orders["discount_percent"] = pd.to_numeric(orders["discount_percent"], errors="coerce")
    orders["discount_percent"] = orders["discount_percent"].fillna(0)

    return orders


def create_validation_report(raw_orders, clean_orders, clean_products, clean_path):
    total_raw_orders = len(raw_orders)

    total_valid_orders = len(clean_orders)

    total_removed_duplicate_orders = raw_orders.duplicated(
        subset=["order_id"]
    ).sum()

    quantity_as_number = pd.to_numeric(raw_orders["quantity"], errors="coerce")

    total_removed_invalid_quantity_rows = (
        quantity_as_number.isna() | (quantity_as_number <= 0)
    ).sum()

    total_orders_with_missing_product_id = raw_orders["product_id"].isna().sum()

    valid_product_ids = set(clean_products["product_id"].dropna().astype(str))

    clean_order_product_ids = clean_orders["product_id"].astype(str)

    total_orders_with_invalid_product_id = (
        clean_orders["product_id"].notna()
        & ~clean_order_product_ids.isin(valid_product_ids)
    ).sum()

    report = f"""Validation Report
-----------------

Total raw orders: {total_raw_orders}
Total valid orders: {total_valid_orders}
Total removed duplicate orders: {total_removed_duplicate_orders}
Total removed invalid quantity rows: {total_removed_invalid_quantity_rows}
Total orders with missing product_id: {total_orders_with_missing_product_id}
Total orders with invalid product_id: {total_orders_with_invalid_product_id}
"""

    report_path = clean_path / "validation_report.txt"

    with open(report_path, "w") as file:
        file.write(report)

    print("Validation report created successfully.")


clean_path = Path("D:/python projects/Mini Retail Sales Pipeline/data/processed")
clean_path.mkdir(parents=True, exist_ok=True)

customers_clean = clean_customers(customers)
products_clean = clean_products(products)
orders_clean = clean_orders(orders)

create_validation_report(
    raw_orders=orders,
    clean_orders=orders_clean,
    clean_products=products_clean,
    clean_path=clean_path
)

customers_clean.to_csv(clean_path / "customers_clean.csv", index=False)
products_clean.to_csv(clean_path / "products_clean.csv", index=False)
orders_clean.to_csv(clean_path / "orders_clean.csv", index=False)

# creating a fct table which hold all of the datasets

def build_sales_fact(orders, customers, products):
    fct_sales = orders.merge(customers, on="customer_id", how="inner")
    fct_sales = fct_sales.merge(products, on="product_id", how="inner")

    fct_sales["month"] = fct_sales["order_date"].dt.to_period("M").dt.to_timestamp()
    fct_sales["gross_sales"] = fct_sales["quantity"] * fct_sales["unit_price"]
    fct_sales["discount_amount"] = fct_sales["gross_sales"] * fct_sales["discount_percent"] / 100
    fct_sales["net_sales"] = fct_sales["gross_sales"] - fct_sales["discount_amount"]
    fct_sales["total_cost"] = fct_sales["quantity"] * fct_sales["unit_cost"]
    fct_sales["profit"] = fct_sales["net_sales"] - fct_sales["total_cost"]
    fct_sales["profit_margin"] = fct_sales["profit"] / fct_sales["net_sales"].replace(0, pd.NA)

    return fct_sales


fct_sales = build_sales_fact(orders_clean, customers_clean, products_clean)
fct_sales.to_csv(clean_path / "fct_sales.csv", index=False)


# creating a function for monthly KPIs
def create_monthly_kpis(fct_sales):
    monthly_kpis = (
        fct_sales[fct_sales["order_status"] == "Completed"]
        .groupby("month")
        .agg(
            total_orders=("order_id", "count"),
            total_customers=("customer_id", "nunique"),
            gross_sales=("gross_sales", "sum"),
            net_sales=("net_sales", "sum"),
            profit=("profit", "sum")
        ).reset_index())
    monthly_kpis["average_order_value"] = monthly_kpis["net_sales"] / monthly_kpis["total_orders"]
    return monthly_kpis


# creating a function for category kpis
def create_category_kpis(fct_sales):
    category_kpis = (
        fct_sales[fct_sales["order_status"] == "Completed"]
        .groupby("category").agg(
            total_orders=("order_id", "count"),
            total_quantity=("quantity", "sum"),
            net_sales=("net_sales", "sum"),
            total_profit=("profit", "sum")
        )
        .reset_index())

    category_kpis["profit_margin"] = category_kpis["total_profit"] / category_kpis["net_sales"].replace(0, pd.NA)

    return category_kpis


def create_customer_summary(fct_sales):
    customer_summary = (
        fct_sales[fct_sales["order_status"] == "Completed"]
        .groupby(["customer_id", "customer_name", "city"])
        .agg(
            total_orders=("order_id", "count"),
            total_spent=("net_sales", "sum"),
            total_profit=("profit", "sum")
        )
        .reset_index())

    customer_segments = []

    for total_spent in customer_summary["total_spent"]:
        if total_spent >= 1500:
            customer_segments.append("High Value")
        elif total_spent >= 800:
            customer_segments.append("Medium Value")
        else:
            customer_segments.append("Low Value")

    customer_summary["customer_segment"] = customer_segments

    return customer_summary


monthly_kpis = create_monthly_kpis(fct_sales)
category_kpis = create_category_kpis(fct_sales)
customer_summary = create_customer_summary(fct_sales)

monthly_kpis.to_csv(clean_path / "monthly_kpis.csv", index=False)
category_kpis.to_csv(clean_path / "category_kpis.csv", index=False)
customer_summary.to_csv(clean_path / "customer_summary.csv", index=False)

print("Pipeline finished successfully.")
