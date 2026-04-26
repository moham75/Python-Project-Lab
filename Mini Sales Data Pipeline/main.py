import argparse
import logging
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt


# -----------------------------
# Helper functions
# -----------------------------

def setup_logging(project_path: Path):
    logs_path = project_path / "logs"
    logs_path.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(logs_path / "pipeline.log"),
            logging.StreamHandler()
        ]
    )


def missing_text(series):
    return series.isna() | series.astype("string").str.strip().eq("").fillna(False)


# -----------------------------
# Phase 1: Load raw data
# -----------------------------

def load_raw_data(raw_path: Path):
    logging.info("Loading raw CSV files...")

    customers = pd.read_csv(raw_path / "raw_customers.csv")
    orders = pd.read_csv(raw_path / "raw_orders.csv")
    order_items = pd.read_csv(raw_path / "raw_order_items.csv")
    products = pd.read_csv(raw_path / "raw_products.csv")

    logging.info("Raw data loaded successfully.")

    return customers, orders, order_items, products


# -----------------------------
# Phase 2: Validate raw data
# -----------------------------

def validate_raw_data(customers, orders, order_items, products, reports_path: Path):
    logging.info("Running raw data validation checks...")

    reports_path.mkdir(parents=True, exist_ok=True)

    parsed_order_dates = pd.to_datetime(
        orders["order_date"],
        errors="coerce"
    )

    validation_report = {
        "Duplicate customers": customers["customer_id"].duplicated().sum(),

        "Missing customer cities": missing_text(customers["city"]).sum(),

        "Products with missing category": missing_text(products["category"]).sum(),

        "Products where cost > price": (
            products["unit_cost"] > products["unit_price"]
        ).sum(),

        "Invalid order dates": parsed_order_dates.isna().sum(),

        "Orders with invalid customer_id": (
            ~orders["customer_id"].isin(customers["customer_id"])
        ).sum(),

        "Invalid item quantities": (
            order_items["quantity"] <= 0
        ).sum(),

        "Items with invalid product_id": (
            ~order_items["product_id"].isin(products["product_id"])
        ).sum()
    }

    report_file = reports_path / "validation_report.txt"

    with open(report_file, "w") as file:
        file.write("Validation Report\n")
        file.write("-----------------\n")

        for check_name, result in validation_report.items():
            file.write(f"{check_name}: {result}\n")

    logging.info(f"Validation report saved to: {report_file}")

    return validation_report


# -----------------------------
# Phase 3: Clean data
# -----------------------------

def clean_data(customers, orders, order_items, products):
    logging.info("Cleaning raw data...")

    customers_clean = customers.copy()
    orders_clean = orders.copy()
    order_items_clean = order_items.copy()
    products_clean = products.copy()

    # Clean customers
    customers_clean = customers_clean.drop_duplicates(
        subset=["customer_id"],
        keep="first"
    )

    customers_clean.loc[
        missing_text(customers_clean["city"]),
        "city"
    ] = "Unknown"

    # Clean products
    products_clean = products_clean.dropna(subset=["product_id"])

    products_clean.loc[
        missing_text(products_clean["category"]),
        "category"
    ] = "Uncategorized"

    # Clean orders
    orders_clean["order_date"] = pd.to_datetime(
        orders_clean["order_date"],
        errors="coerce"
    )

    orders_clean = orders_clean.dropna(subset=["order_date"])

    orders_clean = orders_clean[
        orders_clean["customer_id"].isin(customers_clean["customer_id"])
    ]

    orders_clean = orders_clean.drop_duplicates(
        subset=["order_id"],
        keep="first"
    )

    # Clean order items
    order_items_clean = order_items_clean[
        order_items_clean["quantity"] > 0
    ]

    order_items_clean = order_items_clean[
        order_items_clean["product_id"].isin(products_clean["product_id"])
    ]

    order_items_clean = order_items_clean[
        order_items_clean["order_id"].isin(orders_clean["order_id"])
    ]

    logging.info("Data cleaning completed.")

    return customers_clean, orders_clean, order_items_clean, products_clean


def save_clean_data(customers_clean, orders_clean, order_items_clean, products_clean, clean_path: Path):
    logging.info("Saving cleaned data files...")

    clean_path.mkdir(parents=True, exist_ok=True)

    customers_clean.to_csv(clean_path / "clean_customers.csv", index=False)
    orders_clean.to_csv(clean_path / "clean_orders.csv", index=False)
    order_items_clean.to_csv(clean_path / "clean_order_items.csv", index=False)
    products_clean.to_csv(clean_path / "clean_products.csv", index=False)

    logging.info("Cleaned data files saved successfully.")


# -----------------------------
# Phase 4: Build sales fact
# -----------------------------

def build_sales_fact(customers_clean, orders_clean, order_items_clean, products_clean):
    logging.info("Building sales fact table...")

    sales_fact = orders_clean.merge(
        order_items_clean,
        on="order_id",
        how="inner"
    )

    sales_fact = sales_fact.merge(
        products_clean,
        on="product_id",
        how="inner"
    )

    sales_fact = sales_fact.merge(
        customers_clean,
        on="customer_id",
        how="inner"
    )

    sales_fact["month"] = sales_fact["order_date"].dt.to_period("M").astype(str)

    sales_fact["gross_sales"] = (
        sales_fact["quantity"] * sales_fact["unit_price_at_order"]
    )

    sales_fact["discount_amount"] = (
        sales_fact["gross_sales"] * sales_fact["discount_pct"] / 100
    )

    sales_fact["net_sales"] = (
        sales_fact["gross_sales"] - sales_fact["discount_amount"]
    )

    sales_fact["cost_amount"] = (
        sales_fact["quantity"] * sales_fact["unit_cost"]
    )

    sales_fact["profit"] = (
        sales_fact["net_sales"] - sales_fact["cost_amount"]
    )

    sales_fact = sales_fact[
        [
            "order_id",
            "order_date",
            "month",
            "customer_id",
            "customer_name",
            "city",
            "segment",
            "sales_channel",
            "status",
            "product_id",
            "product_name",
            "category",
            "quantity",
            "unit_price_at_order",
            "unit_cost",
            "gross_sales",
            "discount_amount",
            "net_sales",
            "cost_amount",
            "profit"
        ]
    ]

    logging.info(f"Sales fact table created with {len(sales_fact)} rows.")

    return sales_fact


# -----------------------------
# Phase 5: Create marts
# -----------------------------

def create_data_marts(sales_fact, marts_path: Path):
    logging.info("Creating data marts...")

    marts_path.mkdir(parents=True, exist_ok=True)

    monthly_kpis = sales_fact.groupby("month", as_index=False).agg(
        total_orders=("order_id", "nunique"),
        total_customers=("customer_id", "nunique"),
        gross_sales=("gross_sales", "sum"),
        net_sales=("net_sales", "sum"),
        profit=("profit", "sum")
    )

    monthly_kpis["avg_order_value"] = (
        monthly_kpis["net_sales"] / monthly_kpis["total_orders"]
    )

    product_summary = sales_fact.groupby(
        ["product_id", "product_name", "category"],
        as_index=False
    ).agg(
        quantity_sold=("quantity", "sum"),
        net_sales=("net_sales", "sum"),
        profit=("profit", "sum")
    )

    customer_summary = sales_fact.groupby(
        ["customer_id", "customer_name", "city", "segment"],
        as_index=False
    ).agg(
        total_orders=("order_id", "nunique"),
        net_sales=("net_sales", "sum"),
        first_order_date=("order_date", "min"),
        last_order_date=("order_date", "max")
    )

    monthly_kpis.to_csv(marts_path / "monthly_kpis.csv", index=False)
    product_summary.to_csv(marts_path / "product_summary.csv", index=False)
    customer_summary.to_csv(marts_path / "customer_summary.csv", index=False)

    logging.info("Data marts saved successfully.")


# -----------------------------
# Phase 6: Create charts
# -----------------------------

def create_charts(sales_fact, charts_path: Path):
    logging.info("Creating charts...")

    charts_path.mkdir(parents=True, exist_ok=True)

    # 1. Monthly net sales
    monthly_sales = sales_fact.groupby("month", as_index=False).agg(
        net_sales=("net_sales", "sum")
    )

    monthly_sales = monthly_sales.sort_values("month")

    plt.figure(figsize=(10, 5))
    plt.plot(monthly_sales["month"], monthly_sales["net_sales"], marker="o")
    plt.title("Monthly Net Sales")
    plt.xlabel("Month")
    plt.ylabel("Net Sales")
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(charts_path / "monthly_net_sales.png")
    plt.close()

    # 2. Sales by category
    category_sales = sales_fact.groupby("category", as_index=False).agg(
        net_sales=("net_sales", "sum")
    )

    category_sales = category_sales.sort_values("net_sales", ascending=False)

    plt.figure(figsize=(10, 5))
    plt.bar(category_sales["category"], category_sales["net_sales"])
    plt.title("Sales by Category")
    plt.xlabel("Category")
    plt.ylabel("Net Sales")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(charts_path / "sales_by_category.png")
    plt.close()

    # 3. Profit by channel
    channel_profit = sales_fact.groupby("sales_channel", as_index=False).agg(
        profit=("profit", "sum")
    )

    channel_profit = channel_profit.sort_values("profit", ascending=False)

    plt.figure(figsize=(8, 5))
    plt.bar(channel_profit["sales_channel"], channel_profit["profit"])
    plt.title("Profit by Channel")
    plt.xlabel("Sales Channel")
    plt.ylabel("Profit")
    plt.tight_layout()
    plt.savefig(charts_path / "profit_by_channel.png")
    plt.close()

    # 4. Top 10 customers
    top_customers = sales_fact.groupby(
        ["customer_id", "customer_name"],
        as_index=False
    ).agg(
        net_sales=("net_sales", "sum")
    )

    top_customers = top_customers.sort_values(
        "net_sales",
        ascending=False
    ).head(10)

    top_customers["customer_label"] = (
        top_customers["customer_name"]
        + " - "
        + top_customers["customer_id"].astype(str)
    )

    plt.figure(figsize=(10, 6))
    plt.bar(top_customers["customer_label"], top_customers["net_sales"])
    plt.title("Top 10 Customers by Net Sales")
    plt.xlabel("Customer")
    plt.ylabel("Net Sales")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(charts_path / "top_10_customers.png")
    plt.close()

    # 5. Orders by status
    status_summary = sales_fact.groupby("status", as_index=False).agg(
        total_orders=("order_id", "nunique")
    )

    status_summary = status_summary.sort_values(
        "total_orders",
        ascending=False
    )

    plt.figure(figsize=(8, 5))
    plt.bar(status_summary["status"], status_summary["total_orders"])
    plt.title("Orders by Status")
    plt.xlabel("Status")
    plt.ylabel("Total Orders")
    plt.tight_layout()
    plt.savefig(charts_path / "orders_by_status.png")
    plt.close()

    logging.info("Charts saved successfully.")


# -----------------------------
# Main pipeline
# -----------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Small Python sales data pipeline"
    )

    parser.add_argument(
        "--month",
        type=str,
        required=False,
        help="Run pipeline for one month only. Example: 2026-02"
    )

    args = parser.parse_args()

    project_path = Path(__file__).resolve().parent

    setup_logging(project_path)

    logging.info("Pipeline started.")

    raw_path = project_path / "data" / "raw"
    clean_path = project_path / "data" / "clean"
    analytics_path = project_path / "data" / "analytics"
    marts_path = project_path / "data" / "marts"
    charts_path = project_path / "data" / "charts"
    reports_path = project_path / "reports"

    analytics_path.mkdir(parents=True, exist_ok=True)

    customers, orders, order_items, products = load_raw_data(raw_path)

    validate_raw_data(
        customers,
        orders,
        order_items,
        products,
        reports_path
    )

    customers_clean, orders_clean, order_items_clean, products_clean = clean_data(
        customers,
        orders,
        order_items,
        products
    )

    save_clean_data(
        customers_clean,
        orders_clean,
        order_items_clean,
        products_clean,
        clean_path
    )

    sales_fact = build_sales_fact(
        customers_clean,
        orders_clean,
        order_items_clean,
        products_clean
    )

    if args.month:
        logging.info(f"Filtering sales fact for month: {args.month}")

        sales_fact = sales_fact[
            sales_fact["month"] == args.month
        ]

    if sales_fact.empty:
        logging.warning("No data found for the selected period.")
    else:
        sales_fact.to_csv(
            analytics_path / "sales_fact.csv",
            index=False
        )

        logging.info("sales_fact.csv saved successfully.")

        create_data_marts(sales_fact, marts_path)
        create_charts(sales_fact, charts_path)

    logging.info("Pipeline finished successfully.")


if __name__ == "__main__":
    main()