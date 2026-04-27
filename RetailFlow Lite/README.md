# RetailFlow Lite

A small Python-only retail data pipeline that cleans raw CSV files, builds a sales fact table, and generates business KPI reports using loops and functions.

## Project Objective

The goal of this project is to practise core Python skills in a realistic data workflow.

This project focuses on:

- Reading CSV files
- Using loops
- Writing reusable functions
- Cleaning raw data
- Joining datasets manually
- Creating calculated business columns
- Producing simple KPI outputs
- Saving processed files as CSV

This is a beginner-friendly project, but it still follows a real data engineering and data analysis workflow.

---

## Business Scenario

A small retail company has three raw CSV files:

1. `customers.csv`
2. `products.csv`
3. `orders.csv`

The business wants to understand:

- Monthly sales performance
- Sales by product category
- Customer spending behaviour
- Profit and profit margin
- Data quality issues in the raw files

Your task is to build a small Python pipeline that reads the raw files, cleans them, creates a final sales fact table, and generates analysis outputs.

---

## Tech Stack

This project uses Python only.

Recommended libraries:

```python
csv
datetime
pathlib
collections
```

Optional later improvement:

```python
pandas
matplotlib
```

For the first version, avoid pandas so you can practise loops and functions properly.

---

## Project Structure

```text
retailflow_lite/
│
├── data/
│   ├── raw/
│   │   ├── customers.csv
│   │   ├── products.csv
│   │   └── orders.csv
│   │
│   └── processed/
│       ├── sales_fact.csv
│       ├── monthly_kpis.csv
│       ├── category_kpis.csv
│       └── customer_summary.csv
│
├── reports/
│   └── validation_report.txt
│
├── main.py
└── README.md
```

---

## Datasets

### 1. customers.csv

Contains customer information.

| Column | Description |
|---|---|
| customer_id | Unique customer ID |
| customer_name | Customer name |
| city | Customer city |
| signup_date | Date when the customer signed up |

Cleaning rules:

| Issue | Rule |
|---|---|
| Missing city | Replace with `Unknown` |
| Duplicate customer ID | Keep the first record |
| Empty customer name | Remove the row |

---

### 2. products.csv

Contains product information.

| Column | Description |
|---|---|
| product_id | Unique product ID |
| product_name | Product name |
| category | Product category |
| unit_cost | Product cost |
| unit_price | Selling price |

Cleaning rules:

| Issue | Rule |
|---|---|
| Missing category | Replace with `Uncategorized` |
| Missing product_id | Remove the row |
| unit_cost | Convert to float |
| unit_price | Convert to float |

---

### 3. orders.csv

Contains order transactions.

| Column | Description |
|---|---|
| order_id | Unique order ID |
| order_date | Date of the order |
| customer_id | Customer ID |
| product_id | Product ID |
| quantity | Number of items ordered |
| discount_percent | Discount percentage |
| order_status | Completed, Cancelled, or Refunded |
| payment_method | Payment method used |

Cleaning rules:

| Issue | Rule |
|---|---|
| Duplicate order_id | Keep the first record |
| Empty quantity | Remove the row |
| Negative quantity | Remove the row |
| Invalid order_date | Remove the row or report it |
| Invalid product_id | Add it to the validation report |
| discount_percent | Convert to float |

---

## Main Tasks

### Task 1: Read CSV Files

Create a function:

```python
def read_csv_file(file_path):
    pass
```

The function should:

1. Open a CSV file.
2. Read all rows.
3. Return the data as a list of dictionaries.

Expected structure:

```python
[
    {
        "order_id": "O1001",
        "order_date": "2026-01-03",
        "customer_id": "C001",
        "product_id": "P001",
        "quantity": "2"
    }
]
```

---

### Task 2: Clean Customers

Create a function:

```python
def clean_customers(customers):
    pass
```

The function should return a cleaned list of customer records.

---

### Task 3: Clean Products

Create a function:

```python
def clean_products(products):
    pass
```

The function should return a cleaned list of product records.

---

### Task 4: Clean Orders

Create a function:

```python
def clean_orders(orders):
    pass
```

The function should return:

1. Cleaned orders
2. Validation information

Example:

```python
cleaned_orders, validation_errors = clean_orders(orders)
```

---

### Task 5: Build Sales Fact Table

Create a function:

```python
def build_sales_fact(orders, customers, products):
    pass
```

This function joins:

```text
orders + customers + products
```

The output should be a sales fact table.

Expected columns:

| Column |
|---|
| order_id |
| order_date |
| order_month |
| customer_id |
| customer_name |
| city |
| product_id |
| product_name |
| category |
| quantity |
| unit_price |
| unit_cost |
| discount_percent |
| order_status |
| payment_method |
| gross_sales |
| discount_amount |
| net_sales |
| total_cost |
| profit |
| profit_margin |

---

## Custom Columns

### order_month

Required column:

| Column | Source |
|---|---|
| order_date | orders.csv |

Formula:

```python
order_month = order_date[:7]
```

Example:

```text
2026-01-03 -> 2026-01
```

---

### gross_sales

Required columns:

| Column | Source |
|---|---|
| quantity | orders.csv |
| unit_price | products.csv |

Formula:

```python
gross_sales = quantity * unit_price
```

---

### discount_amount

Required columns:

| Column | Source |
|---|---|
| gross_sales | custom column |
| discount_percent | orders.csv |

Formula:

```python
discount_amount = gross_sales * discount_percent / 100
```

---

### net_sales

Required columns:

| Column | Source |
|---|---|
| gross_sales | custom column |
| discount_amount | custom column |

Formula:

```python
net_sales = gross_sales - discount_amount
```

---

### total_cost

Required columns:

| Column | Source |
|---|---|
| quantity | orders.csv |
| unit_cost | products.csv |

Formula:

```python
total_cost = quantity * unit_cost
```

---

### profit

Required columns:

| Column | Source |
|---|---|
| net_sales | custom column |
| total_cost | custom column |

Formula:

```python
profit = net_sales - total_cost
```

---

### profit_margin

Required columns:

| Column | Source |
|---|---|
| profit | custom column |
| net_sales | custom column |

Formula:

```python
profit_margin = profit / net_sales
```

Important: avoid division by zero.

---

## Task 6: Create Monthly KPIs

Create a function:

```python
def create_monthly_kpis(sales_fact):
    pass
```

Group by:

```text
order_month
```

Only include:

```text
order_status = Completed
```

Output columns:

| Column | Description |
|---|---|
| order_month | Month of the order |
| total_orders | Number of completed orders |
| total_quantity | Total quantity sold |
| gross_sales | Total gross sales |
| net_sales | Total net sales |
| total_profit | Total profit |
| average_order_value | Net sales divided by total orders |

Formula:

```python
average_order_value = net_sales / total_orders
```

Output file:

```text
data/processed/monthly_kpis.csv
```

---

## Task 7: Create Category KPIs

Create a function:

```python
def create_category_kpis(sales_fact):
    pass
```

Group by:

```text
category
```

Only include completed orders.

Output columns:

| Column | Description |
|---|---|
| category | Product category |
| total_orders | Number of completed orders |
| total_quantity | Total quantity sold |
| net_sales | Total net sales |
| total_profit | Total profit |
| profit_margin | Profit divided by net sales |

Output file:

```text
data/processed/category_kpis.csv
```

---

## Task 8: Create Customer Summary

Create a function:

```python
def create_customer_summary(sales_fact):
    pass
```

Group by:

```text
customer_id
customer_name
city
```

Only include completed orders.

Output columns:

| Column | Description |
|---|---|
| customer_id | Customer ID |
| customer_name | Customer name |
| city | Customer city |
| total_orders | Number of completed orders |
| total_spent | Total net sales |
| total_profit | Total profit |
| customer_segment | Customer value segment |

Customer segment logic:

```python
if total_spent >= 1500:
    customer_segment = "High Value"
elif total_spent >= 800:
    customer_segment = "Medium Value"
else:
    customer_segment = "Low Value"
```

Output file:

```text
data/processed/customer_summary.csv
```

---

## Task 9: Create Validation Report

Create a function:

```python
def create_validation_report(validation_info):
    pass
```

The report should include:

```text
Validation Report
-----------------

Total raw orders:
Total valid orders:
Total removed duplicate orders:
Total removed invalid quantity rows:
Total orders with missing product_id:
Total orders with invalid product_id:
```

Output file:

```text
reports/validation_report.txt
```

---

## Final Expected Outputs

After running the project, these files should be created:

```text
data/processed/sales_fact.csv
data/processed/monthly_kpis.csv
data/processed/category_kpis.csv
data/processed/customer_summary.csv
reports/validation_report.txt
```

---

## Suggested Execution Flow

Your `main.py` should follow this order:

```python
from pathlib import Path

BASE_DIR = Path(__file__).parent
RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
REPORTS_DIR = BASE_DIR / "reports"

customers = read_csv_file(RAW_DIR / "customers.csv")
products = read_csv_file(RAW_DIR / "products.csv")
orders = read_csv_file(RAW_DIR / "orders.csv")

customers_clean = clean_customers(customers)
products_clean = clean_products(products)
orders_clean, validation_info = clean_orders(orders)

sales_fact = build_sales_fact(
    orders_clean,
    customers_clean,
    products_clean
)

monthly_kpis = create_monthly_kpis(sales_fact)
category_kpis = create_category_kpis(sales_fact)
customer_summary = create_customer_summary(sales_fact)

write_csv_file(PROCESSED_DIR / "sales_fact.csv", sales_fact)
write_csv_file(PROCESSED_DIR / "monthly_kpis.csv", monthly_kpis)
write_csv_file(PROCESSED_DIR / "category_kpis.csv", category_kpis)
write_csv_file(PROCESSED_DIR / "customer_summary.csv", customer_summary)

create_validation_report(validation_info)
```

---

## Skills Practised

### Data Engineering Skills

- Reading raw files
- Cleaning data
- Validating records
- Building a simple pipeline
- Creating processed output files
- Writing a validation report

### Data Analysis Skills

- Creating sales KPIs
- Grouping by month
- Grouping by category
- Grouping by customer
- Calculating revenue, profit, and margin
- Creating customer segments

### Python Skills

- Loops
- Functions
- Dictionaries
- Lists
- CSV file handling
- Type conversion
- Date validation
- Basic aggregation logic

---

## Future Improvements

After finishing the first version, you can improve the project by adding:

1. A pandas version of the same pipeline
2. Charts using matplotlib
3. A command-line argument to run the pipeline for one month only
4. Logging messages
5. More validation checks
6. Unit tests for each function
7. A small dashboard using Streamlit

---

## Example Command

```bash
python main.py
```

Optional future version:

```bash
python main.py --month 2026-02
```

---

## Project Summary

RetailFlow Lite is a small but practical project that helps you understand how raw CSV data becomes useful business reporting data.

It is intentionally simple so you can focus on the most important Python fundamentals:

```text
loops + functions + clean logic
```
