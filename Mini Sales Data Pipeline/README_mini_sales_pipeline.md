# Mini Sales Data Pipeline & Analytics Project

## Project Overview

This is a small-scale **Data Engineering + Data Analysis** project using **Python only**.

The goal is to build a simple CSV-based data pipeline that reads raw sales data, validates it, cleans it, transforms it into analytical tables, and produces business insights.

You will work with raw CSV files representing customers, products, orders, and order items.

---

## Business Scenario

A small online business sells products through different sales channels such as:

- Website
- Mobile App
- Marketplace
- Partner Sales

The business wants to understand:

- How much revenue it generated
- Which products perform best
- Which customers spend the most
- Which sales channels are most profitable
- Whether the raw data contains quality issues

Your role is to build a Python pipeline that turns raw CSV files into clean and useful analytical outputs.

---

## Technologies Used

Only Python is required.

Recommended Python libraries:

```python
pandas
pathlib
matplotlib
argparse
logging
```

Optional:

```python
numpy
```

---

## Dataset Files

The project uses four raw CSV files:

| File | Description |
|---|---|
| `raw_customers.csv` | Customer master data |
| `raw_products.csv` | Product master data |
| `raw_orders.csv` | Order-level data |
| `raw_order_items.csv` | Order item-level data |

---

## Recommended Folder Structure

```text
mini_sales_pipeline/
│
├── data/
│   ├── raw/
│   │   ├── raw_customers.csv
│   │   ├── raw_products.csv
│   │   ├── raw_orders.csv
│   │   └── raw_order_items.csv
│   │
│   ├── clean/
│   │   ├── customers_clean.csv
│   │   ├── products_clean.csv
│   │   ├── orders_clean.csv
│   │   └── order_items_clean.csv
│   │
│   └── marts/
│       ├── sales_fact.csv
│       ├── monthly_kpis.csv
│       ├── product_summary.csv
│       └── customer_summary.csv
│
├── analysis_charts/
│
├── notebooks/
│   └── analysis.ipynb
│
├── src/
│   ├── extract.py
│   ├── validate.py
│   ├── clean.py
│   ├── transform.py
│   └── analyze.py
│
├── validation_report.txt
├── main.py
└── README.md
```

---

# Project Tasks

## Task 1: Extract Raw Data

Read all raw CSV files into Python using `pandas`.

Files to load:

```text
raw_customers.csv
raw_products.csv
raw_orders.csv
raw_order_items.csv
```

Expected result:

```text
Customers loaded successfully
Products loaded successfully
Orders loaded successfully
Order items loaded successfully
```

Skills practised:

- Reading CSV files
- Using `pandas.read_csv()`
- Using `pathlib.Path`
- Organising file paths

---

## Task 2: Validate Raw Data

Create validation checks before cleaning the data.

Validation checks to perform:

| Check | Table |
|---|---|
| Duplicate customer IDs | customers |
| Missing customer city | customers |
| Missing product category | products |
| Product cost greater than product price | products |
| Invalid order dates | orders |
| Orders with non-existing customer IDs | orders |
| Duplicate order IDs | orders |
| Negative or zero quantities | order items |
| Order items with non-existing product IDs | order items |
| Order items with non-existing order IDs | order items |

Save the result into:

```text
validation_report.txt
```

Example validation report:

```text
Validation Report
-----------------
Duplicate customers: 1
Missing customer cities: 3
Products with missing category: 1
Products where cost > price: 1
Invalid order dates: 1
Orders with invalid customer_id: 1
Duplicate orders: 1
Invalid item quantities: 5
Items with invalid product_id: 1
Items with invalid order_id: 1
```

Skills practised:

- Data quality checks
- Duplicate detection
- Null value detection
- Foreign key validation
- Boolean filtering

---

## Task 3: Clean the Data

Apply cleaning rules to each raw table.

### Customer Cleaning Rules

| Problem | Action |
|---|---|
| Duplicate customer ID | Keep first record |
| Missing city | Replace with `Unknown` |
| Missing segment | Replace with `Unknown` |

Output file:

```text
customers_clean.csv
```

---

### Product Cleaning Rules

| Problem | Action |
|---|---|
| Missing category | Replace with `Uncategorized` |
| Product cost greater than product price | Keep the row but flag it, or remove it |
| Duplicate product ID | Keep first record |

Output file:

```text
products_clean.csv
```

---

### Orders Cleaning Rules

| Problem | Action |
|---|---|
| Invalid order date | Remove row |
| Invalid customer ID | Remove row |
| Duplicate order ID | Keep first record |
| Missing status | Replace with `Unknown` |

Output file:

```text
orders_clean.csv
```

---

### Order Items Cleaning Rules

| Problem | Action |
|---|---|
| Negative or zero quantity | Remove row |
| Invalid product ID | Remove row |
| Invalid order ID | Remove row |
| Duplicate order item ID | Keep first record |

Output file:

```text
order_items_clean.csv
```

Skills practised:

- Cleaning messy data
- Handling missing values
- Removing invalid records
- Deduplication
- Saving cleaned CSVs

---

## Task 4: Build the Sales Fact Table

Create one main analytical table called:

```text
sales_fact.csv
```

This table should join:

```text
orders_clean.csv
order_items_clean.csv
products_clean.csv
customers_clean.csv
```

Recommended columns:

```text
order_id
order_date
month
customer_id
customer_name
city
segment
sales_channel
status
product_id
product_name
category
quantity
unit_price_at_order
unit_cost
gross_sales
discount_amount
net_sales
cost_amount
profit
```

Business formulas:

```text
gross_sales = quantity * unit_price_at_order

discount_amount = gross_sales * discount_pct / 100

net_sales = gross_sales - discount_amount

cost_amount = quantity * unit_cost

profit = net_sales - cost_amount
```

Skills practised:

- Joining multiple datasets
- Creating calculated columns
- Building a fact table
- Preparing data for analytics

---

## Task 5: Build Analytical Mart Tables

Create smaller summary tables from the sales fact table.

---

### 1. Monthly KPIs

Output file:

```text
monthly_kpis.csv
```

Grouped by:

```text
month
```

Columns:

```text
month
total_orders
total_customers
gross_sales
net_sales
profit
avg_order_value
```

---

### 2. Product Summary

Output file:

```text
product_summary.csv
```

Grouped by:

```text
product_id
product_name
category
```

Columns:

```text
product_id
product_name
category
quantity_sold
net_sales
profit
profit_margin
```

Formula:

```text
profit_margin = profit / net_sales
```

---

### 3. Customer Summary

Output file:

```text
customer_summary.csv
```

Grouped by:

```text
customer_id
customer_name
city
segment
```

Columns:

```text
customer_id
customer_name
city
segment
total_orders
total_spent
total_profit
first_order_date
last_order_date
```

Skills practised:

- Grouping data
- Aggregating KPIs
- Creating data marts
- Preparing business reporting tables

---

## Task 6: Analyse the Business

Use the clean marts to answer these questions:

1. What is the total net sales?
2. What is the total profit?
3. Which month had the highest revenue?
4. Which product category generated the most revenue?
5. Who are the top 10 customers by total spending?
6. Which sales channel performs best?
7. Which products have negative profit?
8. What is the average order value?
9. Which city has the highest customer value?
10. What percentage of orders were refunded or cancelled?

Skills practised:

- Business analysis
- KPI interpretation
- Ranking data
- Filtering data
- Summarising insights

---

## Task 7: Create Charts

Create simple visualisations using Python.

Recommended charts:

| Chart | Purpose |
|---|---|
| Monthly net sales line chart | Show revenue trend |
| Sales by category bar chart | Compare product categories |
| Profit by channel bar chart | Compare channels |
| Top 10 customers bar chart | Show best customers |
| Order status bar chart | Show completed, cancelled, and refunded orders |

Save charts into:

```text
analysis_charts/
```

Skills practised:

- Plotting with Python
- Communicating insights visually
- Exporting charts as image files

---

# Optional Challenge Tasks

After completing the basic version, improve the project with these extra tasks.

## Challenge 1: Create a Pipeline Runner

Create a `main.py` file that runs the full pipeline:

```text
extract
validate
clean
transform
analyze
```

Example command:

```bash
python main.py
```

---

## Challenge 2: Add Logging

Add log messages such as:

```text
[INFO] Reading raw files
[INFO] Validating data
[INFO] Cleaning customers table
[INFO] Building sales fact table
[INFO] Creating monthly KPIs
[INFO] Pipeline completed successfully
```

---

## Challenge 3: Add Month Filter

Allow the user to run the pipeline for one specific month.

Example:

```bash
python main.py --month 2026-02
```

---

## Challenge 4: Add Data Quality Failure Rules

Stop the pipeline if serious data quality issues exist.

Example:

```text
Stop the pipeline if more than 10% of order items have invalid quantities.
Stop the pipeline if more than 5% of orders have invalid customer IDs.
```

---

# Expected Final Outputs

At the end of the project, your output folder should contain:

```text
data/clean/customers_clean.csv
data/clean/products_clean.csv
data/clean/orders_clean.csv
data/clean/order_items_clean.csv

data/marts/sales_fact.csv
data/marts/monthly_kpis.csv
data/marts/product_summary.csv
data/marts/customer_summary.csv

validation_report.txt

analysis_charts/monthly_net_sales.png
analysis_charts/sales_by_category.png
analysis_charts/profit_by_channel.png
analysis_charts/top_10_customers.png
analysis_charts/order_status.png
```

---

# Learning Objectives

By completing this project, you will practise:

- Building a Python data pipeline
- Reading and writing CSV files
- Validating raw data
- Cleaning messy data
- Joining multiple datasets
- Creating fact and mart tables
- Calculating business KPIs
- Creating simple charts
- Structuring a portfolio-ready Python project

---

# Portfolio Description

You can describe this project on your CV or GitHub like this:

```text
Built a Python-based sales analytics pipeline that extracts raw CSV data, validates and cleans customer, product, order, and order item datasets, creates a sales fact table and analytical marts, and generates business KPIs and visualisations for revenue, profit, customer, product, and channel performance.
```

---

# Suggested GitHub Project Name

```text
python-mini-sales-pipeline
```

Alternative names:

```text
csv-sales-analytics-pipeline
python-sales-data-pipeline
mini-retail-analytics-python
```
