# Sales & Opportunities Data Pipeline (Medallion Architecture)

## Overview

This project implements a complete **data pipeline using the Medallion Architecture (Bronze → Silver → Gold)** to process and analyze Australian sales and opportunity data.

The goal of the pipeline is to take raw operational data, clean and standardize it, and transform it into **business-ready analytical tables** that can be used for dashboards, reporting, and analysis.

The pipeline was built using **PySpark and Delta Lake in Databricks**, and the final data model supports BI tools such as **Power BI or Tableau**.

---

# Architecture

The pipeline follows the Medallion architecture:

Bronze → Silver → Gold

Each layer has a specific responsibility.

---

# Bronze Layer – Raw Data

The Bronze layer stores the raw source data exactly as it arrives.  
No transformations are applied at this stage.

This layer acts as a **historical record of the source system** and allows the pipeline to replay or reprocess data if necessary.

Typical tables in this layer include:

- `bronze_customers`
- `bronze_orders`
- `bronze_opportunities`

---

# Silver Layer – Cleansed & Conformed Data

The Silver layer focuses on **data quality and standardization**.

In this stage the raw Bronze data is:

- cleaned
- standardized
- validated
- deduplicated

The goal is to create **trusted datasets** that can safely be used for analytics.

Examples of transformations applied in this layer:

- Removing duplicate records
- Standardizing text values
- Converting data types
- Validating key business fields
- Flagging data quality issues

Rather than deleting problematic records, the pipeline adds **data quality flags** such as:

- invalid sales representatives
- orphan customers
- duplicate records
- invalid states

This approach keeps the data transparent and easier to audit.

---

# Gold Layer – Business-Level Analytics

The Gold layer transforms the curated Silver data into **analytical models optimized for reporting and dashboards**.

The data is organized using a **star schema** consisting of fact tables and dimension tables.

---

## Fact Tables

Fact tables contain measurable business events.

- `orders_fact` – completed sales transactions
- `opportunities_fact` – sales pipeline data

These tables store metrics such as:

- revenue
- quantity sold
- opportunity value

---

## Dimension Tables

Dimension tables provide descriptive context for analysis.

- `dim_customer`
- `dim_sales_rep`
- `dim_product`
- `dim_date`
- `dim_phase`

These tables make it easier to analyze the data by customer, time, product, or sales representative.

---

# Data Modeling Approach

The Gold layer follows a **star schema design**, where fact tables reference dimension tables using keys.

This structure simplifies analytical queries and improves performance in BI tools.

Example relationship:
