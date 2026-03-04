# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer – Business-Level Analytics
# MAGIC ### Medallion Architecture – Australian Sales & Opportunities Pipeline
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Goal of the Gold Layer
# MAGIC
# MAGIC The **Gold layer** transforms validated **Silver datasets** (customers, orders, opportunities) into **business-ready analytical models** optimized for reporting, dashboards, and decision-making.
# MAGIC
# MAGIC Unlike the **Silver layer**, which focuses on **data quality and standardization**, the Gold layer focuses on:
# MAGIC
# MAGIC - Business metrics
# MAGIC - Aggregations
# MAGIC - Dimensional modeling
# MAGIC
# MAGIC Gold tables represent **trusted business views of the data**, designed for consumption by:
# MAGIC
# MAGIC - Business Intelligence tools (Power BI, Tableau)
# MAGIC - Data analysts
# MAGIC - Sales leadership
# MAGIC - Finance teams
# MAGIC - Machine learning pipelines
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Objectives
# MAGIC
# MAGIC - Ensure business metrics are **accurate, consistent, and easily consumable** across analytics platforms.
# MAGIC - Aggregate transactional data into **high-level business insights** for reporting and dashboards.
# MAGIC - Organize datasets into **dimensional models (facts and dimensions)** to simplify analytical queries.
# MAGIC - Provide **single-source-of-truth KPIs** such as revenue, deal size, and win rates.
# MAGIC - Optimize tables for **analytical performance and BI workloads**.
# MAGIC - Enable **fast reporting, strategic decision making, and advanced analytics**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Key Principles
# MAGIC
# MAGIC - Gold datasets are **business-focused**, representing metrics and insights rather than operational records.
# MAGIC - All transformations assume **data quality has already been validated in the Silver layer**.
# MAGIC - **Aggregations and KPIs are centralized** in Gold to avoid conflicting metric definitions.
# MAGIC - Schemas follow **dimensional modeling practices** (fact and dimension tables).
# MAGIC - Gold tables contain **only analytical fields**, avoiding unnecessary operational metadata.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # Core Business Rules & Analytical Models
# MAGIC
# MAGIC ## 1. Dimensional Modeling (Analytical Structure)
# MAGIC
# MAGIC Gold datasets follow a **star schema design**, separating measurable events from descriptive attributes.
# MAGIC
# MAGIC - **Fact tables** store business events and measures.
# MAGIC - **Dimension tables** provide context for analysis.
# MAGIC
# MAGIC ### Typical Structure
# MAGIC
# MAGIC **Fact Tables**
# MAGIC - `fact_sales`
# MAGIC - `fact_opportunity_pipeline`
# MAGIC
# MAGIC **Dimension Tables**
# MAGIC - `dim_customer`
# MAGIC - `dim_salesrep`
# MAGIC - `dim_product`
# MAGIC - `dim_date`
# MAGIC
# MAGIC This structure enables **efficient reporting and simplified analytical queries**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 2. Revenue & Sales Metrics (Business KPIs)
# MAGIC
# MAGIC Revenue calculations are derived from **validated Silver order data**.
# MAGIC
# MAGIC ### Key Metrics
# MAGIC
# MAGIC **Total Revenue**
# MAGIC - Sum of `orderamt` across completed orders.
# MAGIC
# MAGIC **Average Deal Size**
# MAGIC - Average order value across all sales.
# MAGIC
# MAGIC **Large Deal Monitoring**
# MAGIC - Deals flagged in Silver (`is_large_deal_suspicious`) are monitored for executive reporting.
# MAGIC
# MAGIC ### Revenue Breakdown
# MAGIC
# MAGIC Revenue aggregated by:
# MAGIC
# MAGIC - state
# MAGIC - customer
# MAGIC - product
# MAGIC - sales representative
# MAGIC - time period
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 3. Sales Pipeline Analytics
# MAGIC
# MAGIC Opportunity data is aggregated to measure **sales pipeline performance**.
# MAGIC
# MAGIC ### Key Metrics
# MAGIC
# MAGIC **Pipeline Value**
# MAGIC - Total value of open opportunities.
# MAGIC
# MAGIC **Win Rate**
# MAGIC - Ratio of opportunities marked **Closed Won** compared to total closed opportunities.
# MAGIC
# MAGIC **Loss Rate**
# MAGIC - Ratio of opportunities marked **Closed Lost**.
# MAGIC
# MAGIC **Sales Stage Distribution**
# MAGIC - Count and value of opportunities by stage.
# MAGIC
# MAGIC These metrics provide visibility into **sales effectiveness and pipeline health**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 4. Customer Analytics
# MAGIC
# MAGIC Customer-level aggregations help identify **high-value accounts and regional performance**.
# MAGIC
# MAGIC Examples include:
# MAGIC
# MAGIC - **Revenue per Customer** – Total sales attributed to each customer
# MAGIC - **Top Customers** – Ranking customers by total revenue
# MAGIC - **Regional Customer Distribution** – Customer activity analyzed by city and state
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 5. Time-Based Analytics
# MAGIC
# MAGIC Gold tables leverage **date attributes created in Silver** to enable temporal analysis.
# MAGIC
# MAGIC Typical aggregations include:
# MAGIC
# MAGIC - Revenue by Year
# MAGIC - Revenue by Month
# MAGIC - Revenue by Day of Week
# MAGIC - Sales Trends Over Time
# MAGIC
# MAGIC These allow dashboards to track **growth, seasonality, and sales cycles**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # Analytical Tables Generated in Gold
# MAGIC
# MAGIC ## Fact Tables
# MAGIC
# MAGIC ### `fact_sales`
# MAGIC
# MAGIC Represents **completed sales transactions**.
# MAGIC
# MAGIC **Measures**
# MAGIC - order amount
# MAGIC - quantity sold
# MAGIC - revenue
# MAGIC
# MAGIC **Dimensions**
# MAGIC - customer
# MAGIC - product
# MAGIC - sales representative
# MAGIC - date
# MAGIC - state
# MAGIC
# MAGIC **Example analyses**
# MAGIC
# MAGIC - revenue by region
# MAGIC - product performance
# MAGIC - sales rep performance
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### `fact_opportunity_pipeline`
# MAGIC
# MAGIC Represents **sales pipeline activity**.
# MAGIC
# MAGIC **Measures**
# MAGIC - opportunity amount
# MAGIC - win/loss status
# MAGIC - deal stage
# MAGIC
# MAGIC **Dimensions**
# MAGIC - customer
# MAGIC - sales representative
# MAGIC - date
# MAGIC - region
# MAGIC
# MAGIC **Example analyses**
# MAGIC
# MAGIC - pipeline value
# MAGIC - win rate
# MAGIC - opportunity conversion
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Dimension Tables
# MAGIC
# MAGIC ### `dim_customer`
# MAGIC
# MAGIC Customer descriptive attributes.
# MAGIC
# MAGIC **Columns**
# MAGIC
# MAGIC - customerid
# MAGIC - customername
# MAGIC - city
# MAGIC - state
# MAGIC
# MAGIC Supports:
# MAGIC
# MAGIC - customer segmentation
# MAGIC - regional sales analysis
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### `dim_salesrep`
# MAGIC
# MAGIC Sales representative details.
# MAGIC
# MAGIC **Columns**
# MAGIC
# MAGIC - salesrep name
# MAGIC - region
# MAGIC - performance metrics
# MAGIC
# MAGIC Supports:
# MAGIC
# MAGIC - sales team performance analysis
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### `dim_date`
# MAGIC
# MAGIC Calendar attributes used for time-based analysis.
# MAGIC
# MAGIC **Columns**
# MAGIC
# MAGIC - date
# MAGIC - year
# MAGIC - month
# MAGIC - dayofweek
# MAGIC - quarter
# MAGIC
# MAGIC Supports:
# MAGIC
# MAGIC - trend analysis
# MAGIC - seasonality insights
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### `dim_product`
# MAGIC
# MAGIC Product details used for sales analysis.
# MAGIC
# MAGIC **Columns**
# MAGIC
# MAGIC - productid
# MAGIC - product category
# MAGIC - product name
# MAGIC
# MAGIC Supports:
# MAGIC
# MAGIC - product performance reporting
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC # Data Governance & Metric Consistency
# MAGIC
# MAGIC All business KPIs are **defined once in the Gold layer** to ensure consistent reporting across analytics platforms.
# MAGIC
# MAGIC Examples of governed metrics include:
# MAGIC
# MAGIC - revenue calculations
# MAGIC - win/loss definitions
# MAGIC - average deal size
# MAGIC - large deal thresholds
# MAGIC
# MAGIC This ensures that **dashboards and reports across the organization reference the same metric definitions**.

# COMMAND ----------

# DBTITLE 1,Dependencies
from pyspark.sql.functions import col

# COMMAND ----------

# DBTITLE 1,Ingestion Methods
def read_sliver_data(table_name):
    df = spark.table(f"salesdata.australia_sales_and_opportunities.{table_name}")
    return df


# COMMAND ----------

silver_customers_df = read_sliver_data("silver_customers")

dim_customer = (
    silver_customers_df
    .filter(
        (col("customerid_is_null") == False) &
        (col("is_duplicate") == False) &
        (col("customername_is_null") == False)
    )
    .select(
        col("customerid").alias("customer_id"),
        col("customername").alias("customer_name"),
        "city",
        "state"
    )
)
display(dim_customer)


silver_orders_df = read_sliver_data("silver_orders")
display(silver_orders_df)
sliver_opportunities_df = read_sliver_data("silver_opportunities")
display(sliver_opportunities_df)
