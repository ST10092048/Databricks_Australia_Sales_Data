# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer – Cleansed & Conformed Data
# MAGIC *Medallion Architecture – Australian Sales & Opportunities Pipeline*
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Goal of the Silver Layer
# MAGIC
# MAGIC The Silver layer transforms raw Bronze data (customers, orders, opportunities) into a **trusted, standardized, enterprise-ready foundation**.
# MAGIC
# MAGIC **Objectives:**
# MAGIC - Ensure core entities are **accurate, consistent, and linkable** across tables.  
# MAGIC - Apply **just-enough cleansing, validation, and standardization** to support analysts, BI tools, and ML pipelines.  
# MAGIC - Preserve **full granularity** — no aggregations or heavy business logic (reserved for Gold).  
# MAGIC - Surface **data quality issues transparently** using flags or quarantine, not silent drops.  
# MAGIC - Facilitate **fast, repeatable reporting, self-service analytics, and feature engineering**.  
# MAGIC - Simplify Gold-layer development by handling **common fixes once** (null keys, invalid states, casing, orphan records).
# MAGIC ---
# MAGIC
# MAGIC ## Key Principles
# MAGIC
# MAGIC - **Critical failures** on identity (`customerid`, `orderid`, `opportunityid`) and money (`amount`, `orderamt`) → quarantine immediately.  
# MAGIC - **Warnings** for non-critical rules → allow data to flow while surfacing issues.  
# MAGIC - **Auditability** → all Silver records include DQ metadata and traceability to Bronze.  
# MAGIC - **Australian business context** → enforce state codes, realistic deal thresholds, and AU-specific business rules.  
# MAGIC - **Iterative improvement** → start strict on essentials, expand as DQ insights are gained.  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Core Business Rules & Quality Gates
# MAGIC
# MAGIC ### 1. Identity & Traceability (Critical – FAIL if violated)
# MAGIC - `customerid`, `orderid`, `opportunityid` **must exist and be unique** per load.  
# MAGIC - Deduplicate on **most recent `ingestion_timestamp`**.  
# MAGIC - Every `customerid` in Orders or Opportunities **must exist in Customers** → flag `is_orphan_customer = true` if unmatched, but retain record.  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2. Financial & Quantity Integrity (Critical – FAIL if violated)
# MAGIC - `orderamt` and `amount` **must be numeric ≥ 0**.  
# MAGIC - `quantity` **must be integer ≥ 1**.  
# MAGIC - Flag deals > AUD 750,000 as `is_large_deal_suspicious` for business review.  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3. Temporal Consistency (WARN)
# MAGIC - Dates must be valid (avoid placeholder values like `1900-01-01` or `9999-12-31`).  
# MAGIC - Orders should not be dated far in the future (> today + 90 days).  
# MAGIC - Soft warning if `orderdate` precedes linked opportunity date significantly.  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4. Australian Domain Rules (WARN + Standardize)
# MAGIC - `state` must match official AU codes (NSW, VIC, QLD, SA, WA, TAS, NT, ACT).  
# MAGIC   - Input normalized to **UPPER CASE**.  
# MAGIC   - Invalid values flagged `invalid_state = true` but retained.  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 5. Categorical & Formatting Cleanup (Standardize)
# MAGIC - `salesrep`: trim whitespace, apply **title case**, reject empty or placeholder values (`"N/A"`, `"TBD"`).  
# MAGIC - `phase` (opportunity): normalize stages  
# MAGIC   - Examples: `"2-closed won"` → `"Closed Won"`, `"3-closed lost"` → `"Closed Lost"`  
# MAGIC - `city`: trim whitespace (no heavy standardization yet due to variability).  
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 6. Data Quality Metadata (Added to Every Silver Table)
# MAGIC
# MAGIC | Column | Type | Purpose |
# MAGIC |--------|------|---------|
# MAGIC | `dq_status` | VARCHAR | 'PASS' / 'WARN' / 'FAIL' |
# MAGIC | `dq_issues` | ARRAY<STRING> | Example: ['negative_amount', 'invalid_state', 'missing_customerid'] |
# MAGIC | `is_orphan_customer` | BOOLEAN | Customer missing from Customers table |
# MAGIC | `is_large_deal_suspicious` | BOOLEAN | Deal exceeds threshold for review |
# MAGIC | `invalid_state` | BOOLEAN | State not valid per AU codes |
# MAGIC | `bronze_trace_id` or `source_hash` | VARCHAR | Full traceability back to raw Bronze data |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Dependencies
from pyspark.sql.functions import col, upper, trim,lower
from pyspark.sql.functions import to_date,year, month,dayofweek
from pyspark.sql.functions import regexp_replace, when, max as spark_max
# from utils.logger import get_logger

# COMMAND ----------

# DBTITLE 1,Ingestion Methods
def read_bronze_data(table_name):
    df = spark.table(f"salesdata.australia_sales_and_opportunities.{table_name}")
    return df

def latest_ingestion_ts(df):
    latest_ingestion_ts = df.agg(spark_max(col("ingestion_timestamp"))).collect()[0][0]
    df = df.filter(col("ingestion_timestamp") == latest_ingestion_ts)
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ###  **Standardization**
# MAGIC In this stage, raw data is cleaned, normalized, and transformed into a consistent schema by enforcing data types and standardizing formats, to prepare it for downstream analytical processing.

# COMMAND ----------

# DBTITLE 1,Standardization Methods

def standardize_data_strings(df, columns:[]):
    for c in columns:
        df = df.withColumn(c, lower(trim(col(c))))
    return df

def standardize_data_numeric(df,columns:[]):
    for c in columns:
        df = df.withColumn(c, col(c).cast("double"))
        df = df.withColumn(c, when(col(c).isNull(), 0).otherwise(col(c)))
    return df

def lower_column_names(df):
    df = df.toDF(*[c.lower() for c in df.columns])
    return df
    
    

# COMMAND ----------

# DBTITLE 1,Customer Standardization

# logger = get_logger("silver_pipeline")
string_columns = ['customerid', 'customername','city','state']

customer_df = read_bronze_data("bronze_customers")
customer_df = latest_ingestion_ts(customer_df)

customer_df = lower_column_names(customer_df)
customer_df = standardize_data_strings(customer_df, string_columns)

display(customer_df)
# logger.info("Customer Table standardization completed")

# COMMAND ----------

# DBTITLE 1,orders Standardization

string_columns = ['customerid', 'productid','salesrep']
numeric_columns = ["quantity", "orderid", "orderamt"]

orders_df = read_bronze_data("bronze_orders")
orders_df = latest_ingestion_ts(orders_df)
orders_df = lower_column_names(orders_df)

orders_df = standardize_data_strings(orders_df,string_columns)
orders_df = standardize_data_numeric(orders_df,numeric_columns)

display(orders_df)


# COMMAND ----------

# DBTITLE 1,Opportunities Standardization
string_columns = ['opportunityid', 'customerid','state','salesrep','phase']
numeric_columns = ["amount"]

opportunities_df = read_bronze_data("bronze_opportunities")
display(opportunities_df.columns)
opportunities_df = latest_ingestion_ts(opportunities_df)
opportunities_df = lower_column_names(opportunities_df)

opportunities_df = standardize_data_strings(opportunities_df,string_columns)
opportunities_df = standardize_data_numeric(opportunities_df,numeric_columns)

display(opportunities_df)
# logger.info("Opportunities Table standardization completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Validation**
# MAGIC In the Silver layer, data is validated against schema definitions, integrity constraints, and business rules. Records failing validation are flagged, quarantined, or corrected. This ensures that downstream analytical tables in Silver and Gold layers only contain accurate, complete, and consistent data suitable for reporting and advanced analytics.

# COMMAND ----------

# DBTITLE 1,Data Validation Methods
def check_nulls(df, columns):
    df.filter(sum(col(c).isNull().cast("int") for c in columns) > 0 )
    return df

def check_duplicates(df, columns:[]):
    df.groupBy(columns).count().filter(col("count") > 1)

    return df


# COMMAND ----------

# DBTITLE 1,Customer Null Check

id_columns = ["customerid","customername"]
customer_df_null_check = check_nulls(customer_df, id_columns)
display(customer_df_null_check)

# COMMAND ----------

# DBTITLE 1,Orders Null Check


id_columns = ["orderid","customerid","productid","orderdate","orderamt","quantity"]
orders_df_null_check = check_nulls(orders_df, id_columns)

display(orders_df_null_check)

# COMMAND ----------

# DBTITLE 1,Opportunities Null Check
id_columns = ["opportunityid","customerid","salesrep"]
opportunities_df_null_check = check_nulls(opportunities_df, id_columns) 
display(opportunities_df_null_check)

# COMMAND ----------

# DBTITLE 1,Customer Duplicates
dup_cols = ["customerid"]
customer_df_duplicates = check_duplicates(customer_df, dup_cols)
display(customer_df_duplicates)


# COMMAND ----------

# DBTITLE 1,opportunities Duplicates
opportunities_df_duplicates =(
    opportunities_df.groupBy("opportunityid")
      .count()
      .filter(col("count") > 1  
      )
)
display(opportunities_df_duplicates)

# COMMAND ----------

# DBTITLE 1,Orders Duplicates
orders_df_duplicates = (
    orders_df.groupBy("orderid")
      .count()
      .filter(col("count") > 1)
)

display(orders_df_duplicates)


# COMMAND ----------

# MAGIC %md
# MAGIC Data 

# COMMAND ----------

orders_df = orders_df.withColumn("orderdate", to_date(col("orderdate"),'yyyy-MM-dd'))
orders_df = orders_df.withColumn("year",year("orderdate"))\
    .withColumn("month",month("orderdate"))\
    .withColumn("dayofweek",dayofweek("orderdate"))
orders_df.show()
