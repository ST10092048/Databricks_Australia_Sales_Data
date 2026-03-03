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

# MAGIC %md
# MAGIC ## **Dependencies**
# MAGIC I run the dependencies step separately and first for pipeline debugging. This ensures that if a dependency fails, I can detect it early without affecting the rest of the pipeline. Sometimes the pipeline logic itself is correct, but a dependency may fail due to external issues. Running dependencies independently prevents other tables from being processed or written when one table has a problem, helping to avoid partial or inconsistent data transactions.

# COMMAND ----------

# DBTITLE 1,Dependencies
from pyspark.sql.functions import col, upper, trim,lower
from pyspark.sql.functions import to_date,year, month,dayofweek
from pyspark.sql.functions import regexp_replace, when, max as spark_max,row_number, desc
from pyspark.sql.window import Window

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

def deduplication(df, column, debug=False):
    window_spec = Window.partitionBy(column).orderBy(desc("ingestion_timestamp"))

    # Add row_number
    df_with_rn = df.withColumn("rn", row_number().over(window_spec))

    if debug:
        print(f"=== Ranking before deduplication by '{column}' ===")
        df_with_rn.orderBy(column, "rn").show(truncate=False)

    # Keep only the first row per key
    df_dedup = df_with_rn.filter(col("rn") == 1).drop("rn")
    return df_dedup

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
customer_df = deduplication(customer_df, "customerid")

customer_df = lower_column_names(customer_df)
customer_df = standardize_data_strings(customer_df, string_columns)
customer_df = customer_df.withColumn("state",upper(trim(col("state"))))

display(customer_df)
# logger.info("Customer Table standardization completed")

# COMMAND ----------

# DBTITLE 1,orders Standardization

string_columns = ['customerid', 'productid','salesrep']
numeric_columns = ["quantity", "orderid", "orderamt"]

orders_df = read_bronze_data("bronze_orders")
orders_df = latest_ingestion_ts(orders_df)
orders_df = deduplication(orders_df, "orderid")
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
opportunities_df = deduplication(opportunities_df,"opportunityid")
opportunities_df = lower_column_names(opportunities_df)

opportunities_df = standardize_data_strings(opportunities_df,string_columns)
opportunities_df = standardize_data_numeric(opportunities_df,numeric_columns)
opportunities_df = opportunities_df.withColumn("status_clean",upper(
        trim(
            regexp_replace("phase", r"^\d+-\s*", "")
        )
    )
)

display(opportunities_df)
# logger.info("Opportunities Table standardization completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Validation**
# MAGIC In the Silver layer, data is validated against schema definitions, integrity constraints, and business rules. Records failing validation are flagged, quarantined, or corrected. This ensures that downstream analytical tables in Silver and Gold layers only contain accurate, complete, and consistent data suitable for reporting and advanced analytics.

# COMMAND ----------

# DBTITLE 1,Data Validation Methods
def check_nulls(df, columns):
    nulls =df.filter(sum(col(c).isNull().cast("int") for c in columns) > 0 )

    if nulls.count() > 0:
        print(f"❌ Nulls found")
        nulls.show()
        raise Exception("Nulls found")
    else:
        print(f"✅ No nulls found")

    return df
def bad_sales_rep(df):
    bad_salesrep = df.filter(
        col("salesrep").isNull() |
        (trim(col("salesrep")) == "") |
        (trim(col("salesrep")).isin("N/A", "TBD"))
)

    if bad_salesrep.count() > 0:
        raise Exception("❌ Invalid salesrep values found")
    else:
        print(f"✅ No invalid salesrep values found")

def big_orders(df,column,amount):
    big_orders = df.withColumn("is_large_deal_suspicious", when(col(column) > amount, True).otherwise(False))
    return big_orders
def check_values(df, columns):
    for column in columns:
        print(f"Checking column: {column}")
        
        invalid_rows = df.filter(col(column) < 1)
        
        if invalid_rows.count() > 0:
            print(f"❌ Column {column} has values < 1")
            invalid_rows.show()
            raise Exception("Invalid values found")
        else:
            print(f"✅ Column {column} is valid")

    return invalid_rows

def check_duplicates(df, columns:[]):
    dups = df.groupBy(columns).count().filter(col("count") > 1)
    if dups.count() > 0:
        print(f"❌ Duplicates found")
        dups.show()
        raise Exception("Duplicates found")
    else:
        print(f"✅ No duplicates found")

    return df

def check_orphan_rows(df_main,df_check,key):
    orphan_rows = df_main.join(df_check,on=key,how="left_anti")
    if orphan_rows.count() > 0:
        print(f"❌ Orphan rows found")
        orphan_rows.show()
        raise Exception("Orphan rows found")
    else:
        print(f"✅ No orphan rows found")
    return orphan_rows


# COMMAND ----------

# DBTITLE 1,Customer Validation

id_columns = ["customerid","customername"]
customer_df_null_check = check_nulls(customer_df, id_columns)
display(customer_df_null_check)

dup_cols = ["customerid"]
customer_df_duplicates = check_duplicates(customer_df, dup_cols)
display(customer_df_duplicates)


# COMMAND ----------

# DBTITLE 1,Orders Validation


id_columns = ["orderid","customerid","productid","orderdate","orderamt","quantity","salesrep"]
value_checks = ["quantity","orderamt"]
orders_df_null_check = check_nulls(orders_df, id_columns)

display(orders_df_null_check)
dup_cols = ["orderid"]
orders_df_duplicates = check_duplicates(orders_df, dup_cols)
display(orders_df_duplicates)
invalid_rows = check_values(orders_df, value_checks)
display(invalid_rows)
id_columns = ["customerid","salesrep"]
orphan_rows = check_orphan_rows(customer_df,orders_df,'customerid')
big_orders = big_orders(orders_df,'orderamt',750000)
bad_sales_rep(orders_df)
display(big_orders)


# COMMAND ----------

# DBTITLE 1,Opportunities Validation
id_columns = ["opportunityid","customerid","salesrep"]
opportunities_df_null_check = check_nulls(opportunities_df, id_columns) 
display(opportunities_df_null_check)

dup_cols = ["opportunityid"]
opportunities_df_duplicates = check_duplicates(opportunities_df, dup_cols)
display(opportunities_df_duplicates)
invalid_rows = check_values(opportunities_df, ["amount"])
orphan_rows = check_orphan_rows(opportunities_df,orders_df,'customerid')
bad_sales_rep(opportunities_df)


# COMMAND ----------

# MAGIC %md
# MAGIC Data Enrichment

# COMMAND ----------

# DBTITLE 1,Enrichment Methods
def enrich_date(df,column):
    df = df.withColumn(column, to_date(col(column), 'yyyy-MM-dd'))
    df = df.withColumn("year",year(column))\
        .withColumn("month",month(column))\
        .withColumn("dayofweek",dayofweek(column))
    return df

# COMMAND ----------

# DBTITLE 1,Order enrichment
orders_df = enrich_date(orders_df,"orderdate")

# COMMAND ----------

# DBTITLE 1,opportunities enrichment
opportunities_df = enrich_date(opportunities_df,"date")
display(opportunities_df)
