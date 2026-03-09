# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "4"
# ///
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
from pyspark.sql.functions import col,row_number
from pyspark.sql.functions import to_date, year, quarter, month,dayofmonth, dayofweek, weekofyear,date_format, when
from pyspark.sql.window import Window


# COMMAND ----------

# DBTITLE 1,Ingestion Methods
def read_sliver_data(table_name):
    df = spark.table(f"salesdata.silver.{table_name}")
    return df


# COMMAND ----------

def filter_data(df,columns):
    for column in columns:
        df = df.filter(col(column) == False)
    return df

def select_with_alias(df, columns: dict):
    return df.select([col(k).alias(v) for k, v in columns.items()])

def dim_table(df,filter_columns,selected_columns):
    df = filter_data(df,filter_columns)
    df = select_with_alias(df,selected_columns)
    df = df.dropDuplicates()
    return df

def fact_table(df,filter_columns,selected_columns):
    df = filter_data(df,filter_columns)
    df = select_with_alias(df,selected_columns)

    return df
    
def enrich_date(df, column):

    df = df.withColumn(column, to_date(col(column), "yyyy-MM-dd"))

    df = (
        df
        .withColumn("year", year(col(column)))
        .withColumn("quarter", quarter(col(column)))
        .withColumn("month", month(col(column)))
        .withColumn("month_name", date_format(col(column), "MMMM"))
        .withColumn("week_of_year", weekofyear(col(column)))
        .withColumn("day_of_month", dayofmonth(col(column)))
        .withColumn("day_of_week", dayofweek(col(column)))
        .withColumn("day_name", date_format(col(column), "EEEE"))
        .withColumn(
            "is_weekend",
            when(dayofweek(col(column)).isin(1,7), True).otherwise(False)
        )
    )

    return df

# COMMAND ----------

silver_customers_df = read_sliver_data("customers")
silver_orders_df = read_sliver_data("orders")
display(silver_orders_df)
silver_opportunities_df = read_sliver_data("opportunities")
display(silver_opportunities_df)


# COMMAND ----------

def create_fact_key(name,key_name, final_column, *df_columns):

    df_union = df_columns[0][0].selectExpr(f"{df_columns[0][1]} as {final_column}")

    for df, col in df_columns[1:]:
        df_union = df_union.union(df.selectExpr(f"{col} as {final_column}"))

    df_union = df_union.distinct()

    df_key = add_key(df_union, key_name, final_column,name)

    return df_key


def add_key(df, key_name, select_column,name):

    if name == 'date':
        df = df.withColumn(key_name,date_format(col(select_column), "yyyyMMdd").cast("int"))
        df = enrich_date(df,select_column)
    
    else: 
        window = Window.orderBy(select_column)

        df = df.withColumn(
            key_name,
            row_number().over(window)
        ).select(key_name, select_column)

    return df

def join_key_fact_table(df_main,df_second,key_name,how="left"):
    df = df_main.join(
        df_second,
        on=key_name,
        how=how
    )
    return df

# COMMAND ----------

vaild_customers_id = ["customerid_is_null","is_duplicate","customername_is_null"]
select_customers = {"customerid":"customer_id","customername":"customer_name","city":"city","state":"state"}
dim_customer = dim_table(silver_customers_df,vaild_customers_id,select_customers)

vaild_sales_rep_id = ["invalid_salesrep"]
select_sales_rep = {"salesrep":"sales_rep"}
dim_sales_rep = create_fact_key(None,"sales_rep_key","salesrep",
                                (silver_orders_df,"salesrep"),
                                (silver_opportunities_df,"salesrep"))
display(dim_sales_rep)

dim_product = dim_table(silver_orders_df,["productid_is_null"],{"productid":"product_id"})
display(dim_product)

date_key = create_fact_key('date','date_key','date',
                           (silver_orders_df,"orderdate"),
                           (silver_opportunities_df,"date"))
display(date_key)

phase_dim = dim_table(silver_opportunities_df,["opportunityid_is_null"],{"phase":"phase","status_clean":"status"})
display(phase_dim)


# COMMAND ----------

valid_orders_id = ['orderid_is_null','is_orphan']
select_order_columns = {
    "customerid":"customer_id",
    "orderid":"order_id",
    "productid":"product_id",
    "date_key":"date_key",
    "quantity":"quantity",
    "orderamt":"order_amt",
    "sales_rep_key":"sales_rep_key"}
orders_fact_key = join_key_fact_table(silver_orders_df,dim_sales_rep,"salesrep")
# display(orders_fact_key)
orders_fact = fact_table(orders_fact_key,valid_orders_id,select_order_columns)
display(orders_fact)
vaild_opportunityid_is_null = ["opportunityid_is_null","is_orphan"]
select_opportunity_columns = {
    "customerid":"customer_id",
    "opportunityid":"opportunity_id",
    "state":"state",
    "date":"date",
    "amount":"amount",
    "phase":"phase",
    "status_clean":"status",
    "sales_rep_key":"sales_rep_key",
    "date_key":"date_key"}
opportunities_fact_key = join_key_fact_table(silver_opportunities_df,dim_sales_rep,"salesrep")
# display(opportunities_fact_key)
opportunities_fact = fact_table(opportunities_fact_key,vaild_opportunityid_is_null,select_opportunity_columns)
display(opportunities_fact)


# COMMAND ----------


def create_table_gold(df,format,mode,table_name):
    df.write.format(format).mode(mode).option("mergeSchema", "true")\
    .saveAsTable(f"salesdata.gold.{table_name}")

# COMMAND ----------

create_table_gold(dim_customer,"delta","overwrite","dim_customer")
create_table_gold(dim_sales_rep,"delta","overwrite","dim_sales_rep")
create_table_gold(dim_product,"delta","overwrite","dim_product")
create_table_gold(date_key,"delta","overwrite","dim_date")
create_table_gold(phase_dim,"delta","overwrite","dim_phase")
create_table_gold(orders_fact,"delta","overwrite","orders_fact")
create_table_gold(opportunities_fact,"delta","overwrite","opportunities_fact")
