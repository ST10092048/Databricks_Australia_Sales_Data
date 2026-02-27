# Databricks notebook source
# MAGIC %md
# MAGIC # **Business Rules – Simulated Australia Sales and Opportunities Data**
# MAGIC
# MAGIC Context: These rules are applied in the Silver layer of a medallion architecture pipeline to ensure that ingested raw data from the Bronze layer is accurate, consistent, and analytics-ready.
# MAGIC
# MAGIC ## 1. Required Fields
# MAGIC
# MAGIC CustomerID, OrderID, OpportunityID, and SalesAmount must be populated; null or missing values are flagged for review.
# MAGIC
# MAGIC Ensures every record is traceable and can be linked to the corresponding customer, order, or opportunity.
# MAGIC
# MAGIC ## 2. Data Type and Format Constraints
# MAGIC
# MAGIC SalesAmount and Quantity must be numeric.
# MAGIC
# MAGIC OrderDate and OpportunityDate must be valid date formats.
# MAGIC
# MAGIC Region and State must conform to valid Australian state codes (e.g., NSW, VIC, QLD).
# MAGIC
# MAGIC ## 3. Logical Consistency Rules
# MAGIC
# MAGIC OpportunityCloseDate must be greater than or equal to OpportunityOpenDate.
# MAGIC
# MAGIC OrderDate must not precede OpportunityCloseDate.
# MAGIC
# MAGIC SalesAmount must be greater than or equal to zero.
# MAGIC
# MAGIC ## 4. Uniqueness Constraints
# MAGIC
# MAGIC OrderID and OpportunityID must be unique to prevent duplicate transactions or opportunities.
# MAGIC
# MAGIC ## 5. Referential Integrity
# MAGIC
# MAGIC Every CustomerID must correspond to an existing customer record.
# MAGIC
# MAGIC Every SalesRepID must exist in the sales representative master table.
# MAGIC
# MAGIC ## 6. Categorical Validation
# MAGIC
# MAGIC Fields such as SalesStage, OpportunityStatus, and ProductCategory must only contain allowed values.
# MAGIC
# MAGIC Example: SalesStage ∈ {Prospect, Qualification, Proposal, Closed Won, Closed Lost}
# MAGIC
# MAGIC ## 7. Threshold and Business Constraints
# MAGIC
# MAGIC DiscountPercentage must be between 0% and 100%.
# MAGIC
# MAGIC Quantity must be greater than or equal to 1.

# COMMAND ----------

# DBTITLE 1,Dependencies
from pyspark.sql.functions import col, upper, trim,lower
from pyspark.sql.functions import to_date,year, month,dayofweek
# from utils.logger import get_logger

# COMMAND ----------

# MAGIC %md
# MAGIC ###  **Standardization**
# MAGIC In this stage, raw data is cleaned, normalized, and transformed into a consistent schema by enforcing data types and standardizing formats, to prepare it for downstream analytical processing.

# COMMAND ----------

# DBTITLE 1,Customer Standardization

# logger = get_logger("silver_pipeline")
customer_df = spark.table("databricks_simulated_australia_sales_and_opportunities_data.v01.customers")
customer_df = customer_df.toDF(*[c.lower() for c in customer_df.columns])
display(customer_df.columns)

string_columns = ['customerid', 'customername','city','state']
for c in string_columns:
    customer_df = customer_df.withColumn(c, lower(trim(col(c))))

display(customer_df)
# logger.info("Customer Table standardization completed")

# COMMAND ----------

# DBTITLE 1,orders Standardization
orders_df = spark.table("databricks_simulated_australia_sales_and_opportunities_data.v01.orders")

orders_df = orders_df.toDF(*[c.lower() for c in orders_df.columns])
display(orders_df.columns)

string_columns = ['customerid', 'productid','salesrep']
for c in string_columns:
    orders_df = orders_df.withColumn(c, lower(trim(col(c))))

display(orders_df)
# logger.info("Orders Table standardization completed")

# COMMAND ----------

# DBTITLE 1,Opportunities Standardization
opportunities_df = spark.table("databricks_simulated_australia_sales_and_opportunities_data.v01.opportunities")

opportunities_df = opportunities_df.toDF(*[c.lower() for c in opportunities_df.columns])
display(opportunities_df.columns)

string_columns = ['opportunityid', 'customerid','state','salesrep','phase']
for c in string_columns:
    opportunities_df = opportunities_df.withColumn(c, lower(trim(col(c))))

display(opportunities_df)
# logger.info("Opportunities Table standardization completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Validation**
# MAGIC In the Silver layer, data is validated against schema definitions, integrity constraints, and business rules. Records failing validation are flagged, quarantined, or corrected. This ensures that downstream analytical tables in Silver and Gold layers only contain accurate, complete, and consistent data suitable for reporting and advanced analytics.

# COMMAND ----------

# DBTITLE 1,Customer Null Check

id_columns = ["customerid","customername"]
customer_df_null_check = customer_df.filter(
    sum(col(c).isNull().cast("int") for c in id_columns) > 0
)

display(customer_df_null_check)

# COMMAND ----------

# DBTITLE 1,Orders Null Check


id_columns = ["orderid","customerid","productid","orderdate"]
orders_df_null_check = orders_df.filter(
    sum(col(c).isNull().cast("int") for c in id_columns) > 0
)

display(orders_df_null_check)

# COMMAND ----------

# DBTITLE 1,Opportunities Null Check
id_columns = ["opportunityid","customerid","salesrep"]
opportunities_df_null_check = opportunities_df.filter(
    sum(col(c).isNull().cast("int") for c in id_columns) > 0
)
display(opportunities_df_null_check)

# COMMAND ----------

# DBTITLE 1,Customer Duplicates
customer_df_duplicates = (
    customer_df.groupBy("customerid")
      .count()
      .filter(col("count") > 1)
)

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
