# Databricks notebook source
# MAGIC %md
# MAGIC Standardization
# MAGIC This is where data is normalized

# COMMAND ----------

# DBTITLE 1,Customer Standardization
from pyspark.sql.functions import col, upper, trim,lower

customer_df = spark.table("databricks_simulated_australia_sales_and_opportunities_data.v01.customers")
customer_df = customer_df.toDF(*[c.lower() for c in customer_df.columns])
display(customer_df.columns)

string_columns = ['customerid', 'customername','city','state']
for c in string_columns:
    customer_df = customer_df.withColumn(c, lower(trim(col(c))))

display(customer_df)

# COMMAND ----------

# DBTITLE 1,orders Standardization
orders_df = spark.table("databricks_simulated_australia_sales_and_opportunities_data.v01.orders")

orders_df = orders_df.toDF(*[c.lower() for c in orders_df.columns])
display(orders_df.columns)

string_columns = ['customerid', 'productid','salesrep']
for c in string_columns:
    orders_df = orders_df.withColumn(c, lower(trim(col(c))))

display(orders_df)

# COMMAND ----------

# DBTITLE 1,Opportunities Standardization
opportunities_df = spark.table("databricks_simulated_australia_sales_and_opportunities_data.v01.opportunities")

opportunities_df = opportunities_df.toDF(*[c.lower() for c in opportunities_df.columns])
display(opportunities_df.columns)

string_columns = ['opportunityid', 'customerid','state','salesrep','phase']
for c in string_columns:
    opportunities_df = opportunities_df.withColumn(c, lower(trim(col(c))))

display(opportunities_df)

# COMMAND ----------


