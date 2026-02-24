# Databricks notebook source
from pyspark.sql.functions import col, upper, trim,lower

customer_df = spark.table("databricks_simulated_australia_sales_and_opportunities_data.v01.customers")
customer_df = customer_df.toDF(*[c.lower() for c in customer_df.columns])
display(customer_df.columns)

string_columns = ['customerid', 'customername','city','state']
for c in string_columns:
    customer_df = customer_df.withColumn(c, lower(trim(col(c))))

display(customer_df)
