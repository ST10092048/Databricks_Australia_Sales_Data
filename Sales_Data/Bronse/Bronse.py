# Databricks notebook source
# MAGIC %md
# MAGIC # **Simulated Australia Sales and Opportunities Data**
# MAGIC
# MAGIC ## About the Data
# MAGIC
# MAGIC This dataset originates from Databricks as a simulated dataset designed to model real-world sales and opportunity workflows.
# MAGIC The data is generated as a one-time load. However, it is not append-only. It contains mutable entities such as the customer table, meaning historical records may change over time. Because of this, we must design the pipeline to properly track and manage data mutations.
# MAGIC
# MAGIC ## Architecture Approach
# MAGIC
# MAGIC We will implement the Medallion Architecture:
# MAGIC
# MAGIC - **Bronze Layer** – Stores raw, source-aligned data with minimal transformation (which would only be paritioning the data for faster reads and queries). This layer serves as the replayable source of the original data.
# MAGIC - **Silver Layer** – Applies transformations, standardization, data quality rules, and handles schema enforcement.
# MAGIC - **Gold Layer** – Provides curated, analytics-ready datasets optimized for reporting and business intelligence use cases.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Data Strategy & Failure Handling
# MAGIC
# MAGIC To ensure reliability and  stability, the pipeline is designed with:
# MAGIC
# MAGIC - **Atomic writes using Delta tables** to guarantee ACID-compliant transactions and prevent partial data commits in the event of job failure.
# MAGIC - **Idempotent write patterns** ( MERGE or controlled deduplication) to prevent duplicate records during retries or re-runs.
# MAGIC - Schema evolution handling to safely absorb structural changes in upstream datasets.
# MAGIC
# MAGIC This design ensures that the pipeline remains resilient, re-runnable, and safe under failure scenarios while maintaining historical consistency.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------


customer_df = spark.table("databricks_simulated_australia_sales_and_opportunities_data.v01.customers")
customer_df = customer_df.withColumn("ingestion_timestamp", current_timestamp())
customer_df.write.format("delta").mode("append").saveAsTable("ausalesdata.bronse.customer_information")

# COMMAND ----------

orders_df = spark.table("databricks_simulated_australia_sales_and_opportunities_data.v01.orders")
orders_df = orders_df.withColumn("ingestion_timestamp", current_timestamp())
orders_df.write.format("delta").mode("append").saveAsTable("ausalesdata.bronse.orders")

# COMMAND ----------

opportunities_df = spark.table("databricks_simulated_australia_sales_and_opportunities_data.v01.opportunities")
opportunities_df = opportunities_df.withColumn("ingestion_timestamp", current_timestamp())
opportunities_df.write.format("delta").mode("append").saveAsTable("ausalesdata.bronse.opportunities")
