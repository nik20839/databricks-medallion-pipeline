-- Databricks notebook source
-- MAGIC %python
-- MAGIC spark.sql("""
-- MAGIC SELECT * ,
-- MAGIC       upper(customer_name) as Customer_Name_Upper,
-- MAGIC       date(current_timestamp()) as processDate
-- MAGIC FROM workspace.bronze.bronze_table""").createOrReplaceTempView("silver_source")

-- COMMAND ----------

SELECT * FROM silver_source

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS workspace.silver.silver_table
                AS 
                SELECT * FROM silver_source

-- COMMAND ----------

MERGE INTO workspace.silver.silver_table
USING silver_source
ON workspace.silver.silver_table.order_id = silver_source.order_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

SELECT * FROM workspace.silver.silver_table

-- COMMAND ----------

