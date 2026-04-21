-- Databricks notebook source
-- MAGIC %python
-- MAGIC if spark.catalog.tableExists("workspace.bronze.bronze_table"):
-- MAGIC     last_load_date = spark.sql("SELECT max(order_date) FROM workspace.bronze.bronze_table").collect()[0][0]
-- MAGIC else:
-- MAGIC     last_load_date = '1000-01-01'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC last_load_date

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f"""SELECT * FROM workspace.default.source_data
-- MAGIC WHERE order_date > '{last_load_date}'""").createOrReplaceTempView("bronze_source")

-- COMMAND ----------

SELECT * FROM bronze_source

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.bronze.bronze_table
AS
SELECT * FROM bronze_source