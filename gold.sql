-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##Dim Customers

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gold.DimCustomers
AS
WITH rem_dup AS
(
SELECT 
  DISTINCT(customer_id),
  customer_email,
  customer_name,
  Customer_Name_Upper
FROM workspace.silver.silver_table
)
SELECT * ,
      row_number() OVER (ORDER BY customer_id) as DimCustomerKey
FROM rem_dup

-- COMMAND ----------

select * from workspace.gold.DimCustomers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Dim Products

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gold.DimProducts
AS
WITH rem_dup AS
(
SELECT 
  DISTINCT(product_id),
  product_name,
  product_category
FROM 
  workspace.silver.silver_table
)
SELECT * ,
      row_number() OVER (ORDER BY product_id) as DimProductKey
FROM rem_dup

-- COMMAND ----------

select * from workspace.gold.DimProducts

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gold.DimPayments
WITH rem_dup AS 
(
SELECT 
  DISTINCT(payment_type)
FROM workspace.silver.silver_table
) 
SELECT *, row_number() OVER (ORDER BY payment_type) as DimPaymentKey FROM rem_dup

-- COMMAND ----------

select* from workspace.gold.DimPayments

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gold.DimRegions
WITH rem_dup AS 
(
SELECT 
  DISTINCT(country)
FROM workspace.silver.silver_table
) 
SELECT *, row_number() OVER (ORDER BY country) as DimRegionKey FROM rem_dup

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gold.DimSales
AS
SELECT 
 row_number() OVER (ORDER BY order_id) as DimSaleKey,
 order_id,
 order_date,
 customer_id,
 customer_name,
 customer_email,
 product_id,
 product_name,
 product_category,
 payment_type,
 country,
 last_updated,
 Customer_Name_Upper,
 processDate
FROM
  workspace.silver.silver_table

-- COMMAND ----------

SELECT * FROM workspace.gold.DimSales

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Fact Table

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gold.FactSales
AS
SELECT 
  S.DimSaleKey,
  C.DimCustomerKey,
  P.DimProductKey,
  R.DimRegionKey,
  PY.DimPaymentKey,
  F.quantity,
  F.unit_price
FROM 
  workspace.silver.silver_table F
LEFT JOIN 
  workspace.gold.dimcustomers C
  ON F.customer_id = C.customer_id
LEFT JOIN 
  workspace.gold.dimproducts P
  ON F.product_id = P.product_id
LEFT JOIN 
  workspace.gold.dimregions R
  ON F.country = R.country
LEFT JOIN 
  workspace.gold.dimpayments PY
  ON F.payment_type = PY.payment_type
LEFT JOIN 
  workspace.gold.dimsales S
  ON F.order_id = S.order_id

-- COMMAND ----------

select * from workspace.gold.factsales

-- COMMAND ----------

