-- Databricks notebook source
CREATE TABLE IF NOT EXISTS workspace.default.source_data
(
    order_id INT PRIMARY KEY,
    order_date DATE,
    customer_id INT,
    customer_name VARCHAR(100),
    customer_email VARCHAR(100),
    product_id INT,
    product_name VARCHAR(100),
    product_category VARCHAR(50),
    quantity INT,
    unit_price DECIMAL(10, 2),
    payment_type VARCHAR(50),
    country VARCHAR(50),
    last_updated DATE
)

-- COMMAND ----------

-- Initial Load
INSERT INTO workspace.default.source_data VALUES 
(1001, '2024-07-01', 1, 'Alice Johnson', 'alice@gmail.com', 501, 'iPhone 14', 'Electronics', 1, 999.99, 'Credit Card', 'USA', '2024-07-01'),
(1002, '2024-07-01', 2, 'Bob Smith', 'bob@yahoo.com', 502, 'AirPods Pro', 'Electronics', 2, 199.99, 'PayPal', 'USA', '2024-07-01'),
(1003, '2024-07-01', 3, 'Charlie Brown', 'charlie@outlook.com', 503, 'Nike Shoes', 'Footwear', 1, 129.99, 'Credit Card', 'Canada', '2024-07-01');

-- COMMAND ----------

SELECT * FROM workspace.default.source_data

-- COMMAND ----------

-- Incremental Load
INSERT INTO workspace.default.source_data VALUES 
(1004, '2024-07-02', 4, 'David Lee', 'david@abc.com', 504, 'Samsung S23', 'Electronics', 1, 899.99, 'Credit Card', 'USA', '2024-07-02'),
(1005, '2024-07-02', 1, 'Alice Johnson', 'alice@gmail.com', 503, 'Nike Shoes', 'Footwear', 2, 129.99, 'Credit Card', 'USA', '2024-07-02');

-- COMMAND ----------

