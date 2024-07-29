-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Learning Objectives
-- MAGIC   1. Create managed table using Python
-- MAGIC   2. Create managed table using SQL
-- MAGIC   3. Effect and dropping a managed table
-- MAGIC   4. Descrite table

-- COMMAND ----------

-- MAGIC %run "../Includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

USE demo;
SHOW TABLES;

-- COMMAND ----------


DESCRIBE EXTENDED race_results_python;

-- COMMAND ----------

select * from demo.race_results_python
where race_year= 2020;


-- COMMAND ----------

USE demo;
create or replace table race_results_sql 
as 
select * from demo.race_results_python
where race_year= 2020;

-- COMMAND ----------

drop table race_results_sql;

-- COMMAND ----------


