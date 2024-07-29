-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Learning Objectives
-- MAGIC   1. Create external table using Python
-- MAGIC   2. Create external table using SQL
-- MAGIC   3. Effect and dropping a managed table
-- MAGIC

-- COMMAND ----------

-- MAGIC %run "../Includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC race_results_df.write.format("parquet").option("path",f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

DESC EXTENDED demo.race_results_ext_py;

-- COMMAND ----------

CREATE TABLE demo.race_results_ext_sql
(race_year INT,
race_name STRING,
race_date TIMESTAMP,
circuit_location STRING,
driver_name STRING,
driver_number INT,
driver_nationality STRING,
team STRING,
grid INT,
fastest_lap INT,
race_time STRING,
points FLOAT,
position INT,
created_date TIMESTAMP
)
USING parquet
LOCATION "/mnt/formula1ltedl/presentation/race_results_ext_sql"

-- COMMAND ----------

SHOW TABLES in demo;

-- COMMAND ----------

SELECT * from demo.race_results_ext_sql

-- COMMAND ----------

select * from demo.race_results_ext_sql

-- COMMAND ----------

DROP TABLE demo.race_results_ext_sql

-- COMMAND ----------

SHOW TABLES in demo;

-- COMMAND ----------

select * from global_temp.gv_race_results
