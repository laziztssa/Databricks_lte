-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Views on tables
-- MAGIC #### Learning Objectives
-- MAGIC 1. Create Temp View
-- MAGIC 2. Create Global Temp View
-- MAGIC 3. Create Permanent view

-- COMMAND ----------

Use demo;
select current_database()

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_race_results
AS
select * 
  FROM demo.race_results_python
where race_year=2018;

-- COMMAND ----------

drop view v_race_results

-- COMMAND ----------

select * from v_race_results

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW gv_race_results
AS
select * 
  FROM demo.race_results_python
where race_year=2012;

-- COMMAND ----------

select * from global_temp.gv_race_results

-- COMMAND ----------

show tables in global_temp;

-- COMMAND ----------

CREATE OR REPLACE VIEW pv_race_results
AS
select * 
  FROM demo.race_results_python
where race_year=2000;

-- COMMAND ----------

show tables;

-- COMMAND ----------

use f1_presentation;

-- COMMAND ----------

select * from f1_processed.drivers
