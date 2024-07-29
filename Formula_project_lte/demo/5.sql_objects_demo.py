# Databricks notebook source
# MAGIC %md
# MAGIC ### Lesson Objectives
# MAGIC   1. Spark SQl documentation
# MAGIC   2. Create Database demo
# MAGIC   3. Data tab in the UI
# MAGIC   4. SHOW command
# MAGIC   5. DESCRIBE command
# MAGIC   6. find the currend database

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS demo; 

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases

# COMMAND ----------

# MAGIC %sql
# MAGIC describe database demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe database extended demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_database();
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES in demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE DEMO;
# MAGIC SHOW TABLES;
