# Databricks notebook source
# MAGIC %md
# MAGIC #### Acces Azure Data Lake using access keys
# MAGIC 1. set the spark config fs.azure.account.key
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv file

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1ltedl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1ltedl.dfs.core.windows.net/circuits.csv"))
