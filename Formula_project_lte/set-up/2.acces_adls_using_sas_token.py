# Databricks notebook source
# MAGIC %md
# MAGIC #### Acces Azure Data Lake using SAS Token
# MAGIC 1. set the spark config fs.azure.account.key
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv file

# COMMAND ----------

formula1lteld_demo_SAS_token = dbutils.secrets.get(scope='formula1-scope', key='formula1lteld-demo-SAS-token')

# COMMAND ----------

 spark.conf.set("fs.azure.account.auth.type.formula1ltedl.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1ltedl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1ltedl.dfs.core.windows.net", formula1lteld_demo_SAS_token)


# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1ltedl.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1ltedl.dfs.core.windows.net/circuits.csv"))
