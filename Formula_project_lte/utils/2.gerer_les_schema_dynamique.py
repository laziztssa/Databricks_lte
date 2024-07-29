# Databricks notebook source
df = spark.read.json("/mnt/formula1ltedl/raw/2021-03-21/constructors.json")

# COMMAND ----------

schema=df.schema

# COMMAND ----------

schema

# COMMAND ----------

circuits_df = spark.read.option("header", True).schema(schema).csv("/mnt/formula1ltedl/raw/2021-03-21/circuits.csv").display()
