# Databricks notebook source
# MAGIC %run "../Includes/configuration"

# COMMAND ----------

race_df = spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

display(race_df)

# COMMAND ----------

races_filtered_df = race_df.filter("race_year= 2019 and round<=5")

# COMMAND ----------

races_filtered_df = race_df.where((race_df["race_year"] == 2019) & (race_df["round"] <= 5))

# COMMAND ----------

display(races_filtered_df)

# COMMAND ----------


