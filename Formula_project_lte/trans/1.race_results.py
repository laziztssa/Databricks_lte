# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/Common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### read data 

# COMMAND ----------

# read driver data +renamed columns
drivers_df = spark.read.format("delta").load(f"{processed_folder_path}/drivers").withColumnRenamed("number","driver_number") \
    .withColumnRenamed("name", "driver_name") \
    .withColumnRenamed("nationality", "driver_nationality")

# read races data +renamed columns
races_df = spark.read.format("delta").load(f"{processed_folder_path}/races").withColumnRenamed("name", "race_name") \
    .withColumnRenamed("race_timestamp", "race_date")

# read races data +renamed columns
circuits_df = spark.read.format("delta").load(f"{processed_folder_path}/circuits").withColumnRenamed("location", "circuit_location")

# read results data +renamed columns
results_df = spark.read.format("delta").load(f"{processed_folder_path}/results") \
    .filter(f"file_date = '{v_file_date}'") \
    .withColumnRenamed("time", "race_time") \
    .withColumnRenamed("race_id", "results_race_id") \
    .withColumnRenamed("file_date", "results_file_date")      

# read constructors data +renamed columns
constructors_df = spark.read.format("delta").load(f"{processed_folder_path}/constructors").withColumnRenamed("name", "team")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### join circuits to race

# COMMAND ----------

race_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner") \
    .select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### joins result to all other dataframes

# COMMAND ----------

race_results_df = results_df.join(race_circuits_df, results_df.results_race_id == race_circuits_df.race_id) \
                            .join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
                            .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = race_results_df.select("race_id", "race_year", "race_name", "race_date", "circuit_location", "driver_name", 
                                  "driver_number", "driver_nationality", "team", "grid", "fastest_lap", "race_time", "points", "position", "results_file_date") \
                          .withColumn("created_date", current_timestamp()) \
                          .withColumnRenamed("results_file_date", "file_date")


# COMMAND ----------

#overwrite_partition(final_df, 'f1_presentation', 'race_results', 'race_id')
merge_condition ="tgt.race_id = src.race_id and tgt.driver_name = src.driver_name"
overwrite_delta_mode(final_df, 'f1_presentation', 'race_results', presentation_folder_path, 'race_id', merge_condition )    
