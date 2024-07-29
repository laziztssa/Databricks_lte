# Databricks notebook source
# MAGIC %md
# MAGIC #### Produce driver standings

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/Common_functions"

# COMMAND ----------

race_results_list = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
    .filter(f"file_date = '{v_file_date}'") \
    .select("race_year") \
    .distinct() \
    .collect()
   

# COMMAND ----------

race_year_list = []
for race_year in race_results_list:
    race_year_list.append(race_year.race_year)


# COMMAND ----------

from pyspark.sql.functions import col
race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
    .filter(col("race_year").isin(race_year_list))

# COMMAND ----------

from pyspark.sql.functions import sum, col, count, when
driver_standing_df = race_results_df \
    .groupBy("race_year", "driver_name", "driver_nationality") \
    .agg(sum("points").alias("total_points"), count(when(col("position") == 1, True)).alias("wins"))    

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standing_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

#overwrite_partition(final_df, 'f1_presentation', 'driver_standing', 'race_year')

merge_condition ="tgt.driver_name = src.driver_name and tgt.race_year = src.race_year"
overwrite_delta_mode(final_df, 'f1_presentation', 'driver_standing', presentation_folder_path, 'race_year', merge_condition )
