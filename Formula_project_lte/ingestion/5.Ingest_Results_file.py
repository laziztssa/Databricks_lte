# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest Results.json file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the JSON file using spark dataframe reader API

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/Common_functions"

# COMMAND ----------

from pyspark.sql.types import  StructField, StructType, StringType, DateType, IntegerType, FloatType

# COMMAND ----------

results_Schema = StructType(fields=[StructField("resultId", IntegerType(), True),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("positionText", StringType(), True),
                                    StructField("positionOrder", IntegerType(), True),
                                    StructField("points", FloatType(), True),
                                    StructField("laps", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapTime", IntegerType(), True),
                                    StructField("fastestLapSpeed", FloatType(), True),
                                    StructField("statusId", StringType(), True)
])

# COMMAND ----------

results_df= spark.read.schema(results_Schema).json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename Columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

results_with_columns_df= results_df.withColumnRenamed("resultId", "result_id") \
                                    .withColumnRenamed("raceId", "race_id") \
                                    .withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("constructorId", "constructor_id") \
                                    .withColumnRenamed("positionText", "position_text") \
                                    .withColumnRenamed("positionOrder", "position_order") \
                                    .withColumnRenamed("fastestLap", "fastest_lap") \
                                    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                                    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                                    .withColumn("ingestion_date", current_timestamp()) \
                                    .withColumn("data_source", lit(v_data_source)) \
                                    .withColumn("file_date", lit(v_file_date))      

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Drop the unwanted columns
# MAGIC

# COMMAND ----------

results_final_df=results_with_columns_df.drop(col("statusId"))

# COMMAND ----------

result_deduped_df = results_final_df.dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write to output to processed container in parquet format
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Method 1

# COMMAND ----------

# MAGIC %md
# MAGIC #### Method 2

# COMMAND ----------

#overwrite_partition(results_final_df, 'f1_processed', 'results', 'race_id')

# COMMAND ----------

#def overwrite_delta_mode(input_df, db_name, table_name, path_name, partition_column, merge_condition):
merge_condition ="tgt.result_id = src.result_id and tgt.race_id = src.race_id"
overwrite_delta_mode(result_deduped_df, 'f1_processed', 'results', processed_folder_path, 'race_id', merge_condition )    

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC select driver_id , race_id , file_date, count(*) from f1_processed.results
# MAGIC group by driver_id , race_id , file_date
# MAGIC having count(*)>1
