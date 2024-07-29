# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest Pit_stops.json file

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

pit_stops_Schema = StructType(fields=[StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("stop", StringType(), True),
                                    StructField("lap", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("duration", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True)
                                    
])

# COMMAND ----------

pit_stops_df= spark.read.schema(pit_stops_Schema).option("multiline", True).json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename Columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

pit_stops_final_df= pit_stops_df.withColumnRenamed("raceId", "race_id") \
                                    .withColumnRenamed("driverId", "driver_id") \
                                    .withColumn("ingestion_date", current_timestamp()) \
                                    .withColumn("data_source", lit(v_data_source)) \
                                    .withColumn("file_date", lit(v_file_date))   

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write to output to processed container in parquet format
# MAGIC
# MAGIC

# COMMAND ----------

#overwrite_partition(pit_stops_final_df, 'f1_processed', 'pit_stops', 'race_id')
#def overwrite_delta_mode(input_df, db_name, table_name, path_name, partition_column, merge_condition):
merge_condition ="tgt.driver_id = src.driver_id and tgt.stop = src.stop and tgt.race_id = src.race_id"
overwrite_delta_mode(pit_stops_final_df, 'f1_processed', 'pit_stops', processed_folder_path, 'race_id', merge_condition )    

# COMMAND ----------


