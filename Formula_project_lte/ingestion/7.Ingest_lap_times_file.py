# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest lap_times folder

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the multiple csv files using spark dataframe reader API

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

lap_times_Schema = StructType(fields=[StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("lap", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("time", StringType(), True), 
                                    StructField("milliseconds", IntegerType(), True)
                                    
])

# COMMAND ----------

lap_times_df= spark.read.schema(lap_times_Schema).csv(f"{raw_folder_path}/{v_file_date}/lap_times")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename Columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

lap_times_final_df= lap_times_df.withColumnRenamed("raceId", "race_id") \
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

#overwrite_partition(lap_times_final_df, 'f1_processed', 'lap_times', 'race_id')
merge_condition ="tgt.driver_id = src.driver_id and tgt.lap = src.lap and tgt.race_id = src.race_id"
overwrite_delta_mode(lap_times_final_df, 'f1_processed', 'lap_times', processed_folder_path, 'race_id', merge_condition )    
