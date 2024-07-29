# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest qualifying folder

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the multiple json files using spark dataframe reader API

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

qualifying_Schema = StructType(fields=[StructField("qualifyId", IntegerType(), True),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("position", IntegerType(), True), 
                                    StructField("q1",  StringType(), True),
                                    StructField("q2",  StringType(), True),
                                    StructField("q3",  StringType(), True)
                                    
])

# COMMAND ----------

qualifying_df= spark.read.schema(qualifying_Schema).option("multiline", True).json(f"{raw_folder_path}/{v_file_date}/qualifying")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename Columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

qualifying_final_df= qualifying_df.withColumnRenamed("qualifyId", "qualify_id") \
                                    .withColumnRenamed("raceId", "race_id") \
                                    .withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("constructorId", "constructor_id") \
                                    .withColumn("ingestion_date", current_timestamp()) \
                                    .withColumn("data_source", lit(v_data_source)) \
                                    .withColumn("file_date", lit(v_file_date))   

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write to output to processed container in parquet format
# MAGIC
# MAGIC

# COMMAND ----------

#overwrite_partition(qualifying_final_df, 'f1_processed', 'qualifying', 'race_id')
#def overwrite_delta_mode(input_df, db_name, table_name, path_name, partition_column, merge_condition):
merge_condition ="tgt.qualify_id = src.qualify_id and tgt.race_id = src.race_id"
overwrite_delta_mode(qualifying_final_df, 'f1_processed', 'qualifying', processed_folder_path, 'race_id', merge_condition )    

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.qualifying
