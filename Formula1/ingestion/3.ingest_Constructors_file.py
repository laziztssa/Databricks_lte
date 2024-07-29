# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest Constructors.json file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read the JSON file using spark dataframe reader

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

constructors_schema ="constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df =spark.read.schema(constructors_schema).json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Drop unwanted columns from dataframe

# COMMAND ----------

constructor_dropped_df = constructor_df.drop('URL')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3- Rename Columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

constructor_final_df=constructor_dropped_df.withColumnRenamed("constructorId" , "constructor_id") \
                                           .withColumnRenamed("constructorRef", "constructor_ref") \
                                           .withColumn("ingestion_date", current_timestamp() ) \
                                           .withColumn("data_source", lit(v_data_source)) \
                                            .withColumn("file_date", lit(v_file_date))     

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write output to parquet file

# COMMAND ----------

constructor_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")
