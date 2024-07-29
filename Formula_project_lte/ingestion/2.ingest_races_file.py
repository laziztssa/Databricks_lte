# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest races.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step1 - Read the csv file using the spark dataframe reader

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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("year", IntegerType(), True),
                                      StructField("round", IntegerType(), True),
                                      StructField("circuitId", IntegerType(), True),
                                      StructField("name", StringType(), True),
                                      StructField("date", DateType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("url", StringType(), True)
])

# COMMAND ----------

races_df = spark.read.option("header", True).schema(races_schema).csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step2 - Add ingestion date and race timestamps to the data Frame

# COMMAND ----------

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, to_timestamp, col, concat

# COMMAND ----------

races_with_timestamp_df=races_df.withColumn("ingestion_date", current_timestamp()) \
                                .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-mm-dd HH:mm:ss')) \
                                .withColumn("data_source", lit(v_data_source)) \
                                .withColumn("file_source", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3- Select Only the columns required

# COMMAND ----------

races_selected_df=races_with_timestamp_df.select(col('raceId').alias('race_id'), col('year').alias('race_year'), col('round'),  col('circuitId').alias('circuit_id'), col('name'), col('ingestion_date'), col('race_timestamp'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4- Write data to datalake as parquet

# COMMAND ----------

#races_selected_df.write.mode("overwrite").partitionBy('race_year').parquet("/mnt/formula1ltedl/processed/races")
races_selected_df.write.mode("overwrite").partitionBy('race_year').format("delta").saveAsTable("f1_processed.races")
