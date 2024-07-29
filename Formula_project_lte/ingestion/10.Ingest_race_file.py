# Databricks notebook source
# MAGIC %md 
# MAGIC ## Entrainement Pyspark
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1-preparation environnement

# COMMAND ----------

# MAGIC %run "../Includes/configuration"
# MAGIC

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2-Reccuperation dynamique du schema

# COMMAND ----------

Race_Schema=StructType([StructField('raceId', StringType(), True), 
			StructField('year', StringType(), True), 
			StructField('round', StringType(), True), 
			StructField('circuitId', StringType(), True), 
			StructField('name', StringType(), True), 
			StructField('date', StringType(), True), 
			StructField('time', StringType(), True), 
			StructField('url', StringType(), True)])

# COMMAND ----------



# COMMAND ----------

Results_df = spark.read.option("header", True).schema(Race_Schema).csv(f"{raw_folder_path}/races.csv")

# COMMAND ----------

suivant = Window.partitionBy("circuitId").orderBy("date")
Results_filtered_df = Results_df.filter((Results_df.year > '1900') & (Results_df.name == 'Austrian Grand Prix')) \
                                .orderBy(Results_df.year.desc()) \
                                .withColumn ("durée", datediff(lead(Results_df.date).over(suivant), Results_df.date)) \
                                .
                                       
                                     
                                  
                               
                                 

# COMMAND ----------



# COMMAND ----------

Results_date_calcul.display()

