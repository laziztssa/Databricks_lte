# Databricks notebook source
# MAGIC %md
# MAGIC ####
# MAGIC 1. Write data to delta lake (managed table)
# MAGIC 2. Write data to delta lake (external table)
# MAGIC 3. Read data from delta lake (Table)
# MAGIC 4. Read data from delta lake (File)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo
# MAGIC LOCATION '/mnt/formula1ltedl/demo'

# COMMAND ----------

result_df = spark.read.option("inferSchema",  True).json("/mnt/formula1ltedl/raw/2021-03-28/results.json")

# COMMAND ----------

result_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

result_df.write.format("delta").mode("overwrite").save("/mnt/formula1ltedl/demo/results_externals")

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE TABLE f1_demo.results_external
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/formula1ltedl/demo/results_externals'

# COMMAND ----------

results_external_df=spark.read.format("delta").load("/mnt/formula1ltedl/demo/results_externals")

# COMMAND ----------

result_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("f1_demo.results_partitionned")

# COMMAND ----------

# MAGIC %md
# MAGIC ####
# MAGIC 1. Update Delta Table
# MAGIC 2. Delete from Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

# MAGIC %sql 
# MAGIC UPDATE f1_demo.results_managed
# MAGIC SET points = 11 - position
# MAGIC where position<=10

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/formula1ltedl/demo/results_managed")
deltaTable.update("position <= 10" , {"points": "21 - position"})


# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/formula1ltedl/demo/results_managed")
deltaTable.delete("position > 10")

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/formula1ltedl/demo/results_managed")
deltaTable.delete("points = 0")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #####
# MAGIC Upsert Using Merge

# COMMAND ----------

# Drivers Day 1
drivers_day1_df = spark.read \
    .option("inferSchema",  True) \
        .json("/mnt/formula1ltedl/raw/2021-03-28/drivers.json") \
            .filter("driverId <= 10") \
                .select("driverId", "dob", "name.forename", "name.surname")

# Drivers Day 2
from pyspark.sql.functions import upper
drivers_day2_df = spark.read \
    .option("inferSchema",  True) \
        .json("/mnt/formula1ltedl/raw/2021-03-28/drivers.json") \
            .filter("driverId Between 6 and 15") \
                .select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# Drivers Day 3
from pyspark.sql.functions import upper
drivers_day3_df = spark.read \
    .option("inferSchema",  True) \
        .json("/mnt/formula1ltedl/raw/2021-03-28/drivers.json") \
            .filter("driverId Between 1 and 5 OR driverId Between 16 and 20") \
                .select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))



# COMMAND ----------

# Create tempView for the 3 dataframes
drivers_day1_df.createOrReplaceTempView("drivers_day1")
drivers_day2_df.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_merge (
# MAGIC driverId INT, 
# MAGIC dob DATE, 
# MAGIC forename STRING,
# MAGIC surname STRING,
# MAGIC createdDate DATE,
# MAGIC updatedDate DATE)
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC /*day1*/
# MAGIC MERGE into f1_demo.drivers_merge a
# MAGIC USING drivers_day1 b
# MAGIC on a.driverId = b.driverId
# MAGIC When matched THEN
# MAGIC   UPDATE SET a.dob = b.dob,
# MAGIC              a.forename =b.forename,
# MAGIC              a.surname = b.surname,
# MAGIC              a.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   Insert (driverId,dob, forename,surname,createdDate) VALUES (driverId,dob, forename,surname,current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC /*day2*/
# MAGIC MERGE into f1_demo.drivers_merge a
# MAGIC USING drivers_day2 b
# MAGIC on a.driverId = b.driverId
# MAGIC When matched THEN
# MAGIC   UPDATE SET a.dob = b.dob,
# MAGIC              a.forename =b.forename,
# MAGIC              a.surname = b.surname,
# MAGIC              a.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   Insert (driverId,dob, forename,surname,createdDate) VALUES (driverId,dob, forename,surname,current_timestamp)

# COMMAND ----------

#from delta.tables import DeltaTable

#deltaTable = DeltaTable.forPath(spark, "/mnt/formula1ltedl/demo/drivers_merge")
#deltaTable.delete()

# COMMAND ----------

# day 3 merge with pyspark

from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp

deltaTable= deltaTable.forPath(spark, "/mnt/formula1ltedl/demo/drivers_merge")

deltaTable.alias("tgt").merge(drivers_day3_df.alias("upd"), "tgt.driverId = upd.driverId") \
        .whenMatchedUpdate(set = {"dob": "upd.dob", "forename": "upd.forename", "surname": "upd.surname", "updatedDate": "current_timestamp()"}) \
        .whenNotMatchedInsert (values={
                "driverId": "upd.driverId",
                "dob": "upd.dob",
                "forename": "upd.forename",
                "surname": "upd.surname",
                "createdDate": "current_timestamp()"
                }
                ) \
        .execute()

# COMMAND ----------

# MAGIC %md
# MAGIC 1. History & versionning
# MAGIC 2. Time travel
# MAGIC 3. Vaccum

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge timestamp as of '2024-06-01T13:49:23.000+00:00';

# COMMAND ----------

df = spark.read.format("delta").option("timestampAsOf", '2024-06-01T14:08:07.000+00:00').load("/mnt/formula1ltedl/demo/drivers_merge")

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM f1_demo.drivers_merge RETAIN 0 HOURS

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from f1_demo.drivers_merge where driverId=1;

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  f1_demo.drivers_merge version as of 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into f1_demo.drivers_merge as a using f1_demo.drivers_merge version as of 10 b on (a.driverId=b.driverId)
# MAGIC when not matched then 
# MAGIC insert *

# COMMAND ----------

# MAGIC %md
# MAGIC #### DeltaLake Transaction Log

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_txn (
# MAGIC driverId INT, 
# MAGIC dob DATE, 
# MAGIC forename STRING,
# MAGIC surname STRING,
# MAGIC createdDate DATE,
# MAGIC updatedDate DATE)
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history f1_demo.drivers_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into f1_demo.drivers_txn
# MAGIC select * from f1_demo.drivers_merge
# MAGIC where driverId=2;

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from f1_demo.drivers_txn
# MAGIC where driverId=1;

# COMMAND ----------

for driver_id in range(3, 20):
    spark.sql(f"""insert into f1_demo.drivers_txn
                  select * from f1_demo.drivers_merge where driverId = {driver_id}""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Convert Parquet to Delta

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_convert_to_delta (
# MAGIC driverId INT, 
# MAGIC dob DATE, 
# MAGIC forename STRING,
# MAGIC surname STRING,
# MAGIC createdDate DATE,
# MAGIC updatedDate DATE)
# MAGIC USING PARQUET

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into f1_demo.drivers_convert_to_delta 
# MAGIC select * from f1_demo.drivers_merge
# MAGIC ;

# COMMAND ----------

# MAGIC %sql 
# MAGIC CONVERT TO DELTA f1_demo.drivers_convert_to_delta 

# COMMAND ----------

df = spark.table("f1_demo.drivers_convert_to_delta")
df.write.format("parquet").save("/mnt/formula1ltedl/demo/drivers_convert_to_delta_new")

# COMMAND ----------

# MAGIC %sql 
# MAGIC CONVERT TO DELTA parquet.`/mnt/formula1ltedl/demo/drivers_convert_to_delta_new`
