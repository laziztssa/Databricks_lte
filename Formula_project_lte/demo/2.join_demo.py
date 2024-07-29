# Databricks notebook source
# MAGIC %md
# MAGIC ##### Spark Join Transformation

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races").filter("race_year = 2019").withColumnRenamed("name", "race_name")
circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits").filter("circuit_id <70 ").withColumnRenamed("name", "circuit_name")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### inner join

# COMMAND ----------

#inner join 
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "inner") \
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Left outer join

# COMMAND ----------

#left outer join 
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "leftouter") \
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### right outer join

# COMMAND ----------

#right outer join 
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "right") \
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

#right outer join 
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "full") \
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### semi Join

# COMMAND ----------

#Semi Join
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "semi").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Anti join

# COMMAND ----------

race_circuits_df = races_df.join(circuits_df, circuits_df.circuit_id == races_df.circuit_id, "anti").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Cross Join

# COMMAND ----------

race_circuits_df = races_df.crossJoin(circuits_df).display()
