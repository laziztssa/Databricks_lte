# Databricks notebook source
v_result = dbutils.notebook.run("1.ingest_circuits_file", 0,  {"p_data_souce": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("2.ingest_races_file", 0, {"p_data_souce": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("3.ingest_Constructors_file", 0, {"p_data_souce": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("4.Ingest_Drivers_file", 0, {"p_data_souce": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("5.Ingest_Results_file", 0, {"p_data_souce": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("6.Ingest_Pit_Stops_file", 0, {"p_data_souce": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("7.Ingest_lap_times_file", 0, {"p_data_souce": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("8.Ingest_qualifying_file", 0, {"p_data_souce": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

v_result
