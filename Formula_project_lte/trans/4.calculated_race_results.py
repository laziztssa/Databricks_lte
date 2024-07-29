# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

spark.sql (f"""
            CREATE TABLE IF NOT EXISTS f1_presentation.calculated_race_results
                (
                 race_year INT,
                 team_name STRING,
                 driver_id INT,
                 driver_name STRING,
                 race_id INT,
                 position INT,
                 points INT,
                 calculated_points INT,
                 created_date TIMESTAMP,
                 updated_date TIMESTAMP
                )
                USING DELTA
        """)

# COMMAND ----------

spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW race_results_updated
        AS
        SELECT d.race_year,
            c.name as team_name,
            b.name as driver_name,
            b.driver_id,
            d.race_id,
            a.position,
            a.points,
            11 - a.position as calculated_points
        FROM f1_processed.results A
        JOIN f1_processed.drivers B ON  (A.driver_id=B.driver_id)
        JOIN f1_processed.constructors C ON(a.constructor_id=c.constructor_id)
        JOIN f1_processed.races D ON(a.race_id=d.race_id)
        WHERE a.position<= 10
        AND A.file_date = '{v_file_date}'
        """);

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE into f1_presentation.calculated_race_results tgt
# MAGIC USING race_results_updated src
# MAGIC on (tgt.driver_id = src.driver_id and tgt.race_id = src.race_id)
# MAGIC When matched THEN
# MAGIC   UPDATE SET tgt.position = src.position,
# MAGIC              tgt.points =src.points,
# MAGIC              tgt.calculated_points = src.calculated_points,
# MAGIC              tgt.updated_date = current_timestamp
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   Insert (race_year,team_name ,driver_name ,driver_id,race_id,position,points,calculated_points,created_date) VALUES (race_year,team_name ,driver_name ,driver_id, race_id,position,points,calculated_points,current_timestamp)

# COMMAND ----------


