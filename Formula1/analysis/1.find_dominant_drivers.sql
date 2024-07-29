-- Databricks notebook source
REFRESH TABLE f1_presentation.calculated_race_results


-- COMMAND ----------

select driver_name,
        count(1) as total_races,
        SUM(calculated_points) as total_points,
        avg(calculated_points)as avg_points
from f1_presentation.calculated_race_results
WHERE race_year between 2011 and 2020
group by driver_name
having total_races>= 50
order by avg_points desc;
