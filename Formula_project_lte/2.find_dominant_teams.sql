-- Databricks notebook source
select team_name,
        count(1) as total_races,
        SUM(calculated_points) as total_points,
        avg(calculated_points)as avg_points
from f1_presentation.calculated_race_results
group by team_name
having total_races>= 100
order by avg_points desc;

-- COMMAND ----------

select team_name,
        count(1) as total_races,
        SUM(calculated_points) as total_points,
        avg(calculated_points)as avg_points
from f1_presentation.calculated_race_results
WHERE race_year between 2001 and 2011
group by team_name
having total_races>= 100
order by avg_points desc;
