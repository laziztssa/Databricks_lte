-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style="color:black;text-align:center;front-family:Ariel">Report On Dominant Formula1 Teams </h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

create or replace temp view v_dominant_teams
as 
select team_name,
        count(1) as total_races,
        SUM(calculated_points) as total_points,
        avg(calculated_points)as avg_points,
         rank() over(order by  avg(calculated_points) desc) as team_rank
from f1_presentation.calculated_race_results
group by team_name
having total_races>= 100
order by avg_points desc;

-- COMMAND ----------

select *  from v_dominant_teams

-- COMMAND ----------

select  race_year,
        team_name,
        count(1) as total_races,
        SUM(calculated_points) as total_points,
        avg(calculated_points)as avg_points,
        rank() over(order by  avg(calculated_points) desc) as team_rank
from f1_presentation.calculated_race_results
  where team_name in (select team_name from v_dominant_teams where team_rank<=10)
group by race_year,team_name
order by avg_points desc;

-- COMMAND ----------

select  race_year,
        team_name,
        count(1) as total_races,
        SUM(calculated_points) as total_points,
        avg(calculated_points)as avg_points,
        rank() over(order by  avg(calculated_points) desc) as team_rank
from f1_presentation.calculated_race_results
  where team_name in (select team_name from v_dominant_teams where team_rank<=10)
group by race_year,team_name
order by avg_points desc;

-- COMMAND ----------

select  race_year,
        team_name,
        count(1) as total_races,
        SUM(calculated_points) as total_points,
        avg(calculated_points)as avg_points,
        rank() over(order by  avg(calculated_points) desc) as team_rank
from f1_presentation.calculated_race_results
  where team_name in (select team_name from v_dominant_teams where team_rank<=10)
group by race_year,team_name
order by avg_points desc;
