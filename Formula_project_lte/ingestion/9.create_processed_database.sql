-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/formula1ltedl/processed"

-- COMMAND ----------

DROP TABLE f1_processed.circuits;
DROP TABLE f1_processed.constructors;
DROP TABLE f1_processed.drivers;
DROP TABLE f1_processed.lap_times;
DROP TABLE f1_processed.pit_stops;
drop TABLE f1_processed.qualifying;
DROP table f1_processed.races;
DROP TABLE f1_processed.results;
