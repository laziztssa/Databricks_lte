-- Databricks notebook source
-- MAGIC %run "../Includes/configuration"

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create circuits tables

-- COMMAND ----------

--

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits ;
CREATE TABLE IF NOT EXISTS f1_raw.circuits (circuitId INT,
circuitRef STRING,
name STRING,
location STRING,
country STRING,
lat DOUBLE,
lng DOUBLE,
alt INT,
url STRING
)
using csv
OPTIONS (path '/mnt/formula1ltedl/raw/circuits.csv', header true) ;    

-- COMMAND ----------

select * from f1_raw.circuits

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create races tables

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races ;
CREATE TABLE IF NOT EXISTS f1_raw.races(raceId INT,
year INT,
round INT,
circuitId INT,
name String,
date Date,
time String,
url String      
)
using csv
OPTIONS (path '/mnt/formula1ltedl/raw/races.csv', header true) ; 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create Tables for JSON files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create Constructors Table
-- MAGIC 1. Single Line JSON
-- MAGIC 2. Simple Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors ;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(constructorId INT, 
constructorRef STRING, 
name STRING, 
nationality STRING, 
url STRING)
using json
OPTIONS (path '/mnt/formula1ltedl/raw/constructors.json') ; 


-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create drivers table
-- MAGIC 1. Single Line JSON
-- MAGIC 2. complex  Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers ;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(driverId Int,
driverRef String,
number Int,
code String,
name STRUCT<forename: String, surname: String >,
dob Date,
nationality String,
url String)
using json
OPTIONS (path '/mnt/formula1ltedl/raw/drivers.json') ; 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create results tables
-- MAGIC 1. Single Line JSON
-- MAGIC 2. Simple Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results ;
CREATE TABLE IF NOT EXISTS f1_raw.results( resultId  Integer ,
 raceId  Integer ,
 driverId  Integer ,
 constructorId  Integer ,
 number  Integer ,
 grid  Integer ,
 position  Integer ,
 positionText  String ,
 positionOrder  Integer ,
 points  Float ,
 laps  Integer ,
 time  String ,
 milliseconds  Integer ,
 fastestLap  Integer ,
 rank  Integer ,
 fastestLapTime  Integer ,
 fastestLapSpeed  Float ,
 statusId  String )
using json
OPTIONS (path '/mnt/formula1ltedl/raw/results.json') ; 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create pit_stops tables
-- MAGIC 1. MultiLine JSON
-- MAGIC 2. Simple Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops ;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops( raceId  Integer ,
driverId  Integer ,
stop  String ,
lap  Integer ,
time  String ,
duration  String ,
milliseconds  Integer
)
using json
OPTIONS (path '/mnt/formula1ltedl/raw/pit_stops.json', multiline true) ;  

-- COMMAND ----------

select * from f1_raw.pit_stops

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create Qualifying tables
-- MAGIC 1. JSON file
-- MAGIC 2. MultiLine JSON
-- MAGIC 3. Multiple files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying ;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying(qualifyId INT ,
raceId INT ,
driverId INT ,
constructorId INT ,
position INT , 
q1  STRING ,
q2  STRING ,
q3  STRING 
)
using json
OPTIONS (path "/mnt/formula1ltedl/raw/qualifying", multiline true) ;  

-- COMMAND ----------

select * from f1_raw.qualifying

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create lap_times tables
-- MAGIC 1. csv 
-- MAGIC 2. Simple Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times ;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times(raceId  Integer ,
 driverId  Integer ,
 lap  Integer ,
 position  Integer ,
 time  String , 
 milliseconds  Integer 
)
using csv
OPTIONS (path "/mnt/formula1ltedl/raw/lap_times") ;  

-- COMMAND ----------

select * from f1_raw.lap_times

-- COMMAND ----------

DESC EXTENDED f1_raw.lap_times
