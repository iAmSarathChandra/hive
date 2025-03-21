-- Databricks notebook source
-- MAGIC %python
-- display(dbutils.fs.ls("dbfs:/FileStore/shared_uploads/iamsarathchandra.12@gmail.com/"))


create table if not exists nba_data(
  name STRING, 
  number INT,
  position STRING,
  height STRING , 
  weight INT,
  college STRING,
  salary INT 
) partitioned by (team STRING)
stored as parquet
location 'dbfs:/FileStore/shared_uploads/iamsarathchandra.12@gmail.com'


-- creating a temp view and placing csv data into it.
  
CREATE OR REPLACE TEMP VIEW temp_nba_raw 
USING CSV 
OPTIONS (
    path 'dbfs:/FileStore/shared_uploads/iamsarathchandra.12@gmail.com/nba_copy_1.csv',
    header 'true',
    inferSchema 'true'
);


select * from temp_nba_raw order by 1 desc ;


set hive.exec.dynamic.partition.mode=nonstrict


INSERT OVERWRITE  TABLE nba_data PARTITION (team)
SELECT name, number, position, height, weight, college, salary, team
FROM temp_nba_raw; 



select * from nba_data order by 1 asc limit 10 ;


SELECT name, number, position, height, weight, COALESCE(college, 'Unknown') AS college, salary, team
FROM nba_data 
WHERE team = 'Chicago Bulls' 
ORDER BY 1 DESC;


-- creating table for a team

CREATE TABLE IF NOT EXISTS chicago_bulls (
    name STRING,
    number INT,
    position STRING,
    height STRING,
    weight INT,
    college STRING,
    salary INT,
    team STRING
)
CLUSTERED BY (number) INTO 3 BUCKETS
STORED AS PARQUET;


-- handeling the null values and inserting data ito chicago_bulls

INSERT OVERWRITE TABLE chicago_bulls
SELECT name, number, position, height, weight, 
       COALESCE(college, 'Unknown') AS college, 
       salary, team
FROM nba_data
WHERE team = 'Chicago Bulls'
ORDER BY name DESC;


-- description of the table (meta data.)

DESCRIBE FORMATTED chicago_bulls;

