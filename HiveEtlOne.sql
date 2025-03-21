-- Databricks notebook source
CREATE EXTERNAL TABLE IF NOT EXISTS smartphones (
    brand STRING,
    model STRING,
    price DOUBLE,
    rating FLOAT,
    storage INT , 
    battery INT
)
PARTITIONED BY (ram STRING) 
STORED AS PARQUET

LOCATION 'dbfs:/FileStore/shared_uploads/*****************@gmail.com';


-- COMMAND ----------

INSERT INTO TABLE smartphones PARTITION (ram='8GB') 
VALUES ('Apple', 'iPhone 14', 999.99, 4.5, 128, 4000);

INSERT INTO TABLE smartphones PARTITION (ram='12GB') 
VALUES ('Samsung', 'Galaxy S23', 899.99, 4.6, 256, 4500);

INSERT INTO TABLE smartphones PARTITION (ram='6GB') 
VALUES ('Google', 'Pixel 7', 799.99, 4.4, 128, 4355);

INSERT INTO TABLE smartphones PARTITION (ram='8GB') 
VALUES ('OnePlus', '9 Pro', 749.99, 4.3, 256, 4500);

INSERT INTO TABLE smartphones PARTITION (ram='12GB') 
VALUES ('Xiaomi', 'Mi 11 Ultra', 699.99, 4.2, 256, 5000);

INSERT INTO TABLE smartphones PARTITION (ram='16GB') 
VALUES ('Asus', 'ROG Phone 6', 999.99, 4.7, 512, 6000);

INSERT INTO TABLE smartphones PARTITION (ram='4GB') 
VALUES ('Nokia', 'G50', 299.99, 3.9, 64, 5000);

INSERT INTO TABLE smartphones PARTITION (ram='6GB') 
VALUES ('Realme', 'GT Master', 399.99, 4.1, 128, 4300);

INSERT INTO TABLE smartphones PARTITION (ram='8GB') 
VALUES ('Vivo', 'X70 Pro', 699.99, 4.3, 256, 4450);

INSERT INTO TABLE smartphones PARTITION (ram='12GB') 
VALUES ('Oppo', 'Find X5 Pro', 849.99, 4.5, 256, 5000);

INSERT INTO TABLE smartphones PARTITION (ram='16GB') 
VALUES ('Black Shark', '4 Pro', 899.99, 4.6, 512, 6000);


-- COMMAND ----------

-- Top two costliest mobiles 
CREATE TABLE IF NOT EXISTS  top_smartphones AS 
select * from smartphones order by price desc limit 2 ;


-- COMMAND ----------

-- top two affordable mobiles.
CREATE TABLE IF NOT EXISTS top_flagshipphones AS 
select * from smartphones where storage > 128 and  ram ='12GB' order by price  asc limit 2 ;

-- COMMAND ----------

select * from top_smartphones ;

-- COMMAND ----------

select * from top_flagshipphones;
