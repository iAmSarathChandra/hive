create table if not exists order_details(
  InvoiceNo string, 
  stockCode string , 
  description string,
  quantity string ,
  invioceData string,
  unitPrice double,
  customerId int 
) partitioned by (country STRING)
stored as parquet
location 'dbfs:/FileStore/shared_uploads/iamsarathchandra.12@gmail.com'


SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;


CREATE OR REPLACE TEMP VIEW temp_orders_data 
USING CSV 
OPTIONS (
    path 'dbfs:/FileStore/shared_uploads/iamsarathchandra.12@gmail.com/data.csv',
    header 'true',
    inferSchema 'true'
);

select * from temp_orders_data limit 10

INSERT OVERWRITE  TABLE order_details PARTITION (country)
SELECT *
FROM temp_orders_data; 

select * from  order_details limit 10;

SELECT country, SUM(unitprice * quantity) AS total_revenue
FROM order_details
GROUP BY country
ORDER BY total_revenue DESC;

SELECT customerId , SUM(unitprice * quantity) as customerSpend 
FROM order_details 
GROUP BY customerId
ORDER BY customerSpend DESC
limit 10 ;

SELECT stockCode, SUM(quantity) AS total_sold
FROM order_details
GROUP BY stockCode
ORDER BY total_sold DESC
LIMIT 5;
