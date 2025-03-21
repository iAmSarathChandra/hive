--Manual code for the static partitons 

INSERT INTO TABLE sales_data PARTITION (country='USA')
SELECT customer_id, product, amount, date FROM raw_sales WHERE country = 'USA';

INSERT INTO TABLE sales_data PARTITION (country='India')
SELECT customer_id, product, amount, date FROM raw_sales WHERE country = 'India';

INSERT INTO TABLE sales_data PARTITION (country='UK')
SELECT customer_id, product, amount, date FROM raw_sales WHERE country = 'UK';

INSERT INTO TABLE sales_data PARTITION (country='Germany')
SELECT customer_id, product, amount, date FROM raw_sales WHERE country = 'Germany';

INSERT INTO TABLE sales_data PARTITION (country='Canada')
SELECT customer_id, product, amount, date FROM raw_sales WHERE country = 'Canada';


--code to automate the static partition

SET hive.exec.dynamic.partition = false;

-- Step 1: Get Unique Country List (Run Separately)
SELECT DISTINCT country FROM raw_sales;

-- Step 2: Generate Dynamic Queries (Run as a Loop in Shell)
!for country in (SELECT DISTINCT country FROM raw_sales);
DO
    INSERT INTO TABLE sales_data PARTITION (country='${country}')
    SELECT customer_id, product, amount, date FROM raw_sales WHERE country = '${country}';
END;


--code for the dynamic partiton

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;  -- Enables full dynamic partitioning

INSERT INTO TABLE sales PARTITION (year, month)
SELECT customer_id, amount, year, month FROM sales_raw;


