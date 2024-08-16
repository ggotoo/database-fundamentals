use mmai_db;

SELECT * FROM assignment01.bakery_sales ORDER BY quantity DESC;

-- Q1
SELECT *
FROM (SELECT YEAR(sale_date) as year,
       MONTH(sale_date) as month,
       article,
       SUM(quantity) as quantity,
       SUM(quantity * unit_price) as revenue,
       RANK() OVER (PARTITION BY YEAR(sale_date), MONTH(sale_date) ORDER BY SUM(quantity) DESC) as highest_quantity
      FROM assignment01.bakery_sales
      GROUP BY YEAR(sale_date), MONTH(sale_date), article) as test
WHERE highest_quantity <=3
ORDER BY year, month, highest_quantity ASC;

-- Q2
SELECT *
FROM (SELECT ticket_number, COUNT(DISTINCT article) as number_of_articles
      FROM assignment01.bakery_sales
      WHERE YEAR(sale_date) = 2021
        AND MONTH(sale_date) = 12
      GROUP BY ticket_number) as number_of_articles_by_ticket_number
WHERE number_of_articles >= 5;

-- Q3
SELECT hour_of_day
FROM (SELECT DATEPART(HOUR, sale_time) as hour_of_day, RANK() OVER (ORDER BY SUM(quantity) DESC ) as rank
      FROM assignment01.bakery_sales
      WHERE article = 'Traditional Baguette' AND MONTH(sale_date) = 7
      GROUP BY DATEPART(HOUR, sale_time)
      ) as baguette_sales_by_hour
WHERE rank = 1;
-- Most popular hour of day is between 11 am and 12 pm

-- Q4
-- Check for any null values
SELECT * FROM assignment01.bakery_sales
WHERE sale_date IS NULL
   OR sale_time IS NULL
   OR ticket_number IS NULL
   OR article IS NULL
   OR quantity IS NULL
   OR unit_price IS NULL
   OR sale_datetime IS NULL;
-- There are 5 rows with null unit_price (also invalid article name)

-- Check for duplicates
SELECT *, COUNT(*)
FROM assignment01.bakery_sales
GROUP BY sale_datetime, sale_date, sale_time, ticket_number, article, quantity, unit_price
HAVING COUNT(*) > 1;
-- There are 1155 duplicate rows

-- Check for outliers or invalid values for quantity
SELECT AVG(quantity) as average_quantity, MAX(quantity) as max_quantity, MIN(quantity) as min_quantity
FROM assignment01.bakery_sales;
-- There seem to be negative values for quantity which is impossible

SELECT *
FROM assignment01.bakery_sales
WHERE quantity <= 0;

-- There are 1295 rows where the quantity is less than or equal to 0. These might be human error or just bad data

-- Checking for the same with unit_price
SELECT AVG(unit_price) as average_unit_price, MAX(unit_price) as max_unit_price, MIN(unit_price) as min_unit_price
FROM assignment01.bakery_sales;

-- There seem to be rows with unit price of 0

SELECT *
FROM assignment01.bakery_sales
WHERE unit_price = 0;

-- There are 27 rows with unit price of 0. This could be human error as this is rather unlikely