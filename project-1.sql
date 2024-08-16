use ggotoo_db;

CREATE SCHEMA customer360;

CREATE OR ALTER VIEW customer360.customer360
AS SELECT conversion_and_first_order_data.customer_id,
          conversion_and_first_order_data.first_name,
          conversion_and_first_order_data.last_name,
          conversion_and_first_order_data.conversion_id,
          conversion_and_first_order_data.conversion_number,
          conversion_and_first_order_data.conversion_type,
          conversion_and_first_order_data.conversion_date,
          conversion_and_first_order_data.conversion_week,
          conversion_and_first_order_data.conversion_channel,
          conversion_and_first_order_data.next_conversion_week,
          conversion_and_first_order_data.first_order_number,
          conversion_and_first_order_data.first_order_date,
          conversion_and_first_order_data.first_order_week,
          conversion_and_first_order_data.first_order_product,
          conversion_and_first_order_data.first_order_unit_price,
          conversion_and_first_order_data.first_order_discount,
          conversion_and_first_order_data.first_order_total_paid,
          RANK() OVER (PARTITION BY conversion_id ORDER BY order_week ASC) as week_counter,
          order_history_data.order_week,
          order_history_data.orders_placed,
          order_history_data.total_before_discounts,
          order_history_data.total_discounts,
          order_history_data.total_paid_in_week,
          SUM(total_paid_in_week) OVER (PARTITION BY conversion_id ORDER BY order_week ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS conversion_cumulative_revenue,
          SUM(total_paid_in_week) OVER (PARTITION BY customer_id ORDER BY order_week ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS lifetime_cumulative_revenue
   FROM (SELECT cd.sk_customer,
                cd.customer_id,
                cd.first_name,
                cd.last_name,
                cs.conversion_id,
                RANK() OVER (PARTITION BY cd.customer_id ORDER BY cs.conversion_id ASC) AS conversion_number,
                cs.conversion_type,
                cs.conversion_date,
                first_conversion_date_dimension.year_week AS conversion_week,
                cs.conversion_channel,
                next_conversion_date_dimension.year_week AS next_conversion_week,
                orders.order_number AS first_order_number,
                orders.order_date AS first_order_date,
                orders_date_dimension.year_week AS first_order_week,-- explain
                pr.product_name as first_order_product,
                orders.unit_price first_order_unit_price,
                orders.discount_value AS first_order_discount,
                orders.price_paid AS first_order_total_paid
         FROM mmai_db.fact_tables.conversions AS cs
             LEFT JOIN (
             SELECT cv1.conversion_id,
                    MIN(cv2.fk_conversion_date) as fk_next_conversion_date
             FROM mmai_db.fact_tables.conversions as cv1
                 LEFT JOIN mmai_db.fact_tables.conversions as cv2
                     ON cv1.fk_customer = cv2.fk_customer
             WHERE cv1.conversion_date < cv2.conversion_date
             GROUP BY cv1.conversion_id
             ) AS next_conversion_table
                 ON cs.conversion_id = next_conversion_table.conversion_id
             LEFT JOIN mmai_db.dimensions.customer_dimension AS cd
                 ON cs.fk_customer = cd.sk_customer
             LEFT JOIN mmai_db.fact_tables.orders AS orders
                 ON cs.order_number = orders.order_number
             LEFT JOIN mmai_db.dimensions.date_dimension AS first_conversion_date_dimension
                 ON cs.fk_conversion_date = first_conversion_date_dimension.sk_date
             LEFT JOIN mmai_db.dimensions.date_dimension AS next_conversion_date_dimension
                 ON next_conversion_table.fk_next_conversion_date = next_conversion_date_dimension.sk_date
             LEFT JOIN mmai_db.dimensions.date_dimension AS orders_date_dimension
                 ON orders.fk_order_date =  orders_date_dimension.sk_date
             LEFT JOIN mmai_db.dimensions.product_dimension AS pr
                 ON cs.fk_product = pr.sk_product
         ) AS conversion_and_first_order_data
       LEFT JOIN (
       SELECT customer_by_week.sk_customer AS foreign_key_customer,
              customer_by_week.year_week AS order_week,
              ISNULL(grand_total, 0)    AS total_before_discounts,
              ISNULL(total_discount, 0) AS total_discounts,
              ISNULL(total_paid, 0)     AS total_paid_in_week,
              ISNULL(orders_placed, 0)     AS orders_placed
       FROM (SELECT customers.sk_customer,
                    weeks.year_week
             FROM (SELECT DISTINCT sk_customer
                   FROM mmai_db.dimensions.customer_dimension) AS customers
                 CROSS JOIN (SELECT DISTINCT year_week
                             FROM mmai_db.dimensions.date_dimension) AS weeks) AS customer_by_week
           LEFT JOIN (SELECT fk_customer,
                             year_week,
                             SUM(unit_price)     AS grand_total,
                             SUM(discount_value) AS total_discount,
                             SUM(price_paid)     AS total_paid,
                             COUNT(DISTINCT year_week) AS orders_placed
                      FROM mmai_db.fact_tables.orders AS orders
                          LEFT JOIN mmai_db.dimensions.date_dimension AS dd
                              ON orders.fk_order_date = dd.sk_date
                      GROUP BY orders.fk_customer, dd.year_week) AS customer_order_by_active_week
               ON customer_by_week.sk_customer = customer_order_by_active_week.fk_customer AND
                  customer_by_week.year_week = customer_order_by_active_week.year_week) AS order_history_data
           ON conversion_and_first_order_data.sk_customer = order_history_data.foreign_key_customer
   WHERE conversion_and_first_order_data.conversion_week <= order_history_data.order_week
     AND ISNULL(conversion_and_first_order_data.next_conversion_week, '2024-W01') > order_history_data.order_week;

-- Steps taken and challenges faced
-- First steps were to construct the conversion and first order data
-- This data required simple joins between the conversions table and the other dimensions and fact tables.
-- Since the conversion date is the same date as the first order date, getting the first order data was quite simple. We just had to
-- join the conversion table to the orders table on the order number, and that matched the first orders data. The challenging part about
-- creating the conversion and first order data was getting the next conversion week. This was solved by constructing a CTE of
-- the conversions table joined on itself at the customer level and filtered for only the rows where the right table's conversion
-- week was greater than the left table's conversion week. The conversions were then grouped by conversion id and the right table's conversion
-- week column was aggregated by minimum to get the next_conversion_week

-- The second part of the problem was constructing the order data.The biggest issue was that the result has to contain data by week for each
-- customer from the week they first converted till the last week available in the date dimension. However there were a lot of weeks in between
-- where customers didn't buy anything so those rows had to be constructed somehow. To do this a CTE was first constructed to make a table
-- of rows that had every combination of every customer along with every week from the start of the date dimension time to the end of it by using a
-- CROSS JOIN. That was then left joined on the aggregated orders data that was grouped by customer fkey and year_week with the necessary financial
-- columns aggregated via sum. Another column aggregated here was the orders_placed column which was just a count on distinct year_week values,
-- since that would only be 1 for weeks when an order was placed and null otherwise.
-- By left joining our cross joined customer fkey year_week combinations table to the aggregated data table we create a new table with aggregated data
-- for every single week for every single customer. To take care of rows and values where there were nulls i.e. weeks where the customer didn't buy anything
-- ISNULL() was used to replace null values by 0 (since there were no orders placed that week all values are by default 0)

-- Now that we have these two tables we can combine to get our final result. The challenge here was to only include the rows for order data in the final table
-- that corresponded to the beginning till end of each conversions time period. Then next conversion week was immensely helpful for that. The conversion and
-- first order data was left joined on the aggregate order data filtering for rows where the data week was equal to or greater than the conversion week
-- and less than the next conversion week. For rows where there was no next conversion week, that conversion would have data till the end of the date dimension
-- time period i.e. 2023-W53 hence an ISNULL() was used to default those null values to 2024-W01 to capture that functionality

-- Finally came the issue of week counter which was just a rank over partition on conversion id (that gets you a ranked week sequence for each conversion)
-- and the issue of lifetime cumulative and conversion cumulative revenue. Those columns were constructed by summing over partitions of conversion id
-- (for conversion cumulative) and customer id (for lifetime cumulative) but summing just over all previous rows and the current row
