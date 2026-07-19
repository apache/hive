SET hive.vectorized.execution.enabled=true;
create table web_sales_txt
(
    ws_sold_date_sk           int,
    ws_sold_time_sk           int,
    ws_ship_date_sk           int,
    ws_item_sk                int,
    ws_bill_customer_sk       int,
    ws_bill_cdemo_sk          int,
    ws_bill_hdemo_sk          int,
    ws_bill_addr_sk           int,
    ws_ship_customer_sk       int,
    ws_ship_cdemo_sk          int,
    ws_ship_hdemo_sk          int,
    ws_ship_addr_sk           int,
    ws_web_page_sk            int,
    ws_web_site_sk            int,
    ws_ship_mode_sk           int,
    ws_warehouse_sk           int,
    ws_promo_sk               int,
    ws_order_number           int,
    ws_quantity               int,
    ws_wholesale_cost         decimal(7,2),
    ws_list_price             decimal(7,2),
    ws_sales_price            decimal(7,2),
    ws_ext_discount_amt       decimal(7,2),
    ws_ext_sales_price        decimal(7,2),
    ws_ext_wholesale_cost     decimal(7,2),
    ws_ext_list_price         decimal(7,2),
    ws_ext_tax                decimal(7,2),
    ws_coupon_amt             decimal(7,2),
    ws_ext_ship_cost          decimal(7,2),
    ws_net_paid               decimal(7,2),
    ws_net_paid_inc_tax       decimal(7,2),
    ws_net_paid_inc_ship      decimal(7,2),
    ws_net_paid_inc_ship_tax  decimal(7,2),
    ws_net_profit             decimal(7,2)
)
row format delimited fields terminated by '|'
stored as textfile;

LOAD DATA LOCAL INPATH '../../data/files/web_sales_2k' OVERWRITE INTO TABLE web_sales_txt;

-- Baseline query to verify data load.
select ws_bill_customer_sk, ws_item_sk from web_sales_txt;

-- Vectorized LAG: verify output column order follows the SELECT list
-- (ws_bill_customer_sk, ws_item_sk), not the PARTITION BY order, when window
-- functions use different PARTITION BY column orderings.
SELECT
    ws_bill_customer_sk,
    ws_item_sk,
    ws_sold_date_sk,
    ws_sales_price,
    LAG(ws_sales_price) OVER (
        PARTITION BY ws_item_sk, ws_bill_customer_sk
        ORDER BY ws_sold_date_sk
    ) AS prev_sales_price,
    ws_sales_price - LAG(ws_sales_price) OVER (
        PARTITION BY ws_bill_customer_sk, ws_item_sk
        ORDER BY ws_sold_date_sk
    ) AS sales_price_diff
FROM
    web_sales_txt;

-- Vectorized LEAD: same column-ordering check using LEAD window functions.
SELECT
    ws_bill_customer_sk,
    ws_item_sk,
    ws_sold_date_sk,
    ws_sales_price,
    LEAD(ws_sales_price) OVER (
        PARTITION BY ws_item_sk, ws_bill_customer_sk
        ORDER BY ws_sold_date_sk
    ) AS next_sales_price,
    LEAD(ws_sales_price) OVER (
        PARTITION BY ws_bill_customer_sk, ws_item_sk
        ORDER BY ws_sold_date_sk
    ) - ws_sales_price AS sales_price_diff
FROM
    web_sales_txt;

-- Vectorized FIRST_VALUE/LAST_VALUE: same column-ordering check using FIRST_VALUE and
-- LAST_VALUE window functions with differing PARTITION BY orderings.
SELECT
    ws_bill_customer_sk,
    ws_item_sk,
    ws_sold_date_sk,
    ws_sales_price,
    FIRST_VALUE(ws_sales_price) OVER (
        PARTITION BY ws_item_sk, ws_bill_customer_sk
        ORDER BY ws_sold_date_sk
    ) AS first_price,
    LAST_VALUE(ws_sales_price) OVER (
        PARTITION BY ws_bill_customer_sk, ws_item_sk
        ORDER BY ws_sold_date_sk
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS last_price
FROM
    web_sales_txt;

SET hive.vectorized.execution.enabled=false;

-- Non-vectorized LAG: reference results for validating vectorized LAG query execution above.
SELECT
    ws_bill_customer_sk,
    ws_item_sk,
    ws_sold_date_sk,
    ws_sales_price,
    LAG(ws_sales_price) OVER (
        PARTITION BY ws_item_sk, ws_bill_customer_sk
        ORDER BY ws_sold_date_sk
    ) AS prev_sales_price,
    ws_sales_price - LAG(ws_sales_price) OVER (
        PARTITION BY ws_bill_customer_sk, ws_item_sk
        ORDER BY ws_sold_date_sk
    ) AS sales_price_diff
FROM
    web_sales_txt;

-- Non-vectorized LEAD: reference results for validating vectorized LEAD query execution above.
SELECT
    ws_bill_customer_sk,
    ws_item_sk,
    ws_sold_date_sk,
    ws_sales_price,
    LEAD(ws_sales_price) OVER (
        PARTITION BY ws_item_sk, ws_bill_customer_sk
        ORDER BY ws_sold_date_sk
    ) AS next_sales_price,
    LEAD(ws_sales_price) OVER (
        PARTITION BY ws_bill_customer_sk, ws_item_sk
        ORDER BY ws_sold_date_sk
    ) - ws_sales_price AS sales_price_diff
FROM
    web_sales_txt;

-- Non-vectorized FIRST_VALUE/LAST_VALUE: reference results for validating vectorized
-- FIRST_VALUE/LAST_VALUE query execution above.
SELECT
    ws_bill_customer_sk,
    ws_item_sk,
    ws_sold_date_sk,
    ws_sales_price,
    FIRST_VALUE(ws_sales_price) OVER (
        PARTITION BY ws_item_sk, ws_bill_customer_sk
        ORDER BY ws_sold_date_sk
    ) AS first_price,
    LAST_VALUE(ws_sales_price) OVER (
        PARTITION BY ws_bill_customer_sk, ws_item_sk
        ORDER BY ws_sold_date_sk
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS last_price
FROM
    web_sales_txt;
