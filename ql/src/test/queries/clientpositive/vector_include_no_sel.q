set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reducesink.new.enabled=false;
set hive.cbo.enable=false;
SET hive.auto.convert.join=true;
SET hive.auto.convert.join.noconditionaltask=true;
SET hive.auto.convert.join.noconditionaltask.size=1000000000;
SET hive.mapred.mode=nonstrict;
set hive.fetch.task.conversion=none;

-- HIVE-13872
-- Looking for TableScan immediately followed by ReduceSink (no intervening SEL operator).
-- This caused problems for Vectorizer not eliminating columns which are not included.
-- The input file format didn't fill in those vectorized columns and thus caused NPE in
-- ReduceSink.
-- Only a problem when NOT CBO because of CBO rule-based transforms.
--
-- Using a cross-product.

create table store_sales_txt
(
    ss_sold_date_sk           int,
    ss_sold_time_sk           int,
    ss_item_sk                int,
    ss_customer_sk            int,
    ss_cdemo_sk               int,
    ss_hdemo_sk               int,
    ss_addr_sk                int,
    ss_store_sk               int,
    ss_promo_sk               int,
    ss_ticket_number          int,
    ss_quantity               int,
    ss_wholesale_cost         float,
    ss_list_price             float,
    ss_sales_price            float,
    ss_ext_discount_amt       float,
    ss_ext_sales_price        float,
    ss_ext_wholesale_cost     float,
    ss_ext_list_price         float,
    ss_ext_tax                float,
    ss_coupon_amt             float,
    ss_net_paid               float,
    ss_net_paid_inc_tax       float,
    ss_net_profit             float                  
)
row format delimited fields terminated by '|' 
stored as textfile;

LOAD DATA LOCAL INPATH '../../data/files/store_sales.txt' OVERWRITE INTO TABLE store_sales_txt;

create table store_sales_n1 stored as orc as select * from store_sales_txt;


create table customer_demographics_txt
(
    cd_demo_sk                int,
    cd_gender                 string,
    cd_marital_status         string,
    cd_education_status       string,
    cd_purchase_estimate      int,
    cd_credit_rating          string,
    cd_dep_count              int,
    cd_dep_employed_count     int,
    cd_dep_college_count      int 
)
row format delimited fields terminated by '|' 
stored as textfile;

LOAD DATA LOCAL INPATH '../../data/files/customer_demographics.txt' OVERWRITE INTO TABLE customer_demographics_txt;

create table customer_demographics stored as orc as select * from customer_demographics_txt;

explain vectorization expression
select count(1) from customer_demographics,store_sales_n1
where ((customer_demographics.cd_demo_sk = store_sales_n1.ss_cdemo_sk and customer_demographics.cd_marital_status = 'M') or
       (customer_demographics.cd_demo_sk = store_sales_n1.ss_cdemo_sk and customer_demographics.cd_marital_status = 'U'));

select count(1) from customer_demographics,store_sales_n1
where ((customer_demographics.cd_demo_sk = store_sales_n1.ss_cdemo_sk and customer_demographics.cd_marital_status = 'M') or
       (customer_demographics.cd_demo_sk = store_sales_n1.ss_cdemo_sk and customer_demographics.cd_marital_status = 'U'));
