--! qt:dataset:impala_dataset

explain cbo select  count(*) 
from impala_tpcds_store_sales
    ,impala_tpcds_household_demographics 
    ,impala_tpcds_time_dim, impala_tpcds_store
where ss_sold_time_sk = impala_tpcds_time_dim.t_time_sk   
    and ss_hdemo_sk = impala_tpcds_household_demographics.hd_demo_sk 
    and ss_store_sk = s_store_sk
    and impala_tpcds_time_dim.t_hour = 16
    and impala_tpcds_time_dim.t_minute >= 30
    and impala_tpcds_household_demographics.hd_dep_count = 6
    and impala_tpcds_store.s_store_name = 'ese'
order by count(*)
limit 100;

explain select  count(*) 
from impala_tpcds_store_sales
    ,impala_tpcds_household_demographics 
    ,impala_tpcds_time_dim, impala_tpcds_store
where ss_sold_time_sk = impala_tpcds_time_dim.t_time_sk   
    and ss_hdemo_sk = impala_tpcds_household_demographics.hd_demo_sk 
    and ss_store_sk = s_store_sk
    and impala_tpcds_time_dim.t_hour = 16
    and impala_tpcds_time_dim.t_minute >= 30
    and impala_tpcds_household_demographics.hd_dep_count = 6
    and impala_tpcds_store.s_store_name = 'ese'
order by count(*)
limit 100;
