--! qt:dataset:impala_dataset

explain cbo select 
  c_last_name,c_first_name,substr(s_city,1,30),ss_ticket_number,amt,profit
  from
   (select ss_ticket_number
          ,ss_customer_sk
          ,impala_tpcds_store.s_city
          ,sum(ss_coupon_amt) amt
          ,sum(ss_net_profit) profit
    from impala_tpcds_store_sales,impala_tpcds_date_dim,impala_tpcds_store,impala_tpcds_household_demographics
    where impala_tpcds_store_sales.ss_sold_date_sk = impala_tpcds_date_dim.d_date_sk
    and impala_tpcds_store_sales.ss_store_sk = impala_tpcds_store.s_store_sk  
    and impala_tpcds_store_sales.ss_hdemo_sk = impala_tpcds_household_demographics.hd_demo_sk
    and (impala_tpcds_household_demographics.hd_dep_count = 0 or impala_tpcds_household_demographics.hd_vehicle_count > 3)
    and impala_tpcds_date_dim.d_dow = 1
    and impala_tpcds_date_dim.d_year in (1998,1998+1,1998+2) 
    and impala_tpcds_store.s_number_employees between 200 and 295
    group by ss_ticket_number,ss_customer_sk,ss_addr_sk,impala_tpcds_store.s_city) ms,impala_tpcds_customer
    where ss_customer_sk = c_customer_sk
 order by c_last_name,c_first_name,substr(s_city,1,30), profit
limit 100;

explain select 
  c_last_name,c_first_name,substr(s_city,1,30),ss_ticket_number,amt,profit
  from
   (select ss_ticket_number
          ,ss_customer_sk
          ,impala_tpcds_store.s_city
          ,sum(ss_coupon_amt) amt
          ,sum(ss_net_profit) profit
    from impala_tpcds_store_sales,impala_tpcds_date_dim,impala_tpcds_store,impala_tpcds_household_demographics
    where impala_tpcds_store_sales.ss_sold_date_sk = impala_tpcds_date_dim.d_date_sk
    and impala_tpcds_store_sales.ss_store_sk = impala_tpcds_store.s_store_sk  
    and impala_tpcds_store_sales.ss_hdemo_sk = impala_tpcds_household_demographics.hd_demo_sk
    and (impala_tpcds_household_demographics.hd_dep_count = 0 or impala_tpcds_household_demographics.hd_vehicle_count > 3)
    and impala_tpcds_date_dim.d_dow = 1
    and impala_tpcds_date_dim.d_year in (1998,1998+1,1998+2) 
    and impala_tpcds_store.s_number_employees between 200 and 295
    group by ss_ticket_number,ss_customer_sk,ss_addr_sk,impala_tpcds_store.s_city) ms,impala_tpcds_customer
    where ss_customer_sk = c_customer_sk
 order by c_last_name,c_first_name,substr(s_city,1,30), profit
limit 100;
