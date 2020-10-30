--! qt:dataset:impala_dataset

explain cbo physical select  c_last_name
       ,c_first_name
       ,ca_city
       ,bought_city
       ,ss_ticket_number
       ,amt,profit 
 from
   (select ss_ticket_number
          ,ss_customer_sk
          ,ca_city bought_city
          ,sum(ss_coupon_amt) amt
          ,sum(ss_net_profit) profit
    from impala_tpcds_store_sales,impala_tpcds_date_dim,impala_tpcds_store,impala_tpcds_household_demographics,impala_tpcds_customer_address 
    where impala_tpcds_store_sales.ss_sold_date_sk = impala_tpcds_date_dim.d_date_sk
    and impala_tpcds_store_sales.ss_store_sk = impala_tpcds_store.s_store_sk  
    and impala_tpcds_store_sales.ss_hdemo_sk = impala_tpcds_household_demographics.hd_demo_sk
    and impala_tpcds_store_sales.ss_addr_sk = impala_tpcds_customer_address.ca_address_sk
    and (impala_tpcds_household_demographics.hd_dep_count = 0 or
         impala_tpcds_household_demographics.hd_vehicle_count= 1)
    and impala_tpcds_date_dim.d_dow in (6,0)
    and impala_tpcds_date_dim.d_year in (2000,2000+1,2000+2) 
    and impala_tpcds_store.s_city in ('Five Forks','Oakland','Fairview','Winchester','Farmington') 
    group by ss_ticket_number,ss_customer_sk,ss_addr_sk,ca_city) dn,impala_tpcds_customer,impala_tpcds_customer_address current_addr
    where ss_customer_sk = c_customer_sk
      and impala_tpcds_customer.c_current_addr_sk = current_addr.ca_address_sk
      and current_addr.ca_city <> bought_city
  order by c_last_name
          ,c_first_name
          ,ca_city
          ,bought_city
          ,ss_ticket_number
  limit 100;

explain select  c_last_name
       ,c_first_name
       ,ca_city
       ,bought_city
       ,ss_ticket_number
       ,amt,profit 
 from
   (select ss_ticket_number
          ,ss_customer_sk
          ,ca_city bought_city
          ,sum(ss_coupon_amt) amt
          ,sum(ss_net_profit) profit
    from impala_tpcds_store_sales,impala_tpcds_date_dim,impala_tpcds_store,impala_tpcds_household_demographics,impala_tpcds_customer_address 
    where impala_tpcds_store_sales.ss_sold_date_sk = impala_tpcds_date_dim.d_date_sk
    and impala_tpcds_store_sales.ss_store_sk = impala_tpcds_store.s_store_sk  
    and impala_tpcds_store_sales.ss_hdemo_sk = impala_tpcds_household_demographics.hd_demo_sk
    and impala_tpcds_store_sales.ss_addr_sk = impala_tpcds_customer_address.ca_address_sk
    and (impala_tpcds_household_demographics.hd_dep_count = 0 or
         impala_tpcds_household_demographics.hd_vehicle_count= 1)
    and impala_tpcds_date_dim.d_dow in (6,0)
    and impala_tpcds_date_dim.d_year in (2000,2000+1,2000+2) 
    and impala_tpcds_store.s_city in ('Five Forks','Oakland','Fairview','Winchester','Farmington') 
    group by ss_ticket_number,ss_customer_sk,ss_addr_sk,ca_city) dn,impala_tpcds_customer,impala_tpcds_customer_address current_addr
    where ss_customer_sk = c_customer_sk
      and impala_tpcds_customer.c_current_addr_sk = current_addr.ca_address_sk
      and current_addr.ca_city <> bought_city
  order by c_last_name
          ,c_first_name
          ,ca_city
          ,bought_city
          ,ss_ticket_number
  limit 100;
