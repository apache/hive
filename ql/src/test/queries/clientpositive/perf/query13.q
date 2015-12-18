set hive.mapred.mode=nonstrict;
EXPLAIN SELECT Avg(ss_quantity) , 
       Avg(ss_ext_sales_price) , 
       Avg(ss_ext_wholesale_cost) , 
       Sum(ss_ext_wholesale_cost) 
FROM   store_sales , 
       store , 
       customer_demographics , 
       household_demographics , 
       customer_address , 
       date_dim 
WHERE  store.s_store_sk = store_sales.ss_store_sk 
AND    store_sales.ss_sold_date_sk = date_dim.d_date_sk 
AND    date_dim.d_year = 2001 
AND   (( 
                     store_sales.ss_hdemo_sk=household_demographics.hd_demo_sk 
              AND    customer_demographics.cd_demo_sk = store_sales.ss_cdemo_sk 
              AND    customer_demographics.cd_marital_status = 'M' 
              AND    customer_demographics.cd_education_status = '4 yr Degree' 
              AND    store_sales.ss_sales_price BETWEEN 100.00 AND    150.00 
              AND    household_demographics.hd_dep_count = 3 ) 
       OR     ( 
                     store_sales.ss_hdemo_sk=household_demographics.hd_demo_sk 
              AND    customer_demographics.cd_demo_sk = store_sales.ss_cdemo_sk 
              AND    customer_demographics.cd_marital_status = 'D' 
              AND    customer_demographics.cd_education_status = 'Primary' 
              AND    store_sales.ss_sales_price BETWEEN 50.00 AND    100.00 
              AND    household_demographics.hd_dep_count = 1 ) 
       OR     ( 
                     store_sales.ss_hdemo_sk=household_demographics.hd_demo_sk 
              AND    customer_demographics.cd_demo_sk = ss_cdemo_sk 
              AND    customer_demographics.cd_marital_status = 'U' 
              AND    customer_demographics.cd_education_status = 'Advanced Degree' 
              AND    store_sales.ss_sales_price BETWEEN 150.00 AND    200.00 
              AND    household_demographics.hd_dep_count = 1 )) 
AND   (( 
                     store_sales.ss_addr_sk = customer_address.ca_address_sk 
              AND    customer_address.ca_country = 'United States' 
              AND    customer_address.ca_state IN ('KY', 
                                                   'GA', 
                                                   'NM') 
              AND    store_sales.ss_net_profit BETWEEN 100 AND    200 ) 
       OR     ( 
                     store_sales.ss_addr_sk = customer_address.ca_address_sk 
              AND    customer_address.ca_country = 'United States' 
              AND    customer_address.ca_state IN ('MT', 
                                                   'OR', 
                                                   'IN') 
              AND    store_sales.ss_net_profit BETWEEN 150 AND    300 ) 
       OR     ( 
                     store_sales.ss_addr_sk = customer_address.ca_address_sk 
              AND    customer_address.ca_country = 'United States' 
              AND    customer_address.ca_state IN ('WI', 'MO', 'WV') 
              AND    store_sales.ss_net_profit BETWEEN 50 AND    250 )) ;