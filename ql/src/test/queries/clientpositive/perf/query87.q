set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.auto.convert.join=true;
set hive.fetch.task.conversion=none;
-- start query  1 in stream 0 using template query87.tpl and seed 1819994127
explain vectorization expression
select count(*) 
from ((select distinct c_last_name, c_first_name, d_date
       from store_sales, date_dim, customer
       where store_sales.ss_sold_date_sk = date_dim.d_date_sk
         and store_sales.ss_customer_sk = customer.c_customer_sk
         and d_month_seq between 1212 and 1212+11)
       except
      (select distinct c_last_name, c_first_name, d_date
       from catalog_sales, date_dim, customer
       where catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
         and catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
         and d_month_seq between 1212 and 1212+11)
       except
      (select distinct c_last_name, c_first_name, d_date
       from web_sales, date_dim, customer
       where web_sales.ws_sold_date_sk = date_dim.d_date_sk
         and web_sales.ws_bill_customer_sk = customer.c_customer_sk
         and d_month_seq between 1212 and 1212+11)
) cool_cust
;

-- end query 1 in stream 0 using template query87.tpl
