--! qt:dataset:impala_dataset

explain cbo select  count(*) from (
    select distinct c_last_name, c_first_name, d_date
    from impala_tpcds_store_sales, impala_tpcds_date_dim, impala_tpcds_customer
          where impala_tpcds_store_sales.ss_sold_date_sk = impala_tpcds_date_dim.d_date_sk
      and impala_tpcds_store_sales.ss_customer_sk = impala_tpcds_customer.c_customer_sk
      and d_month_seq between 1186 and 1186 + 11
  intersect
    select distinct c_last_name, c_first_name, d_date
    from impala_tpcds_catalog_sales, impala_tpcds_date_dim, impala_tpcds_customer
          where impala_tpcds_catalog_sales.cs_sold_date_sk = impala_tpcds_date_dim.d_date_sk
      and impala_tpcds_catalog_sales.cs_bill_customer_sk = impala_tpcds_customer.c_customer_sk
      and d_month_seq between 1186 and 1186 + 11
  intersect
    select distinct c_last_name, c_first_name, d_date
    from impala_tpcds_web_sales, impala_tpcds_date_dim, impala_tpcds_customer
          where impala_tpcds_web_sales.ws_sold_date_sk = impala_tpcds_date_dim.d_date_sk
      and impala_tpcds_web_sales.ws_bill_customer_sk = impala_tpcds_customer.c_customer_sk
      and d_month_seq between 1186 and 1186 + 11
) hot_cust
limit 100;

explain select  count(*) from (
    select distinct c_last_name, c_first_name, d_date
    from impala_tpcds_store_sales, impala_tpcds_date_dim, impala_tpcds_customer
          where impala_tpcds_store_sales.ss_sold_date_sk = impala_tpcds_date_dim.d_date_sk
      and impala_tpcds_store_sales.ss_customer_sk = impala_tpcds_customer.c_customer_sk
      and d_month_seq between 1186 and 1186 + 11
  intersect
    select distinct c_last_name, c_first_name, d_date
    from impala_tpcds_catalog_sales, impala_tpcds_date_dim, impala_tpcds_customer
          where impala_tpcds_catalog_sales.cs_sold_date_sk = impala_tpcds_date_dim.d_date_sk
      and impala_tpcds_catalog_sales.cs_bill_customer_sk = impala_tpcds_customer.c_customer_sk
      and d_month_seq between 1186 and 1186 + 11
  intersect
    select distinct c_last_name, c_first_name, d_date
    from impala_tpcds_web_sales, impala_tpcds_date_dim, impala_tpcds_customer
          where impala_tpcds_web_sales.ws_sold_date_sk = impala_tpcds_date_dim.d_date_sk
      and impala_tpcds_web_sales.ws_bill_customer_sk = impala_tpcds_customer.c_customer_sk
      and d_month_seq between 1186 and 1186 + 11
) hot_cust
limit 100;
