--! qt:dataset:impala_dataset

explain cbo select  ca_zip
       ,sum(cs_sales_price)
 from impala_tpcds_catalog_sales
     ,impala_tpcds_customer
     ,impala_tpcds_customer_address
     ,impala_tpcds_date_dim
 where cs_bill_customer_sk = c_customer_sk
      and c_current_addr_sk = ca_address_sk 
      and ( substr(ca_zip,1,5) in ('85669', '86197','88274','83405','86475',
                                   '85392', '85460', '80348', '81792')
            or ca_state in ('CA','WA','GA')
            or cs_sales_price > 500)
      and cs_sold_date_sk = d_date_sk
      and d_qoy = 2 and d_year = 1998
 group by ca_zip
 order by ca_zip
 limit 100;

explain select  ca_zip
       ,sum(cs_sales_price)
 from impala_tpcds_catalog_sales
     ,impala_tpcds_customer
     ,impala_tpcds_customer_address
     ,impala_tpcds_date_dim
 where cs_bill_customer_sk = c_customer_sk
      and c_current_addr_sk = ca_address_sk 
      and ( substr(ca_zip,1,5) in ('85669', '86197','88274','83405','86475',
                                   '85392', '85460', '80348', '81792')
            or ca_state in ('CA','WA','GA')
            or cs_sales_price > 500)
      and cs_sold_date_sk = d_date_sk
      and d_qoy = 2 and d_year = 1998
 group by ca_zip
 order by ca_zip
 limit 100;

