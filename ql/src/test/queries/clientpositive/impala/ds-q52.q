--! qt:dataset:impala_dataset

explain cbo physical select  dt.d_year
      ,impala_tpcds_item.i_brand_id brand_id
      ,impala_tpcds_item.i_brand brand
      ,sum(ss_ext_sales_price) ext_price
 from impala_tpcds_date_dim dt
     ,impala_tpcds_store_sales
     ,impala_tpcds_item
 where dt.d_date_sk = impala_tpcds_store_sales.ss_sold_date_sk
    and impala_tpcds_store_sales.ss_item_sk = impala_tpcds_item.i_item_sk
    and impala_tpcds_item.i_manager_id = 1
    and dt.d_moy=12
    and dt.d_year=2000
 group by dt.d_year
      ,impala_tpcds_item.i_brand
      ,impala_tpcds_item.i_brand_id
 order by dt.d_year
      ,ext_price desc
      ,brand_id
limit 100 ;

explain select  dt.d_year
      ,impala_tpcds_item.i_brand_id brand_id
      ,impala_tpcds_item.i_brand brand
      ,sum(ss_ext_sales_price) ext_price
 from impala_tpcds_date_dim dt
     ,impala_tpcds_store_sales
     ,impala_tpcds_item
 where dt.d_date_sk = impala_tpcds_store_sales.ss_sold_date_sk
    and impala_tpcds_store_sales.ss_item_sk = impala_tpcds_item.i_item_sk
    and impala_tpcds_item.i_manager_id = 1
    and dt.d_moy=12
    and dt.d_year=2000
 group by dt.d_year
      ,impala_tpcds_item.i_brand
      ,impala_tpcds_item.i_brand_id
 order by dt.d_year
      ,ext_price desc
      ,brand_id
limit 100 ;
