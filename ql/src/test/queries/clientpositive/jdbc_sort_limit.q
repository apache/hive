--!qt:database:postgres:q_test_tpcds_tables_schema.postgres.sql

CREATE EXTERNAL TABLE IF NOT EXISTS `date_dim`(
  `d_date_sk` int, 
  `d_date_id` string, 
  `d_date` string, 
  `d_month_seq` int, 
  `d_week_seq` int, 
  `d_quarter_seq` int, 
  `d_year` int, 
  `d_dow` int, 
  `d_moy` int, 
  `d_dom` int, 
  `d_qoy` int, 
  `d_fy_year` int, 
  `d_fy_quarter_seq` int, 
  `d_fy_week_seq` int, 
  `d_day_name` string, 
  `d_quarter_name` string, 
  `d_holiday` string, 
  `d_weekend` string, 
  `d_following_holiday` string, 
  `d_first_dom` int, 
  `d_last_dom` int, 
  `d_same_day_ly` int, 
  `d_same_day_lq` int, 
  `d_current_day` string, 
  `d_current_week` string, 
  `d_current_month` string, 
  `d_current_quarter` string, 
  `d_current_year` string)
STORED BY                                          
'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (                                    
    "hive.sql.database.type" = "POSTGRES",
    "hive.sql.jdbc.driver" = "org.postgresql.Driver",
    "hive.sql.jdbc.url" = "jdbc:postgresql://localhost:5432/qtestDB",
    "hive.sql.dbcp.username" = "qtestuser",
    "hive.sql.dbcp.password" = "qtestpassword",
    "hive.sql.table" = "date_dim"
);

CREATE EXTERNAL TABLE IF NOT EXISTS `store_sales`(
  `ss_sold_date_sk` int,
  `ss_sold_time_sk` int, 
  `ss_item_sk` bigint, 
  `ss_customer_sk` int, 
  `ss_cdemo_sk` int, 
  `ss_hdemo_sk` int, 
  `ss_addr_sk` int, 
  `ss_store_sk` int, 
  `ss_promo_sk` int, 
  `ss_ticket_number` bigint, 
  `ss_quantity` int, 
  `ss_wholesale_cost` decimal(7,2), 
  `ss_list_price` decimal(7,2), 
  `ss_sales_price` decimal(7,2), 
  `ss_ext_discount_amt` decimal(7,2), 
  `ss_ext_sales_price` decimal(7,2), 
  `ss_ext_wholesale_cost` decimal(7,2), 
  `ss_ext_list_price` decimal(7,2), 
  `ss_ext_tax` decimal(7,2), 
  `ss_coupon_amt` decimal(7,2), 
  `ss_net_paid` decimal(7,2), 
  `ss_net_paid_inc_tax` decimal(7,2), 
  `ss_net_profit` decimal(7,2))
STORED BY                                          
'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (                                    
    "hive.sql.database.type" = "POSTGRES",
    "hive.sql.jdbc.driver" = "org.postgresql.Driver",
    "hive.sql.jdbc.url" = "jdbc:postgresql://localhost:5432/qtestDB",
    "hive.sql.dbcp.username" = "qtestuser",
    "hive.sql.dbcp.password" = "qtestpassword",
    "hive.sql.table" = "store_sales"
);

CREATE EXTERNAL TABLE IF NOT EXISTS `item`(
  `i_item_sk` bigint, 
  `i_item_id` string, 
  `i_rec_start_date` string, 
  `i_rec_end_date` string, 
  `i_item_desc` string, 
  `i_current_price` decimal(7,2), 
  `i_wholesale_cost` decimal(7,2), 
  `i_brand_id` int, 
  `i_brand` string, 
  `i_class_id` int, 
  `i_class` string, 
  `i_category_id` int, 
  `i_category` string, 
  `i_manufact_id` int, 
  `i_manufact` string, 
  `i_size` string, 
  `i_formulation` string, 
  `i_color` string, 
  `i_units` string, 
  `i_container` string, 
  `i_manager_id` int, 
  `i_product_name` string)
STORED BY                                          
'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (                                    
    "hive.sql.database.type" = "POSTGRES",
    "hive.sql.jdbc.driver" = "org.postgresql.Driver",
    "hive.sql.jdbc.url" = "jdbc:postgresql://localhost:5432/qtestDB",
    "hive.sql.dbcp.username" = "qtestuser",
    "hive.sql.dbcp.password" = "qtestpassword",
    "hive.sql.table" = "item"
);

set hive.mapred.mode=nonstrict;

explain cbo
select  dt.d_year 
       ,item.i_brand_id brand_id 
       ,item.i_brand brand
       ,sum(ss_ext_sales_price) sum_agg
 from  date_dim dt 
      ,store_sales
      ,item
 where dt.d_date_sk = store_sales.ss_sold_date_sk
   and store_sales.ss_item_sk = item.i_item_sk
   and item.i_manufact_id = 436
   and dt.d_moy=12
 group by dt.d_year
      ,item.i_brand
      ,item.i_brand_id
 order by dt.d_year
         ,sum_agg desc
         ,brand_id
limit 100;
