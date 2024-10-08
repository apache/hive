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

CREATE EXTERNAL TABLE IF NOT EXISTS `store`(
  `s_store_sk` int, 
  `s_store_id` string, 
  `s_rec_start_date` string, 
  `s_rec_end_date` string, 
  `s_closed_date_sk` int, 
  `s_store_name` string, 
  `s_number_employees` int, 
  `s_floor_space` int, 
  `s_hours` string, 
  `s_manager` string, 
  `s_market_id` int, 
  `s_geography_class` string, 
  `s_market_desc` string, 
  `s_market_manager` string, 
  `s_division_id` int, 
  `s_division_name` string, 
  `s_company_id` int, 
  `s_company_name` string, 
  `s_street_number` string, 
  `s_street_name` string, 
  `s_street_type` string, 
  `s_suite_number` string, 
  `s_city` string, 
  `s_county` string, 
  `s_state` string, 
  `s_zip` string, 
  `s_country` string, 
  `s_gmt_offset` decimal(5,2), 
  `s_tax_precentage` decimal(5,2))
STORED BY                                          
'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (                                    
    "hive.sql.database.type" = "POSTGRES",
    "hive.sql.jdbc.driver" = "org.postgresql.Driver",
    "hive.sql.jdbc.url" = "jdbc:postgresql://localhost:5432/qtestDB",
    "hive.sql.dbcp.username" = "qtestuser",
    "hive.sql.dbcp.password" = "qtestpassword",
    "hive.sql.table" = "store"
);

set hive.mapred.mode=nonstrict;

explain cbo
with v1 as(
 select i_category, i_brand,
        s_store_name, s_company_name,
        d_year, d_moy,
        sum(ss_sales_price) sum_sales,
        avg(sum(ss_sales_price)) over
          (partition by i_category, i_brand,
                     s_store_name, s_company_name, d_year)
          avg_monthly_sales,
        rank() over
          (partition by i_category, i_brand,
                     s_store_name, s_company_name
           order by d_year, d_moy) rn
 from item, store_sales, date_dim, store
 where ss_item_sk = i_item_sk and
       ss_sold_date_sk = d_date_sk and
       ss_store_sk = s_store_sk and
       (
         d_year = 2000 or
         ( d_year = 2000-1 and d_moy =12) or
         ( d_year = 2000+1 and d_moy =1)
       )
 group by i_category, i_brand,
          s_store_name, s_company_name,
          d_year, d_moy),
 v2 as(
 select v1.i_category
        ,v1.d_year, v1.d_moy
        ,v1.avg_monthly_sales
        ,v1.sum_sales, v1_lag.sum_sales psum, v1_lead.sum_sales nsum
 from v1, v1 v1_lag, v1 v1_lead
 where v1.i_category = v1_lag.i_category and
       v1.i_category = v1_lead.i_category and
       v1.i_brand = v1_lag.i_brand and
       v1.i_brand = v1_lead.i_brand and
       v1.s_store_name = v1_lag.s_store_name and
       v1.s_store_name = v1_lead.s_store_name and
       v1.s_company_name = v1_lag.s_company_name and
       v1.s_company_name = v1_lead.s_company_name and
       v1.rn = v1_lag.rn + 1 and
       v1.rn = v1_lead.rn - 1)
  select  *
 from v2
 where  d_year = 2000 and    
        avg_monthly_sales > 0 and
        case when avg_monthly_sales > 0 then abs(sum_sales - avg_monthly_sales) / avg_monthly_sales else null end > 0.1
 order by sum_sales - avg_monthly_sales, 3;
