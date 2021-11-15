--! qt:disabled:HIVE-25379
--! qt:dataset:src

CREATE TEMPORARY FUNCTION dboutput AS 'org.apache.hadoop.hive.contrib.genericudf.example.GenericUDFDBOutput';

FROM src
SELECT
dboutput ('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_perf;create=true','user1','passwd1',
'CREATE TABLE CATALOG_SALES ("cs_sold_date_sk" INTEGER, "cs_sold_time_sk" INTEGER, "cs_ship_date_sk" INTEGER, "cs_bill_customer_sk" INTEGER,
  "cs_bill_cdemo_sk" INTEGER, "cs_bill_hdemo_sk" INTEGER, "cs_bill_addr_sk" INTEGER, "cs_ship_customer_sk" INTEGER, "cs_ship_cdemo_sk" INTEGER,
  "cs_ship_hdemo_sk" INTEGER, "cs_ship_addr_sk" INTEGER, "cs_call_center_sk" INTEGER, "cs_catalog_page_sk" INTEGER, "cs_ship_mode_sk" INTEGER,
  "cs_warehouse_sk" INTEGER, "cs_item_sk" INTEGER, "cs_promo_sk" INTEGER, "cs_order_number" INTEGER, "cs_quantity" INTEGER, "cs_wholesale_cost" DECIMAL(7,2),
  "cs_list_price" DECIMAL(7,2), "cs_sales_price" DECIMAL(7,2), "cs_ext_discount_amt" DECIMAL(7,2), "cs_ext_sales_price" DECIMAL(7,2),
  "cs_ext_wholesale_cost" DECIMAL(7,2), "cs_ext_list_price" DECIMAL(7,2), "cs_ext_tax" DECIMAL(7,2), "cs_coupon_amt" DECIMAL(7,2),
  "cs_ext_ship_cost" DECIMAL(7,2), "cs_net_paid" DECIMAL(7,2), "cs_net_paid_inc_tax" DECIMAL(7,2), "cs_net_paid_inc_ship" DECIMAL(7,2),
  "cs_net_paid_inc_ship_tax" DECIMAL(7,2), "cs_net_profit" DECIMAL(7,2))' ),
dboutput ('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_perf;create=true','user1','passwd1',
'CREATE TABLE CATALOG_RETURNS ("cr_returned_date_sk" INTEGER, "cr_returned_time_sk" INTEGER, "cr_item_sk" INTEGER, "cr_refunded_customer_sk" INTEGER,
  "cr_refunded_cdemo_sk" INTEGER, "cr_refunded_hdemo_sk" INTEGER, "cr_refunded_addr_sk" INTEGER, "cr_returning_customer_sk" INTEGER,
  "cr_returning_cdemo_sk" INTEGER, "cr_returning_hdemo_sk" INTEGER, "cr_returning_addr_sk" INTEGER, "cr_call_center_sk" INTEGER,
  "cr_catalog_page_sk" INTEGER, "cr_ship_mode_sk" INTEGER, "cr_warehouse_sk" INTEGER, "cr_reason_sk" INTEGER, "cr_order_number" INTEGER,
  "cr_return_quantity" INTEGER, "cr_return_amount" DECIMAL(7,2), "cr_return_tax" DECIMAL(7,2), "cr_return_amt_inc_tax" DECIMAL(7,2),
  "cr_fee" DECIMAL(7,2), "cr_return_ship_cost" DECIMAL(7,2), "cr_refunded_cash" DECIMAL(7,2), "cr_reversed_charge" DECIMAL(7,2),
  "cr_store_credit" DECIMAL(7,2), "cr_net_loss" DECIMAL(7,2))' ),
dboutput ('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_perf;create=true','user1','passwd1',
'CREATE TABLE STORE_SALES ("ss_sold_date_sk" INTEGER, "ss_sold_time_sk" INTEGER, "ss_item_sk" INTEGER, "ss_customer_sk" INTEGER, "ss_cdemo_sk" INTEGER,
  "ss_hdemo_sk" INTEGER, "ss_addr_sk" INTEGER, "ss_store_sk" INTEGER, "ss_promo_sk" INTEGER, "ss_ticket_number" INTEGER, "ss_quantity" INTEGER,
  "ss_wholesale_cost" DECIMAL(7,2), "ss_list_price" DECIMAL(7,2), "ss_sales_price" DECIMAL(7,2), "ss_ext_discount_amt" DECIMAL(7,2),
  "ss_ext_sales_price" DECIMAL(7,2), "ss_ext_wholesale_cost" DECIMAL(7,2), "ss_ext_list_price" DECIMAL(7,2), "ss_ext_tax" DECIMAL(7,2),
  "ss_coupon_amt" DECIMAL(7,2), "ss_net_paid" DECIMAL(7,2), "ss_net_paid_inc_tax" DECIMAL(7,2), "ss_net_profit" DECIMAL(7,2))' ),
dboutput ('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_perf;create=true','user1','passwd1',
'CREATE TABLE STORE_RETURNS ("sr_returned_date_sk" INTEGER, "sr_return_time_sk" INTEGER, "sr_item_sk" INTEGER, "sr_customer_sk" INTEGER,
  "sr_cdemo_sk" INTEGER, "sr_hdemo_sk" INTEGER, "sr_addr_sk" INTEGER, "sr_store_sk" INTEGER, "sr_reason_sk" INTEGER, "sr_ticket_number" INTEGER,
  "sr_return_quantity" INTEGER, "sr_return_amt" DECIMAL(7,2), "sr_return_tax" DECIMAL(7,2), "sr_return_amt_inc_tax" DECIMAL(7,2),
  "sr_fee" DECIMAL(7,2), "sr_return_ship_cost" DECIMAL(7,2), "sr_refunded_cash" DECIMAL(7,2), "sr_reversed_charge" DECIMAL(7,2),
  "sr_store_credit" DECIMAL(7,2), "sr_net_loss" DECIMAL(7,2))' ),
dboutput ('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_perf;create=true','user1','passwd1',
'CREATE TABLE WEB_SALES ("ws_sold_date_sk" INTEGER, "ws_sold_time_sk" INTEGER, "ws_ship_date_sk" INTEGER, "ws_item_sk" INTEGER,
  "ws_bill_customer_sk" INTEGER, "ws_bill_cdemo_sk" INTEGER, "ws_bill_hdemo_sk" INTEGER, "ws_bill_addr_sk" INTEGER, "ws_ship_customer_sk" INTEGER,
  "ws_ship_cdemo_sk" INTEGER, "ws_ship_hdemo_sk" INTEGER, "ws_ship_addr_sk" INTEGER, "ws_web_page_sk" INTEGER, "ws_web_site_sk" INTEGER,
  "ws_ship_mode_sk" INTEGER, "ws_warehouse_sk" INTEGER, "ws_promo_sk" INTEGER, "ws_order_number" INTEGER, "ws_quantity" INTEGER,
  "ws_wholesale_cost" DECIMAL(7,2), "ws_list_price" DECIMAL(7,2), "ws_sales_price" DECIMAL(7,2), "ws_ext_discount_amt" DECIMAL(7,2),
  "ws_ext_sales_price" DECIMAL(7,2), "ws_ext_wholesale_cost" DECIMAL(7,2), "ws_ext_list_price" DECIMAL(7,2), "ws_ext_tax" DECIMAL(7,2),
  "ws_coupon_amt" DECIMAL(7,2), "ws_ext_ship_cost" DECIMAL(7,2), "ws_net_paid" DECIMAL(7,2), "ws_net_paid_inc_tax" DECIMAL(7,2),
  "ws_net_paid_inc_ship" DECIMAL(7,2), "ws_net_paid_inc_ship_tax" DECIMAL(7,2), "ws_net_profit" DECIMAL(7,2))' ),
dboutput ('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_perf;create=true','user1','passwd1',
'CREATE TABLE WEB_RETURNS ("wr_returned_date_sk" INTEGER, "wr_returned_time_sk" INTEGER, "wr_item_sk" INTEGER, "wr_refunded_customer_sk" INTEGER,
  "wr_refunded_cdemo_sk" INTEGER, "wr_refunded_hdemo_sk" INTEGER, "wr_refunded_addr_sk" INTEGER, "wr_returning_customer_sk" INTEGER,
  "wr_returning_cdemo_sk" INTEGER, "wr_returning_hdemo_sk" INTEGER, "wr_returning_addr_sk" INTEGER, "wr_web_page_sk" INTEGER,
  "wr_reason_sk" INTEGER, "wr_order_number" INTEGER, "wr_return_quantity" INTEGER, "wr_return_amt" DECIMAL(7,2), "wr_return_tax" DECIMAL(7,2),
  "wr_return_amt_inc_tax" DECIMAL(7,2), "wr_fee" DECIMAL(7,2), "wr_return_ship_cost" DECIMAL(7,2), "wr_refunded_cash" DECIMAL(7,2),
  "wr_reversed_charge" DECIMAL(7,2), "wr_account_credit" DECIMAL(7,2), "wr_net_loss" DECIMAL(7,2))' ),
dboutput ('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_perf;create=true','user1','passwd1',
'CREATE TABLE CUSTOMER ("c_customer_sk" INTEGER, "c_customer_id" CHAR(16), "c_current_cdemo_sk" INTEGER, "c_current_hdemo_sk" INTEGER,
  "c_current_addr_sk" INTEGER, "c_first_shipto_date_sk" INTEGER, "c_first_sales_date_sk" INTEGER, "c_salutation" CHAR(10),
  "c_first_name" CHAR(20), "c_last_name" CHAR(30), "c_preferred_cust_flag" CHAR(1), "c_birth_day" INTEGER, "c_birth_month" INTEGER,
  "c_birth_year" INTEGER, "c_birth_country" VARCHAR(20), "c_login" CHAR(13), "c_email_address" CHAR(50), "c_last_review_date" CHAR(10))' ),
dboutput ('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_perf;create=true','user1','passwd1',
'CREATE TABLE CUSTOMER_ADDRESS ("ca_address_sk" INTEGER, "ca_address_id" CHAR(16), "ca_street_number" CHAR(10), "ca_street_name" VARCHAR(60),
  "ca_street_type" CHAR(15), "ca_suite_number" CHAR(10), "ca_city" VARCHAR(60), "ca_county" VARCHAR(30), "ca_state" CHAR(2),
  "ca_zip" CHAR(10), "ca_country" VARCHAR(20), "ca_gmt_offset" DECIMAL(5,2), "ca_location_type" CHAR(20))' ),
dboutput ('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_perf;create=true','user1','passwd1',
'CREATE TABLE CUSTOMER_DEMOGRAPHICS ("cd_demo_sk" INTEGER, "cd_gender" CHAR(1), "cd_marital_status" CHAR(1), "cd_education_status" CHAR(20),
  "cd_purchase_estimate" INTEGER, "cd_credit_rating" CHAR(10), "cd_dep_count" INTEGER, "cd_dep_employed_count" INTEGER,
  "cd_dep_college_count" INTEGER)' ),
dboutput ('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_perf;create=true','user1','passwd1',
'CREATE TABLE INVENTORY ("inv_date_sk" INTEGER, "inv_item_sk" INTEGER, "inv_warehouse_sk" INTEGER, "inv_quantity_on_hand" INTEGER)' ),
dboutput ('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_perf;create=true','user1','passwd1',
'CREATE TABLE ITEM ("i_item_sk" INTEGER, "i_item_id" CHAR(16), "i_rec_start_date" DATE, "i_rec_end_date" DATE,
  "i_item_desc" VARCHAR(200), "i_current_price" DECIMAL(7,2), "i_wholesale_cost" DECIMAL(7,2), "i_brand_id" INTEGER,
  "i_brand" CHAR(50), "i_class_id" INTEGER, "i_class" CHAR(50), "i_category_id" INTEGER, "i_category" CHAR(50),
  "i_manufact_id" INTEGER, "i_manufact" CHAR(50), "i_size" CHAR(20), "i_formulation" CHAR(20), "i_color" CHAR(20),
  "i_units" CHAR(10), "i_container" CHAR(10), "i_manager_id" INTEGER, "i_product_name" CHAR(50))' ),
dboutput ('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_perf;create=true','user1','passwd1',
'CREATE TABLE WAREHOUSE ("w_warehouse_sk" INTEGER, "w_warehouse_id" CHAR(16), "w_warehouse_name" VARCHAR(20),
  "w_warehouse_sq_ft" INTEGER, "w_street_number" CHAR(10), "w_street_name" VARCHAR(60), "w_street_type" CHAR(15),
  "w_suite_number" CHAR(10), "w_city" VARCHAR(60), "w_county" VARCHAR(30), "w_state" CHAR(2), "w_zip" CHAR(10),
  "w_country" VARCHAR(20), "w_gmt_offset" DECIMAL(5,2))' ),
dboutput ('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_perf;create=true','user1','passwd1',
'CREATE TABLE DATE_DIM ("d_date_sk" INTEGER, "d_date_id" CHAR(16), "d_date" DATE, "d_month_seq" INTEGER, "d_week_seq" INTEGER,
  "d_quarter_seq" INTEGER, "d_year" INTEGER, "d_dow" INTEGER, "d_moy" INTEGER, "d_dom" INTEGER, "d_qoy" INTEGER,
  "d_fy_year" INTEGER, "d_fy_quarter_seq" INTEGER, "d_fy_week_seq" INTEGER, "d_day_name" CHAR(9), "d_quarter_name" CHAR(6),
  "d_holiday" CHAR(1), "d_weekend" CHAR(1), "d_following_holiday" CHAR(1), "d_first_dom" INTEGER, "d_last_dom" INTEGER,
  "d_same_day_ly" INTEGER, "d_same_day_lq" INTEGER, "d_current_day" CHAR(1), "d_current_week" CHAR(1), "d_current_month" CHAR(1),
  "d_current_quarter" CHAR(1), "d_current_year" CHAR(1))' ),
dboutput ('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_perf;create=true','user1','passwd1',
'CREATE TABLE STORE ("s_store_sk" INTEGER, "s_store_id" CHAR(16), "s_rec_start_date" DATE, "s_rec_end_date" DATE,
  "s_closed_date_sk" INTEGER, "s_store_name" VARCHAR(50), "s_number_employees" INTEGER, "s_floor_space" INTEGER,
  "s_hours" CHAR(20), "s_manager" VARCHAR(40), "s_market_id" INTEGER, "s_geography_class" VARCHAR(100),
  "s_market_desc" VARCHAR(100), "s_market_manager" VARCHAR(40), "s_division_id" INTEGER, "s_division_name" VARCHAR(50),
  "s_company_id" INTEGER, "s_company_name" VARCHAR(50), "s_street_number" VARCHAR(10), "s_street_name" VARCHAR(60),
  "s_street_type" CHAR(15), "s_suite_number" CHAR(10), "s_city" VARCHAR(60), "s_county" VARCHAR(30),
  "s_state" CHAR(2), "s_zip" CHAR(10), "s_country" VARCHAR(20), "s_gmt_offset" DECIMAL(5,2), "s_tax_precentage" DECIMAL(5,2))' ),
dboutput ('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_perf;create=true','user1','passwd1',
'CREATE TABLE HOUSEHOLD_DEMOGRAPHICS ("hd_demo_sk" INTEGER, "hd_income_band_sk" INTEGER, "hd_buy_potential" CHAR(15),
  "hd_dep_count" INTEGER, "hd_vehicle_count" INTEGER)' ),
dboutput ('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_perf;create=true','user1','passwd1',
'CREATE TABLE TIME_DIM ("t_time_sk" INTEGER, "t_time_id" CHAR(16), "t_time" INTEGER, "t_hour" INTEGER,
  "t_minute" INTEGER, "t_second" INTEGER, "t_am_pm" CHAR(2), "t_shift" CHAR(20), "t_sub_shift" CHAR(20),
  "t_meal_time" CHAR(20))' ),
dboutput ('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_perf;create=true','user1','passwd1',
'CREATE TABLE REASON ("r_reason_sk" INTEGER, "r_reason_id" CHAR(16), "r_reason_desc" CHAR(100))' )
limit 1;


CREATE EXTERNAL TABLE catalog_sales
(
    cs_sold_date_sk           int                           ,
    cs_sold_time_sk           int                           ,
    cs_ship_date_sk           int                           ,
    cs_bill_customer_sk       int                           ,
    cs_bill_cdemo_sk          int                           ,
    cs_bill_hdemo_sk          int                           ,
    cs_bill_addr_sk           int                           ,
    cs_ship_customer_sk       int                           ,
    cs_ship_cdemo_sk          int                           ,
    cs_ship_hdemo_sk          int                           ,
    cs_ship_addr_sk           int                           ,
    cs_call_center_sk         int                           ,
    cs_catalog_page_sk        int                           ,
    cs_ship_mode_sk           int                           ,
    cs_warehouse_sk           int                           ,
    cs_item_sk                int                           ,
    cs_promo_sk               int                           ,
    cs_order_number           int                           ,
    cs_quantity               int                           ,
    cs_wholesale_cost         decimal(7,2)                  ,
    cs_list_price             decimal(7,2)                  ,
    cs_sales_price            decimal(7,2)                  ,
    cs_ext_discount_amt       decimal(7,2)                  ,
    cs_ext_sales_price        decimal(7,2)                  ,
    cs_ext_wholesale_cost     decimal(7,2)                  ,
    cs_ext_list_price         decimal(7,2)                  ,
    cs_ext_tax                decimal(7,2)                  ,
    cs_coupon_amt             decimal(7,2)                  ,
    cs_ext_ship_cost          decimal(7,2)                  ,
    cs_net_paid               decimal(7,2)                  ,
    cs_net_paid_inc_tax       decimal(7,2)                  ,
    cs_net_paid_inc_ship      decimal(7,2)                  ,
    cs_net_paid_inc_ship_tax  decimal(7,2)                  ,
    cs_net_profit             decimal(7,2)
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
                "hive.sql.database.type" = "DERBY",
                "hive.sql.jdbc.driver" = "org.apache.derby.jdbc.EmbeddedDriver",
                "hive.sql.jdbc.url" = "jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_perf;collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "user1",
                "hive.sql.dbcp.password" = "passwd1",
                "hive.sql.table" = "CATALOG_SALES",
                "hive.sql.dbcp.maxActive" = "1"
);

CREATE EXTERNAL TABLE catalog_returns
(
    cr_returned_date_sk       int                           ,
    cr_returned_time_sk       int                           ,
    cr_item_sk                int                           ,
    cr_refunded_customer_sk   int                           ,
    cr_refunded_cdemo_sk      int                           ,
    cr_refunded_hdemo_sk      int                           ,
    cr_refunded_addr_sk       int                           ,
    cr_returning_customer_sk  int                           ,
    cr_returning_cdemo_sk     int                           ,
    cr_returning_hdemo_sk     int                           ,
    cr_returning_addr_sk      int                           ,
    cr_call_center_sk         int                           ,
    cr_catalog_page_sk        int                           ,
    cr_ship_mode_sk           int                           ,
    cr_warehouse_sk           int                           ,
    cr_reason_sk              int                           ,
    cr_order_number           int                           ,
    cr_return_quantity        int                           ,
    cr_return_amount          decimal(7,2)                  ,
    cr_return_tax             decimal(7,2)                  ,
    cr_return_amt_inc_tax     decimal(7,2)                  ,
    cr_fee                    decimal(7,2)                  ,
    cr_return_ship_cost       decimal(7,2)                  ,
    cr_refunded_cash          decimal(7,2)                  ,
    cr_reversed_charge        decimal(7,2)                  ,
    cr_store_credit           decimal(7,2)                  ,
    cr_net_loss               decimal(7,2)
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
                "hive.sql.database.type" = "DERBY",
                "hive.sql.jdbc.driver" = "org.apache.derby.jdbc.EmbeddedDriver",
                "hive.sql.jdbc.url" = "jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_perf;collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "user1",
                "hive.sql.dbcp.password" = "passwd1",
                "hive.sql.table" = "CATALOG_RETURNS",
                "hive.sql.dbcp.maxActive" = "1"
);

CREATE EXTERNAL TABLE store_sales
(
    ss_sold_date_sk           int                           ,
    ss_sold_time_sk           int                           ,
    ss_item_sk                int                           ,
    ss_customer_sk            int                           ,
    ss_cdemo_sk               int                           ,
    ss_hdemo_sk               int                           ,
    ss_addr_sk                int                           ,
    ss_store_sk               int                           ,
    ss_promo_sk               int                           ,
    ss_ticket_number          int                           ,
    ss_quantity               int                           ,
    ss_wholesale_cost         decimal(7,2)                  ,
    ss_list_price             decimal(7,2)                  ,
    ss_sales_price            decimal(7,2)                  ,
    ss_ext_discount_amt       decimal(7,2)                  ,
    ss_ext_sales_price        decimal(7,2)                  ,
    ss_ext_wholesale_cost     decimal(7,2)                  ,
    ss_ext_list_price         decimal(7,2)                  ,
    ss_ext_tax                decimal(7,2)                  ,
    ss_coupon_amt             decimal(7,2)                  ,
    ss_net_paid               decimal(7,2)                  ,
    ss_net_paid_inc_tax       decimal(7,2)                  ,
    ss_net_profit             decimal(7,2)
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
                "hive.sql.database.type" = "DERBY",
                "hive.sql.jdbc.driver" = "org.apache.derby.jdbc.EmbeddedDriver",
                "hive.sql.jdbc.url" = "jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_perf;collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "user1",
                "hive.sql.dbcp.password" = "passwd1",
                "hive.sql.table" = "STORE_SALES",
                "hive.sql.dbcp.maxActive" = "1"
);

CREATE EXTERNAL TABLE store_returns
(
    sr_returned_date_sk       int                           ,
    sr_return_time_sk         int                           ,
    sr_item_sk                int                           ,
    sr_customer_sk            int                           ,
    sr_cdemo_sk               int                           ,
    sr_hdemo_sk               int                           ,
    sr_addr_sk                int                           ,
    sr_store_sk               int                           ,
    sr_reason_sk              int                           ,
    sr_ticket_number          int                           ,
    sr_return_quantity        int                           ,
    sr_return_amt             decimal(7,2)                  ,
    sr_return_tax             decimal(7,2)                  ,
    sr_return_amt_inc_tax     decimal(7,2)                  ,
    sr_fee                    decimal(7,2)                  ,
    sr_return_ship_cost       decimal(7,2)                  ,
    sr_refunded_cash          decimal(7,2)                  ,
    sr_reversed_charge        decimal(7,2)                  ,
    sr_store_credit           decimal(7,2)                  ,
    sr_net_loss               decimal(7,2)
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
                "hive.sql.database.type" = "DERBY",
                "hive.sql.jdbc.driver" = "org.apache.derby.jdbc.EmbeddedDriver",
                "hive.sql.jdbc.url" = "jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_perf;collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "user1",
                "hive.sql.dbcp.password" = "passwd1",
                "hive.sql.table" = "STORE_RETURNS",
                "hive.sql.dbcp.maxActive" = "1"
);

CREATE EXTERNAL TABLE web_sales
(
    ws_sold_date_sk           int                           ,
    ws_sold_time_sk           int                           ,
    ws_ship_date_sk           int                           ,
    ws_item_sk                int                           ,
    ws_bill_customer_sk       int                           ,
    ws_bill_cdemo_sk          int                           ,
    ws_bill_hdemo_sk          int                           ,
    ws_bill_addr_sk           int                           ,
    ws_ship_customer_sk       int                           ,
    ws_ship_cdemo_sk          int                           ,
    ws_ship_hdemo_sk          int                           ,
    ws_ship_addr_sk           int                           ,
    ws_web_page_sk            int                           ,
    ws_web_site_sk            int                           ,
    ws_ship_mode_sk           int                           ,
    ws_warehouse_sk           int                           ,
    ws_promo_sk               int                           ,
    ws_order_number           int                           ,
    ws_quantity               int                           ,
    ws_wholesale_cost         decimal(7,2)                  ,
    ws_list_price             decimal(7,2)                  ,
    ws_sales_price            decimal(7,2)                  ,
    ws_ext_discount_amt       decimal(7,2)                  ,
    ws_ext_sales_price        decimal(7,2)                  ,
    ws_ext_wholesale_cost     decimal(7,2)                  ,
    ws_ext_list_price         decimal(7,2)                  ,
    ws_ext_tax                decimal(7,2)                  ,
    ws_coupon_amt             decimal(7,2)                  ,
    ws_ext_ship_cost          decimal(7,2)                  ,
    ws_net_paid               decimal(7,2)                  ,
    ws_net_paid_inc_tax       decimal(7,2)                  ,
    ws_net_paid_inc_ship      decimal(7,2)                  ,
    ws_net_paid_inc_ship_tax  decimal(7,2)                  ,
    ws_net_profit             decimal(7,2)
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
                "hive.sql.database.type" = "DERBY",
                "hive.sql.jdbc.driver" = "org.apache.derby.jdbc.EmbeddedDriver",
                "hive.sql.jdbc.url" = "jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_perf;collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "user1",
                "hive.sql.dbcp.password" = "passwd1",
                "hive.sql.table" = "WEB_SALES",
                "hive.sql.dbcp.maxActive" = "1"
);

CREATE EXTERNAL TABLE web_returns
(
    wr_returned_date_sk       int                           ,
    wr_returned_time_sk       int                           ,
    wr_item_sk                int                           ,
    wr_refunded_customer_sk   int                           ,
    wr_refunded_cdemo_sk      int                           ,
    wr_refunded_hdemo_sk      int                           ,
    wr_refunded_addr_sk       int                           ,
    wr_returning_customer_sk  int                           ,
    wr_returning_cdemo_sk     int                           ,
    wr_returning_hdemo_sk     int                           ,
    wr_returning_addr_sk      int                           ,
    wr_web_page_sk            int                           ,
    wr_reason_sk              int                           ,
    wr_order_number           int                           ,
    wr_return_quantity        int                           ,
    wr_return_amt             decimal(7,2)                  ,
    wr_return_tax             decimal(7,2)                  ,
    wr_return_amt_inc_tax     decimal(7,2)                  ,
    wr_fee                    decimal(7,2)                  ,
    wr_return_ship_cost       decimal(7,2)                  ,
    wr_refunded_cash          decimal(7,2)                  ,
    wr_reversed_charge        decimal(7,2)                  ,
    wr_account_credit         decimal(7,2)                  ,
    wr_net_loss               decimal(7,2)
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
                "hive.sql.database.type" = "DERBY",
                "hive.sql.jdbc.driver" = "org.apache.derby.jdbc.EmbeddedDriver",
                "hive.sql.jdbc.url" = "jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_perf;collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "user1",
                "hive.sql.dbcp.password" = "passwd1",
                "hive.sql.table" = "WEB_RETURNS",
                "hive.sql.dbcp.maxActive" = "1"
);

CREATE EXTERNAL TABLE customer
(
    c_customer_sk             int                           ,
    c_customer_id             char(16)                      ,
    c_current_cdemo_sk        int                           ,
    c_current_hdemo_sk        int                           ,
    c_current_addr_sk         int                           ,
    c_first_shipto_date_sk    int                           ,
    c_first_sales_date_sk     int                           ,
    c_salutation              char(10)                      ,
    c_first_name              char(20)                      ,
    c_last_name               char(30)                      ,
    c_preferred_cust_flag     char(1)                       ,
    c_birth_day               int                           ,
    c_birth_month             int                           ,
    c_birth_year              int                           ,
    c_birth_country           varchar(20)                   ,
    c_login                   char(13)                      ,
    c_email_address           char(50)                      ,
    c_last_review_date        char(10)
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
                "hive.sql.database.type" = "DERBY",
                "hive.sql.jdbc.driver" = "org.apache.derby.jdbc.EmbeddedDriver",
                "hive.sql.jdbc.url" = "jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_perf;collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "user1",
                "hive.sql.dbcp.password" = "passwd1",
                "hive.sql.table" = "CUSTOMER",
                "hive.sql.dbcp.maxActive" = "1"
);

CREATE EXTERNAL TABLE customer_address
(
    ca_address_sk             int                           ,
    ca_address_id             char(16)                      ,
    ca_street_number          char(10)                      ,
    ca_street_name            varchar(60)                   ,
    ca_street_type            char(15)                      ,
    ca_suite_number           char(10)                      ,
    ca_city                   varchar(60)                   ,
    ca_county                 varchar(30)                   ,
    ca_state                  char(2)                       ,
    ca_zip                    char(10)                      ,
    ca_country                varchar(20)                   ,
    ca_gmt_offset             decimal(5,2)                  ,
    ca_location_type          char(20)
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
                "hive.sql.database.type" = "DERBY",
                "hive.sql.jdbc.driver" = "org.apache.derby.jdbc.EmbeddedDriver",
                "hive.sql.jdbc.url" = "jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_perf;collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "user1",
                "hive.sql.dbcp.password" = "passwd1",
                "hive.sql.table" = "CUSTOMER_ADDRESS",
                "hive.sql.dbcp.maxActive" = "1"
);

CREATE EXTERNAL TABLE customer_demographics
(
    cd_demo_sk                int                           ,
    cd_gender                 char(1)                       ,
    cd_marital_status         char(1)                       ,
    cd_education_status       char(20)                      ,
    cd_purchase_estimate      int                           ,
    cd_credit_rating          char(10)                      ,
    cd_dep_count              int                           ,
    cd_dep_employed_count     int                           ,
    cd_dep_college_count      int
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
                "hive.sql.database.type" = "DERBY",
                "hive.sql.jdbc.driver" = "org.apache.derby.jdbc.EmbeddedDriver",
                "hive.sql.jdbc.url" = "jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_perf;collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "user1",
                "hive.sql.dbcp.password" = "passwd1",
                "hive.sql.table" = "CUSTOMER_DEMOGRAPHICS",
                "hive.sql.dbcp.maxActive" = "1"
);

CREATE EXTERNAL TABLE inventory
(
    inv_date_sk               int                           ,
    inv_item_sk               int                           ,
    inv_warehouse_sk          int                           ,
    inv_quantity_on_hand      int
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
                "hive.sql.database.type" = "DERBY",
                "hive.sql.jdbc.driver" = "org.apache.derby.jdbc.EmbeddedDriver",
                "hive.sql.jdbc.url" = "jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_perf;collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "user1",
                "hive.sql.dbcp.password" = "passwd1",
                "hive.sql.table" = "INVENTORY",
                "hive.sql.dbcp.maxActive" = "1"
);

CREATE EXTERNAL TABLE item
(
    i_item_sk                 int                           ,
    i_item_id                 char(16)                      ,
    i_rec_start_date          date                          ,
    i_rec_end_date            date                          ,
    i_item_desc               varchar(200)                  ,
    i_current_price           decimal(7,2)                  ,
    i_wholesale_cost          decimal(7,2)                  ,
    i_brand_id                int                           ,
    i_brand                   char(50)                      ,
    i_class_id                int                           ,
    i_class                   char(50)                      ,
    i_category_id             int                           ,
    i_category                char(50)                      ,
    i_manufact_id             int                           ,
    i_manufact                char(50)                      ,
    i_size                    char(20)                      ,
    i_formulation             char(20)                      ,
    i_color                   char(20)                      ,
    i_units                   char(10)                      ,
    i_container               char(10)                      ,
    i_manager_id              int                           ,
    i_product_name            char(50)
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
                "hive.sql.database.type" = "DERBY",
                "hive.sql.jdbc.driver" = "org.apache.derby.jdbc.EmbeddedDriver",
                "hive.sql.jdbc.url" = "jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_perf;collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "user1",
                "hive.sql.dbcp.password" = "passwd1",
                "hive.sql.table" = "ITEM",
                "hive.sql.dbcp.maxActive" = "1"
);

CREATE EXTERNAL TABLE warehouse
(
    w_warehouse_sk            int                           ,
    w_warehouse_id            char(16)                      ,
    w_warehouse_name          varchar(20)                   ,
    w_warehouse_sq_ft         int                           ,
    w_street_number           char(10)                      ,
    w_street_name             varchar(60)                   ,
    w_street_type             char(15)                      ,
    w_suite_number            char(10)                      ,
    w_city                    varchar(60)                   ,
    w_county                  varchar(30)                   ,
    w_state                   char(2)                       ,
    w_zip                     char(10)                      ,
    w_country                 varchar(20)                   ,
    w_gmt_offset              decimal(5,2)
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
                "hive.sql.database.type" = "DERBY",
                "hive.sql.jdbc.driver" = "org.apache.derby.jdbc.EmbeddedDriver",
                "hive.sql.jdbc.url" = "jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_perf;collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "user1",
                "hive.sql.dbcp.password" = "passwd1",
                "hive.sql.table" = "WAREHOUSE",
                "hive.sql.dbcp.maxActive" = "1"
);

CREATE EXTERNAL TABLE date_dim
(
    d_date_sk                 int                           ,
    d_date_id                 char(16)                      ,
    d_date                    date                          ,
    d_month_seq               int                           ,
    d_week_seq                int                           ,
    d_quarter_seq             int                           ,
    d_year                    int                           ,
    d_dow                     int                           ,
    d_moy                     int                           ,
    d_dom                     int                           ,
    d_qoy                     int                           ,
    d_fy_year                 int                           ,
    d_fy_quarter_seq          int                           ,
    d_fy_week_seq             int                           ,
    d_day_name                char(9)                       ,
    d_quarter_name            char(6)                       ,
    d_holiday                 char(1)                       ,
    d_weekend                 char(1)                       ,
    d_following_holiday       char(1)                       ,
    d_first_dom               int                           ,
    d_last_dom                int                           ,
    d_same_day_ly             int                           ,
    d_same_day_lq             int                           ,
    d_current_day             char(1)                       ,
    d_current_week            char(1)                       ,
    d_current_month           char(1)                       ,
    d_current_quarter         char(1)                       ,
    d_current_year            char(1)
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
                "hive.sql.database.type" = "DERBY",
                "hive.sql.jdbc.driver" = "org.apache.derby.jdbc.EmbeddedDriver",
                "hive.sql.jdbc.url" = "jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_perf;collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "user1",
                "hive.sql.dbcp.password" = "passwd1",
                "hive.sql.table" = "DATE_DIM",
                "hive.sql.dbcp.maxActive" = "1"
);

CREATE EXTERNAL TABLE store
(
    s_store_sk                int                           ,
    s_store_id                char(16)                      ,
    s_rec_start_date          date                          ,
    s_rec_end_date            date                          ,
    s_closed_date_sk          int                           ,
    s_store_name              varchar(50)                   ,
    s_number_employees        int                           ,
    s_floor_space             int                           ,
    s_hours                   char(20)                      ,
    s_manager                 varchar(40)                   ,
    s_market_id               int                           ,
    s_geography_class         varchar(100)                  ,
    s_market_desc             varchar(100)                  ,
    s_market_manager          varchar(40)                   ,
    s_division_id             int                           ,
    s_division_name           varchar(50)                   ,
    s_company_id              int                           ,
    s_company_name            varchar(50)                   ,
    s_street_number           varchar(10)                   ,
    s_street_name             varchar(60)                   ,
    s_street_type             char(15)                      ,
    s_suite_number            char(10)                      ,
    s_city                    varchar(60)                   ,
    s_county                  varchar(30)                   ,
    s_state                   char(2)                       ,
    s_zip                     char(10)                      ,
    s_country                 varchar(20)                   ,
    s_gmt_offset              decimal(5,2)                  ,
    s_tax_precentage          decimal(5,2)                  
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
                "hive.sql.database.type" = "DERBY",
                "hive.sql.jdbc.driver" = "org.apache.derby.jdbc.EmbeddedDriver",
                "hive.sql.jdbc.url" = "jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_perf;collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "user1",
                "hive.sql.dbcp.password" = "passwd1",
                "hive.sql.table" = "STORE",
                "hive.sql.dbcp.maxActive" = "1"
);

CREATE EXTERNAL TABLE household_demographics
(
    hd_demo_sk                int                           ,
    hd_income_band_sk         int                           ,
    hd_buy_potential          char(15)                      ,
    hd_dep_count              int                           ,
    hd_vehicle_count          int                           
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
                "hive.sql.database.type" = "DERBY",
                "hive.sql.jdbc.driver" = "org.apache.derby.jdbc.EmbeddedDriver",
                "hive.sql.jdbc.url" = "jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_perf;collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "user1",
                "hive.sql.dbcp.password" = "passwd1",
                "hive.sql.table" = "HOUSEHOLD_DEMOGRAPHICS",
                "hive.sql.dbcp.maxActive" = "1"
);

CREATE EXTERNAL TABLE time_dim
(
    t_time_sk                 int                           ,
    t_time_id                 char(16)                      ,
    t_time                    int                           ,
    t_hour                    int                           ,
    t_minute                  int                           ,
    t_second                  int                           ,
    t_am_pm                   char(2)                       ,
    t_shift                   char(20)                      ,
    t_sub_shift               char(20)                      ,
    t_meal_time               char(20)                      
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
                "hive.sql.database.type" = "DERBY",
                "hive.sql.jdbc.driver" = "org.apache.derby.jdbc.EmbeddedDriver",
                "hive.sql.jdbc.url" = "jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_perf;collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "user1",
                "hive.sql.dbcp.password" = "passwd1",
                "hive.sql.table" = "TIME_DIM",
                "hive.sql.dbcp.maxActive" = "1"
);

CREATE EXTERNAL TABLE reason
(
    r_reason_sk               int                           ,
    r_reason_id               char(16)                      ,
    r_reason_desc             char(100)
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
                "hive.sql.database.type" = "DERBY",
                "hive.sql.jdbc.driver" = "org.apache.derby.jdbc.EmbeddedDriver",
                "hive.sql.jdbc.url" = "jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_perf;collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "user1",
                "hive.sql.dbcp.password" = "passwd1",
                "hive.sql.table" = "REASON",
                "hive.sql.dbcp.maxActive" = "1"
);


explain
select inv1.w_warehouse_sk, inv1.i_item_sk, inv1.d_moy, inv1.mean, inv1.cov,
       inv2.w_warehouse_sk, inv2.i_item_sk, inv2.d_moy, inv2.mean, inv2.cov
from (select w_warehouse_name, w_warehouse_sk, i_item_sk, d_moy,
             stdev, mean, case mean when 0.0
                                    then null else stdev/mean end cov
      from (select w_warehouse_name, w_warehouse_sk, i_item_sk, d_moy,
                   sum(inv_quantity_on_hand) as stdev,
                   avg(inv_quantity_on_hand) as mean
            from inventory
            join item on inventory.inv_item_sk = item.i_item_sk
            join warehouse on inventory.inv_warehouse_sk = warehouse.w_warehouse_sk
            join date_dim on inventory.inv_date_sk = date_dim.d_date_sk
            where d_year = 2001
            group by w_warehouse_name, w_warehouse_sk, i_item_sk, d_moy) foo
      where case mean when 0.0
                      then 0.0 else stdev/mean end > 1) inv1
join (select w_warehouse_name, w_warehouse_sk, i_item_sk, d_moy,
             stdev, mean, case mean when 0.0
                                    then null else stdev/mean end cov
      from (select w_warehouse_name, w_warehouse_sk, i_item_sk, d_moy,
                   sum(inv_quantity_on_hand) as stdev,
                   avg(inv_quantity_on_hand) as mean
            from inventory
            join item on inventory.inv_item_sk = item.i_item_sk
            join warehouse on inventory.inv_warehouse_sk = warehouse.w_warehouse_sk
            join date_dim on inventory.inv_date_sk = date_dim.d_date_sk
            where d_year = 2001
            group by w_warehouse_name, w_warehouse_sk, i_item_sk, d_moy) foo
      where case mean when 0.0
                      then 0.0 else stdev/mean end > 1) inv2
  on inv1.i_item_sk = inv2.i_item_sk
     and inv1.w_warehouse_sk = inv2.w_warehouse_sk
where inv1.d_moy = 1 and inv2.d_moy = 1+1
order by inv1.w_warehouse_sk, inv1.i_item_sk, inv1.d_moy, inv1.mean, inv1.cov,
         inv2.d_moy, inv2.mean, inv2.cov;
select inv1.w_warehouse_sk, inv1.i_item_sk, inv1.d_moy, inv1.mean, inv1.cov,
       inv2.w_warehouse_sk, inv2.i_item_sk, inv2.d_moy, inv2.mean, inv2.cov
from (select w_warehouse_name, w_warehouse_sk, i_item_sk, d_moy,
             stdev, mean, case mean when 0.0
                                    then null else stdev/mean end cov
      from (select w_warehouse_name, w_warehouse_sk, i_item_sk, d_moy,
                   sum(inv_quantity_on_hand) as stdev,
                   avg(inv_quantity_on_hand) as mean
            from inventory
            join item on inventory.inv_item_sk = item.i_item_sk
            join warehouse on inventory.inv_warehouse_sk = warehouse.w_warehouse_sk
            join date_dim on inventory.inv_date_sk = date_dim.d_date_sk
            where d_year = 2001
            group by w_warehouse_name, w_warehouse_sk, i_item_sk, d_moy) foo
      where case mean when 0.0
                      then 0.0 else stdev/mean end > 1) inv1
join (select w_warehouse_name, w_warehouse_sk, i_item_sk, d_moy,
             stdev, mean, case mean when 0.0
                                    then null else stdev/mean end cov
      from (select w_warehouse_name, w_warehouse_sk, i_item_sk, d_moy,
                   sum(inv_quantity_on_hand) as stdev,
                   avg(inv_quantity_on_hand) as mean
            from inventory
            join item on inventory.inv_item_sk = item.i_item_sk
            join warehouse on inventory.inv_warehouse_sk = warehouse.w_warehouse_sk
            join date_dim on inventory.inv_date_sk = date_dim.d_date_sk
            where d_year = 2001
            group by w_warehouse_name, w_warehouse_sk, i_item_sk, d_moy) foo
      where case mean when 0.0
                      then 0.0 else stdev/mean end > 1) inv2
  on inv1.i_item_sk = inv2.i_item_sk
     and inv1.w_warehouse_sk = inv2.w_warehouse_sk
where inv1.d_moy = 1 and inv2.d_moy = 1+1
order by inv1.w_warehouse_sk, inv1.i_item_sk, inv1.d_moy, inv1.mean, inv1.cov,
         inv2.d_moy, inv2.mean, inv2.cov;


explain
SELECT cd_gender, 
       cd_marital_status, 
       cd_education_status, 
       Count(*) cnt1, 
       cd_purchase_estimate, 
       Count(*) cnt2, 
       cd_credit_rating, 
       Count(*) cnt3 
FROM   customer c, 
       customer_address ca, 
       customer_demographics 
WHERE  c.c_current_addr_sk = ca.ca_address_sk 
       AND ca_state IN ( 'CO', 'IL', 'MN' ) 
       AND cd_demo_sk = c.c_current_cdemo_sk 
       AND EXISTS (SELECT * 
                   FROM   store_sales, 
                          date_dim 
                   WHERE  c.c_customer_sk = ss_customer_sk 
                          AND ss_sold_date_sk = d_date_sk 
                          AND d_year = 1999 
                          AND d_moy BETWEEN 1 AND 1 + 2) 
       AND ( NOT EXISTS (SELECT * 
                         FROM   web_sales, 
                                date_dim 
                         WHERE  c.c_customer_sk = ws_bill_customer_sk 
                                AND ws_sold_date_sk = d_date_sk 
                                AND d_year = 1999 
                                AND d_moy BETWEEN 1 AND 1 + 2) 
             AND NOT EXISTS (SELECT * 
                             FROM   catalog_sales, 
                                    date_dim 
                             WHERE  c.c_customer_sk = cs_ship_customer_sk 
                                    AND cs_sold_date_sk = d_date_sk 
                                    AND d_year = 1999 
                                    AND d_moy BETWEEN 1 AND 1 + 2) ) 
GROUP  BY cd_gender, 
          cd_marital_status, 
          cd_education_status, 
          cd_purchase_estimate, 
          cd_credit_rating 
ORDER  BY cd_gender, 
          cd_marital_status, 
          cd_education_status, 
          cd_purchase_estimate, 
          cd_credit_rating 
LIMIT  100; 
SELECT cd_gender, 
       cd_marital_status, 
       cd_education_status, 
       Count(*) cnt1, 
       cd_purchase_estimate, 
       Count(*) cnt2, 
       cd_credit_rating, 
       Count(*) cnt3 
FROM   customer c, 
       customer_address ca, 
       customer_demographics 
WHERE  c.c_current_addr_sk = ca.ca_address_sk 
       AND ca_state IN ( 'CO', 'IL', 'MN' ) 
       AND cd_demo_sk = c.c_current_cdemo_sk 
       AND EXISTS (SELECT * 
                   FROM   store_sales, 
                          date_dim 
                   WHERE  c.c_customer_sk = ss_customer_sk 
                          AND ss_sold_date_sk = d_date_sk 
                          AND d_year = 1999 
                          AND d_moy BETWEEN 1 AND 1 + 2) 
       AND ( NOT EXISTS (SELECT * 
                         FROM   web_sales, 
                                date_dim 
                         WHERE  c.c_customer_sk = ws_bill_customer_sk 
                                AND ws_sold_date_sk = d_date_sk 
                                AND d_year = 1999 
                                AND d_moy BETWEEN 1 AND 1 + 2) 
             AND NOT EXISTS (SELECT * 
                             FROM   catalog_sales, 
                                    date_dim 
                             WHERE  c.c_customer_sk = cs_ship_customer_sk 
                                    AND cs_sold_date_sk = d_date_sk 
                                    AND d_year = 1999 
                                    AND d_moy BETWEEN 1 AND 1 + 2) ) 
GROUP  BY cd_gender, 
          cd_marital_status, 
          cd_education_status, 
          cd_purchase_estimate, 
          cd_credit_rating 
ORDER  BY cd_gender, 
          cd_marital_status, 
          cd_education_status, 
          cd_purchase_estimate, 
          cd_credit_rating 
LIMIT  100;


explain
SELECT cd_gender, 
       cd_marital_status, 
       cd_education_status, 
       Count(*) cnt1, 
       cd_purchase_estimate, 
       Count(*) cnt2, 
       cd_credit_rating, 
       Count(*) cnt3 
FROM   customer c, 
       customer_address ca, 
       customer_demographics 
WHERE  c.c_current_addr_sk = ca.ca_address_sk 
       AND ca_state IN ( 'CO', 'IL', 'MN' ) 
       AND cd_demo_sk = c.c_current_cdemo_sk 
       AND EXISTS (SELECT * 
                   FROM   store_sales, 
                          date_dim 
                   WHERE  c.c_customer_sk = ss_customer_sk 
                          AND ss_sold_date_sk = d_date_sk 
                          AND d_year = 1999 
                          AND d_moy NOT BETWEEN 1 AND 1 + 2) 
       AND ( NOT EXISTS (SELECT * 
                         FROM   web_sales, 
                                date_dim 
                         WHERE  c.c_customer_sk = ws_bill_customer_sk 
                                AND ws_sold_date_sk = d_date_sk 
                                AND d_year = 1999 
                                AND d_moy NOT BETWEEN 1 AND 1 + 2) 
             AND NOT EXISTS (SELECT * 
                             FROM   catalog_sales, 
                                    date_dim 
                             WHERE  c.c_customer_sk = cs_ship_customer_sk 
                                    AND cs_sold_date_sk = d_date_sk 
                                    AND d_year = 1999 
                                    AND d_moy NOT BETWEEN 1 AND 1 + 2) ) 
GROUP  BY cd_gender, 
          cd_marital_status, 
          cd_education_status, 
          cd_purchase_estimate, 
          cd_credit_rating 
ORDER  BY cd_gender, 
          cd_marital_status, 
          cd_education_status, 
          cd_purchase_estimate, 
          cd_credit_rating 
LIMIT  100; 
SELECT cd_gender, 
       cd_marital_status, 
       cd_education_status, 
       Count(*) cnt1, 
       cd_purchase_estimate, 
       Count(*) cnt2, 
       cd_credit_rating, 
       Count(*) cnt3 
FROM   customer c, 
       customer_address ca, 
       customer_demographics 
WHERE  c.c_current_addr_sk = ca.ca_address_sk 
       AND ca_state IN ( 'CO', 'IL', 'MN' ) 
       AND cd_demo_sk = c.c_current_cdemo_sk 
       AND EXISTS (SELECT * 
                   FROM   store_sales, 
                          date_dim 
                   WHERE  c.c_customer_sk = ss_customer_sk 
                          AND ss_sold_date_sk = d_date_sk 
                          AND d_year = 1999 
                          AND d_moy NOT BETWEEN 1 AND 1 + 2) 
       AND ( NOT EXISTS (SELECT * 
                         FROM   web_sales, 
                                date_dim 
                         WHERE  c.c_customer_sk = ws_bill_customer_sk 
                                AND ws_sold_date_sk = d_date_sk 
                                AND d_year = 1999 
                                AND d_moy NOT BETWEEN 1 AND 1 + 2) 
             AND NOT EXISTS (SELECT * 
                             FROM   catalog_sales, 
                                    date_dim 
                             WHERE  c.c_customer_sk = cs_ship_customer_sk 
                                    AND cs_sold_date_sk = d_date_sk 
                                    AND d_year = 1999 
                                    AND d_moy NOT BETWEEN 1 AND 1 + 2) ) 
GROUP  BY cd_gender, 
          cd_marital_status, 
          cd_education_status, 
          cd_purchase_estimate, 
          cd_credit_rating 
ORDER  BY cd_gender, 
          cd_marital_status, 
          cd_education_status, 
          cd_purchase_estimate, 
          cd_credit_rating 
LIMIT  100;


explain
SELECT Count(*) 
FROM   (SELECT DISTINCT c_last_name, 
                        c_first_name, 
                        d_date 
        FROM   store_sales, 
               date_dim, 
               customer 
        WHERE  store_sales.ss_sold_date_sk = date_dim.d_date_sk 
               AND store_sales.ss_customer_sk = customer.c_customer_sk 
               AND d_month_seq BETWEEN 1212 AND 1212 + 11 
        intersect 
        SELECT DISTINCT c_last_name, 
                        c_first_name, 
                        d_date 
        FROM   catalog_sales, 
               date_dim, 
               customer 
        WHERE  catalog_sales.cs_sold_date_sk = date_dim.d_date_sk 
               AND catalog_sales.cs_bill_customer_sk = customer.c_customer_sk 
               AND d_month_seq BETWEEN 1212 AND 1212 + 11 
        intersect 
        SELECT DISTINCT c_last_name, 
                        c_first_name, 
                        d_date 
        FROM   web_sales, 
               date_dim, 
               customer 
        WHERE  web_sales.ws_sold_date_sk = date_dim.d_date_sk 
               AND web_sales.ws_bill_customer_sk = customer.c_customer_sk 
               AND d_month_seq BETWEEN 1212 AND 1212 + 11) hot_cust 
LIMIT  100;
SELECT Count(*) 
FROM   (SELECT DISTINCT c_last_name, 
                        c_first_name, 
                        d_date 
        FROM   store_sales, 
               date_dim, 
               customer 
        WHERE  store_sales.ss_sold_date_sk = date_dim.d_date_sk 
               AND store_sales.ss_customer_sk = customer.c_customer_sk 
               AND d_month_seq BETWEEN 1212 AND 1212 + 11 
        intersect 
        SELECT DISTINCT c_last_name, 
                        c_first_name, 
                        d_date 
        FROM   catalog_sales, 
               date_dim, 
               customer 
        WHERE  catalog_sales.cs_sold_date_sk = date_dim.d_date_sk 
               AND catalog_sales.cs_bill_customer_sk = customer.c_customer_sk 
               AND d_month_seq BETWEEN 1212 AND 1212 + 11 
        intersect 
        SELECT DISTINCT c_last_name, 
                        c_first_name, 
                        d_date 
        FROM   web_sales, 
               date_dim, 
               customer 
        WHERE  web_sales.ws_sold_date_sk = date_dim.d_date_sk 
               AND web_sales.ws_bill_customer_sk = customer.c_customer_sk 
               AND d_month_seq BETWEEN 1212 AND 1212 + 11) hot_cust 
LIMIT  100;


explain
WITH ss AS 
( 
         SELECT   i_item_id, 
                  Sum(ss_ext_sales_price) total_sales 
         FROM     store_sales, 
                  date_dim, 
                  customer_address, 
                  item 
         WHERE    i_item_id IN 
                  ( 
                         SELECT i_item_id 
                         FROM   item 
                         WHERE  i_color IN ('orchid', 
                                            'chiffon', 
                                            'lace')) 
         AND      ss_item_sk = i_item_sk 
         AND      ss_sold_date_sk = d_date_sk 
         AND      d_year = 2000 
         AND      d_moy = 1 
         AND      ss_addr_sk = ca_address_sk 
         AND      ca_gmt_offset = -8 
         GROUP BY i_item_id), cs AS 
( 
         SELECT   i_item_id, 
                  Sum(cs_ext_sales_price) total_sales 
         FROM     catalog_sales, 
                  date_dim, 
                  customer_address, 
                  item 
         WHERE    i_item_id IN 
                  ( 
                         SELECT i_item_id 
                         FROM   item 
                         WHERE  i_color IN ('orchid', 
                                            'chiffon', 
                                            'lace')) 
         AND      cs_item_sk = i_item_sk 
         AND      cs_sold_date_sk = d_date_sk 
         AND      d_year = 2000 
         AND      d_moy = 1 
         AND      cs_bill_addr_sk = ca_address_sk 
         AND      ca_gmt_offset = -8 
         GROUP BY i_item_id), ws AS 
( 
         SELECT   i_item_id, 
                  Sum(ws_ext_sales_price) total_sales 
         FROM     web_sales, 
                  date_dim, 
                  customer_address, 
                  item 
         WHERE    i_item_id IN 
                  ( 
                         SELECT i_item_id 
                         FROM   item 
                         WHERE  i_color IN ('orchid', 
                                            'chiffon', 
                                            'lace')) 
         AND      ws_item_sk = i_item_sk 
         AND      ws_sold_date_sk = d_date_sk 
         AND      d_year = 2000 
         AND      d_moy = 1 
         AND      ws_bill_addr_sk = ca_address_sk 
         AND      ca_gmt_offset = -8 
         GROUP BY i_item_id) 
SELECT   i_item_id , 
         Sum(total_sales) total_sales 
FROM     ( 
                SELECT * 
                FROM   ss 
                UNION ALL 
                SELECT * 
                FROM   cs 
                UNION ALL 
                SELECT * 
                FROM   ws) tmp1 
GROUP BY i_item_id 
ORDER BY total_sales limit 100;
WITH ss AS 
( 
         SELECT   i_item_id, 
                  Sum(ss_ext_sales_price) total_sales 
         FROM     store_sales, 
                  date_dim, 
                  customer_address, 
                  item 
         WHERE    i_item_id IN 
                  ( 
                         SELECT i_item_id 
                         FROM   item 
                         WHERE  i_color IN ('orchid', 
                                            'chiffon', 
                                            'lace')) 
         AND      ss_item_sk = i_item_sk 
         AND      ss_sold_date_sk = d_date_sk 
         AND      d_year = 2000 
         AND      d_moy = 1 
         AND      ss_addr_sk = ca_address_sk 
         AND      ca_gmt_offset = -8 
         GROUP BY i_item_id), cs AS 
( 
         SELECT   i_item_id, 
                  Sum(cs_ext_sales_price) total_sales 
         FROM     catalog_sales, 
                  date_dim, 
                  customer_address, 
                  item 
         WHERE    i_item_id IN 
                  ( 
                         SELECT i_item_id 
                         FROM   item 
                         WHERE  i_color IN ('orchid', 
                                            'chiffon', 
                                            'lace')) 
         AND      cs_item_sk = i_item_sk 
         AND      cs_sold_date_sk = d_date_sk 
         AND      d_year = 2000 
         AND      d_moy = 1 
         AND      cs_bill_addr_sk = ca_address_sk 
         AND      ca_gmt_offset = -8 
         GROUP BY i_item_id), ws AS 
( 
         SELECT   i_item_id, 
                  Sum(ws_ext_sales_price) total_sales 
         FROM     web_sales, 
                  date_dim, 
                  customer_address, 
                  item 
         WHERE    i_item_id IN 
                  ( 
                         SELECT i_item_id 
                         FROM   item 
                         WHERE  i_color IN ('orchid', 
                                            'chiffon', 
                                            'lace')) 
         AND      ws_item_sk = i_item_sk 
         AND      ws_sold_date_sk = d_date_sk 
         AND      d_year = 2000 
         AND      d_moy = 1 
         AND      ws_bill_addr_sk = ca_address_sk 
         AND      ca_gmt_offset = -8 
         GROUP BY i_item_id) 
SELECT   i_item_id , 
         Sum(total_sales) total_sales 
FROM     ( 
                SELECT * 
                FROM   ss 
                UNION ALL 
                SELECT * 
                FROM   cs 
                UNION ALL 
                SELECT * 
                FROM   ws) tmp1 
GROUP BY i_item_id 
ORDER BY total_sales limit 100;


explain
WITH sr_items AS 
( 
         SELECT   i_item_id               item_id, 
                  Sum(sr_return_quantity) sr_item_qty 
         FROM     store_returns, 
                  item, 
                  date_dim 
         WHERE    sr_item_sk = i_item_sk 
         AND      d_date IN 
                  ( 
                         SELECT d_date 
                         FROM   date_dim 
                         WHERE  d_week_seq IN 
                                ( 
                                       SELECT d_week_seq 
                                       FROM   date_dim 
                                       WHERE  d_date IN ('1998-01-02', 
                                                         '1998-10-15', 
                                                         '1998-11-10'))) 
         AND      sr_returned_date_sk = d_date_sk 
         GROUP BY i_item_id), cr_items AS 
( 
         SELECT   i_item_id               item_id, 
                  Sum(cr_return_quantity) cr_item_qty 
         FROM     catalog_returns, 
                  item, 
                  date_dim 
         WHERE    cr_item_sk = i_item_sk 
         AND      d_date IN 
                  ( 
                         SELECT d_date 
                         FROM   date_dim 
                         WHERE  d_week_seq IN 
                                ( 
                                       SELECT d_week_seq 
                                       FROM   date_dim 
                                       WHERE  d_date IN ('1998-01-02', 
                                                         '1998-10-15', 
                                                         '1998-11-10'))) 
         AND      cr_returned_date_sk = d_date_sk 
         GROUP BY i_item_id), wr_items AS 
( 
         SELECT   i_item_id               item_id, 
                  Sum(wr_return_quantity) wr_item_qty 
         FROM     web_returns, 
                  item, 
                  date_dim 
         WHERE    wr_item_sk = i_item_sk 
         AND      d_date IN 
                  ( 
                         SELECT d_date 
                         FROM   date_dim 
                         WHERE  d_week_seq IN 
                                ( 
                                       SELECT d_week_seq 
                                       FROM   date_dim 
                                       WHERE  d_date IN ('1998-01-02', 
                                                         '1998-10-15', 
                                                         '1998-11-10'))) 
         AND      wr_returned_date_sk = d_date_sk 
         GROUP BY i_item_id) 
SELECT   sr_items.item_id , 
         sr_item_qty , 
         sr_item_qty/(sr_item_qty+cr_item_qty+wr_item_qty)/3.0 * 100 sr_dev , 
         cr_item_qty , 
         cr_item_qty/(sr_item_qty+cr_item_qty+wr_item_qty)/3.0 * 100 cr_dev , 
         wr_item_qty , 
         wr_item_qty/(sr_item_qty+cr_item_qty+wr_item_qty)/3.0 * 100 wr_dev , 
         (sr_item_qty+cr_item_qty+wr_item_qty)/3.0                   average 
FROM     sr_items , 
         cr_items , 
         wr_items 
WHERE    sr_items.item_id=cr_items.item_id 
AND      sr_items.item_id=wr_items.item_id 
ORDER BY sr_items.item_id , 
         sr_item_qty limit 100;


explain
with ss as (
			 select
				  i_manufact_id,sum(ss_ext_sales_price) total_sales
			 from
				store_sales,
				date_dim,
				 customer_address,
				 item
			 where
				 i_manufact_id in (select
			  i_manufact_id
			from
			 item
			where i_category in ('Books'))
			 and     ss_item_sk              = i_item_sk
			 and     ss_sold_date_sk         = d_date_sk
			 and     d_year                  = 1999
			 and     d_moy                   = 3
			 and     ss_addr_sk              = ca_address_sk
			 and     ca_gmt_offset           = -6 
			 group by i_manufact_id),
			 cs as (
			 select
				  i_manufact_id,sum(cs_ext_sales_price) total_sales
			 from
				catalog_sales,
				date_dim,
				 customer_address,
				 item
			 where
				 i_manufact_id               in (select
			  i_manufact_id
			from
			 item
			where i_category in ('Books'))
			 and     cs_item_sk              = i_item_sk
			 and     cs_sold_date_sk         = d_date_sk
			 and     d_year                  = 1999
			 and     d_moy                   = 3
			 and     cs_bill_addr_sk         = ca_address_sk
			 and     ca_gmt_offset           = -6 
			 group by i_manufact_id),
			 ws as (
			 select
				  i_manufact_id,sum(ws_ext_sales_price) total_sales
			 from
				web_sales,
				date_dim,
				 customer_address,
				 item
			 where
				 i_manufact_id               in (select
			  i_manufact_id
			from
			 item
			where i_category in ('Books'))
			 and     ws_item_sk              = i_item_sk
			 and     ws_sold_date_sk         = d_date_sk
			 and     d_year                  = 1999
			 and     d_moy                   = 3
			 and     ws_bill_addr_sk         = ca_address_sk
			 and     ca_gmt_offset           = -6
			 group by i_manufact_id)
			  select  i_manufact_id ,sum(total_sales) total_sales
			 from  (select * from ss 
				union all
				select * from cs 
				union all
				select * from ws) tmp1
			 group by i_manufact_id
			 order by total_sales
			limit 100;
with ss as (
			 select
				  i_manufact_id,sum(ss_ext_sales_price) total_sales
			 from
				store_sales,
				date_dim,
				 customer_address,
				 item
			 where
				 i_manufact_id in (select
			  i_manufact_id
			from
			 item
			where i_category in ('Books'))
			 and     ss_item_sk              = i_item_sk
			 and     ss_sold_date_sk         = d_date_sk
			 and     d_year                  = 1999
			 and     d_moy                   = 3
			 and     ss_addr_sk              = ca_address_sk
			 and     ca_gmt_offset           = -6 
			 group by i_manufact_id),
			 cs as (
			 select
				  i_manufact_id,sum(cs_ext_sales_price) total_sales
			 from
				catalog_sales,
				date_dim,
				 customer_address,
				 item
			 where
				 i_manufact_id               in (select
			  i_manufact_id
			from
			 item
			where i_category in ('Books'))
			 and     cs_item_sk              = i_item_sk
			 and     cs_sold_date_sk         = d_date_sk
			 and     d_year                  = 1999
			 and     d_moy                   = 3
			 and     cs_bill_addr_sk         = ca_address_sk
			 and     ca_gmt_offset           = -6 
			 group by i_manufact_id),
			 ws as (
			 select
				  i_manufact_id,sum(ws_ext_sales_price) total_sales
			 from
				web_sales,
				date_dim,
				 customer_address,
				 item
			 where
				 i_manufact_id               in (select
			  i_manufact_id
			from
			 item
			where i_category in ('Books'))
			 and     ws_item_sk              = i_item_sk
			 and     ws_sold_date_sk         = d_date_sk
			 and     d_year                  = 1999
			 and     d_moy                   = 3
			 and     ws_bill_addr_sk         = ca_address_sk
			 and     ca_gmt_offset           = -6
			 group by i_manufact_id)
			  select  i_manufact_id ,sum(total_sales) total_sales
			 from  (select * from ss 
				union all
				select * from cs 
				union all
				select * from ws) tmp1
			 group by i_manufact_id
			 order by total_sales
			limit 100;


explain
select *
                          from (select count(*) as h8_30_to_9
                                from store_sales
                                join household_demographics
                                  on store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
                                join time_dim
                                  on store_sales.ss_sold_time_sk = time_dim.t_time_sk
                                join store
                                  on store_sales.ss_store_sk = store.s_store_sk
                                where time_dim.t_hour = 8
                                  and time_dim.t_minute >= 30
                                  and ((household_demographics.hd_dep_count = 4
                                        and household_demographics.hd_vehicle_count <= 3+2)
                                       or (household_demographics.hd_dep_count = 2
                                           and household_demographics.hd_vehicle_count<=0+2)
                                       or (household_demographics.hd_dep_count = 0
                                           and household_demographics.hd_vehicle_count<=1+2))
                                  and store.s_store_name = 'ese') s1 join
                               (select count(*) as h9_to_9_30
                                from store_sales
                                join household_demographics
                                  on store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
                                join time_dim
                                  on store_sales.ss_sold_time_sk = time_dim.t_time_sk
                                join store
                                  on store_sales.ss_store_sk = store.s_store_sk
                                where time_dim.t_hour = 9
                                  and time_dim.t_minute < 30
                                  and ((household_demographics.hd_dep_count = 4
                                        and household_demographics.hd_vehicle_count <= 3+2)
                                       or (household_demographics.hd_dep_count = 2
                                           and household_demographics.hd_vehicle_count<=0+2)
                                       or (household_demographics.hd_dep_count = 0
                                           and household_demographics.hd_vehicle_count<=1+2))
                                  and store.s_store_name = 'ese') s2 join
                               (select count(*) as h9_30_to_10
                                from store_sales
                                join household_demographics
                                  on store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
                                join time_dim
                                  on store_sales.ss_sold_time_sk = time_dim.t_time_sk
                                join store
                                  on store_sales.ss_store_sk = store.s_store_sk
                                where time_dim.t_hour = 9
                                  and time_dim.t_minute >= 30
                                  and ((household_demographics.hd_dep_count = 4
                                        and household_demographics.hd_vehicle_count <= 3+2)
                                       or (household_demographics.hd_dep_count = 2
                                           and household_demographics.hd_vehicle_count<=0+2)
                                       or (household_demographics.hd_dep_count = 0
                                           and household_demographics.hd_vehicle_count<=1+2))
                                  and store.s_store_name = 'ese') s3 join
                               (select count(*) as h10_to_10_30
                                from store_sales
                                join household_demographics
                                  on store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
                                join time_dim
                                  on store_sales.ss_sold_time_sk = time_dim.t_time_sk
                                join store
                                  on store_sales.ss_store_sk = store.s_store_sk
                                where time_dim.t_hour = 10
                                  and time_dim.t_minute < 30
                                  and ((household_demographics.hd_dep_count = 4
                                        and household_demographics.hd_vehicle_count <= 3+2)
                                       or (household_demographics.hd_dep_count = 2
                                           and household_demographics.hd_vehicle_count<=0+2)
                                       or (household_demographics.hd_dep_count = 0
                                           and household_demographics.hd_vehicle_count<=1+2))
                                  and store.s_store_name = 'ese') s4 join
                               (select count(*) as h10_30_to_11
                                from store_sales
                                join household_demographics
                                  on store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
                                join time_dim
                                  on store_sales.ss_sold_time_sk = time_dim.t_time_sk
                                join store
                                  on store_sales.ss_store_sk = store.s_store_sk
                                where time_dim.t_hour = 10
                                  and time_dim.t_minute >= 30
                                  and ((household_demographics.hd_dep_count = 4
                                        and household_demographics.hd_vehicle_count <= 3+2)
                                       or (household_demographics.hd_dep_count = 2
                                           and household_demographics.hd_vehicle_count<=0+2)
                                       or (household_demographics.hd_dep_count = 0
                                           and household_demographics.hd_vehicle_count<=1+2))
                                  and store.s_store_name = 'ese') s5 join
                               (select count(*) as h11_to_11_30
                                from store_sales
                                join household_demographics
                                  on store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
                                join time_dim
                                  on store_sales.ss_sold_time_sk = time_dim.t_time_sk
                                join store
                                  on store_sales.ss_store_sk = store.s_store_sk
                                where time_dim.t_hour = 11
                                  and time_dim.t_minute < 30
                                  and ((household_demographics.hd_dep_count = 4
                                        and household_demographics.hd_vehicle_count <= 3+2)
                                       or (household_demographics.hd_dep_count = 2
                                           and household_demographics.hd_vehicle_count<=0+2)
                                       or (household_demographics.hd_dep_count = 0
                                           and household_demographics.hd_vehicle_count<=1+2))
                                  and store.s_store_name = 'ese') s6 join
                               (select count(*) as h11_30_to_12
                                from store_sales
                                join household_demographics
                                  on store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
                                join time_dim
                                  on store_sales.ss_sold_time_sk = time_dim.t_time_sk
                                join store
                                  on store_sales.ss_store_sk = store.s_store_sk
                                where time_dim.t_hour = 11
                                  and time_dim.t_minute >= 30
                                  and ((household_demographics.hd_dep_count = 4
                                        and household_demographics.hd_vehicle_count <= 3+2)
                                       or (household_demographics.hd_dep_count = 2
                                           and household_demographics.hd_vehicle_count<=0+2)
                                       or (household_demographics.hd_dep_count = 0
                                           and household_demographics.hd_vehicle_count<=1+2))
                                  and store.s_store_name = 'ese') s7 join
                               (select count(*) as h12_to_12_30
                                from store_sales
                                join household_demographics
                                  on store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
                                join time_dim
                                  on store_sales.ss_sold_time_sk = time_dim.t_time_sk
                                join store
                                  on store_sales.ss_store_sk = store.s_store_sk
                                where time_dim.t_hour = 12
                                  and time_dim.t_minute < 30
                                  and ((household_demographics.hd_dep_count = 4
                                        and household_demographics.hd_vehicle_count <= 3+2)
                                       or (household_demographics.hd_dep_count = 2
                                           and household_demographics.hd_vehicle_count<=0+2)
                                       or (household_demographics.hd_dep_count = 0
                                           and household_demographics.hd_vehicle_count<=1+2))
                                  and store.s_store_name = 'ese') s8;
select *
                          from (select count(*) as h8_30_to_9
                                from store_sales
                                join household_demographics
                                  on store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
                                join time_dim
                                  on store_sales.ss_sold_time_sk = time_dim.t_time_sk
                                join store
                                  on store_sales.ss_store_sk = store.s_store_sk
                                where time_dim.t_hour = 8
                                  and time_dim.t_minute >= 30
                                  and ((household_demographics.hd_dep_count = 4
                                        and household_demographics.hd_vehicle_count <= 3+2)
                                       or (household_demographics.hd_dep_count = 2
                                           and household_demographics.hd_vehicle_count<=0+2)
                                       or (household_demographics.hd_dep_count = 0
                                           and household_demographics.hd_vehicle_count<=1+2))
                                  and store.s_store_name = 'ese') s1 join
                               (select count(*) as h9_to_9_30
                                from store_sales
                                join household_demographics
                                  on store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
                                join time_dim
                                  on store_sales.ss_sold_time_sk = time_dim.t_time_sk
                                join store
                                  on store_sales.ss_store_sk = store.s_store_sk
                                where time_dim.t_hour = 9
                                  and time_dim.t_minute < 30
                                  and ((household_demographics.hd_dep_count = 4
                                        and household_demographics.hd_vehicle_count <= 3+2)
                                       or (household_demographics.hd_dep_count = 2
                                           and household_demographics.hd_vehicle_count<=0+2)
                                       or (household_demographics.hd_dep_count = 0
                                           and household_demographics.hd_vehicle_count<=1+2))
                                  and store.s_store_name = 'ese') s2 join
                               (select count(*) as h9_30_to_10
                                from store_sales
                                join household_demographics
                                  on store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
                                join time_dim
                                  on store_sales.ss_sold_time_sk = time_dim.t_time_sk
                                join store
                                  on store_sales.ss_store_sk = store.s_store_sk
                                where time_dim.t_hour = 9
                                  and time_dim.t_minute >= 30
                                  and ((household_demographics.hd_dep_count = 4
                                        and household_demographics.hd_vehicle_count <= 3+2)
                                       or (household_demographics.hd_dep_count = 2
                                           and household_demographics.hd_vehicle_count<=0+2)
                                       or (household_demographics.hd_dep_count = 0
                                           and household_demographics.hd_vehicle_count<=1+2))
                                  and store.s_store_name = 'ese') s3 join
                               (select count(*) as h10_to_10_30
                                from store_sales
                                join household_demographics
                                  on store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
                                join time_dim
                                  on store_sales.ss_sold_time_sk = time_dim.t_time_sk
                                join store
                                  on store_sales.ss_store_sk = store.s_store_sk
                                where time_dim.t_hour = 10
                                  and time_dim.t_minute < 30
                                  and ((household_demographics.hd_dep_count = 4
                                        and household_demographics.hd_vehicle_count <= 3+2)
                                       or (household_demographics.hd_dep_count = 2
                                           and household_demographics.hd_vehicle_count<=0+2)
                                       or (household_demographics.hd_dep_count = 0
                                           and household_demographics.hd_vehicle_count<=1+2))
                                  and store.s_store_name = 'ese') s4 join
                               (select count(*) as h10_30_to_11
                                from store_sales
                                join household_demographics
                                  on store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
                                join time_dim
                                  on store_sales.ss_sold_time_sk = time_dim.t_time_sk
                                join store
                                  on store_sales.ss_store_sk = store.s_store_sk
                                where time_dim.t_hour = 10
                                  and time_dim.t_minute >= 30
                                  and ((household_demographics.hd_dep_count = 4
                                        and household_demographics.hd_vehicle_count <= 3+2)
                                       or (household_demographics.hd_dep_count = 2
                                           and household_demographics.hd_vehicle_count<=0+2)
                                       or (household_demographics.hd_dep_count = 0
                                           and household_demographics.hd_vehicle_count<=1+2))
                                  and store.s_store_name = 'ese') s5 join
                               (select count(*) as h11_to_11_30
                                from store_sales
                                join household_demographics
                                  on store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
                                join time_dim
                                  on store_sales.ss_sold_time_sk = time_dim.t_time_sk
                                join store
                                  on store_sales.ss_store_sk = store.s_store_sk
                                where time_dim.t_hour = 11
                                  and time_dim.t_minute < 30
                                  and ((household_demographics.hd_dep_count = 4
                                        and household_demographics.hd_vehicle_count <= 3+2)
                                       or (household_demographics.hd_dep_count = 2
                                           and household_demographics.hd_vehicle_count<=0+2)
                                       or (household_demographics.hd_dep_count = 0
                                           and household_demographics.hd_vehicle_count<=1+2))
                                  and store.s_store_name = 'ese') s6 join
                               (select count(*) as h11_30_to_12
                                from store_sales
                                join household_demographics
                                  on store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
                                join time_dim
                                  on store_sales.ss_sold_time_sk = time_dim.t_time_sk
                                join store
                                  on store_sales.ss_store_sk = store.s_store_sk
                                where time_dim.t_hour = 11
                                  and time_dim.t_minute >= 30
                                  and ((household_demographics.hd_dep_count = 4
                                        and household_demographics.hd_vehicle_count <= 3+2)
                                       or (household_demographics.hd_dep_count = 2
                                           and household_demographics.hd_vehicle_count<=0+2)
                                       or (household_demographics.hd_dep_count = 0
                                           and household_demographics.hd_vehicle_count<=1+2))
                                  and store.s_store_name = 'ese') s7 join
                               (select count(*) as h12_to_12_30
                                from store_sales
                                join household_demographics
                                  on store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
                                join time_dim
                                  on store_sales.ss_sold_time_sk = time_dim.t_time_sk
                                join store
                                  on store_sales.ss_store_sk = store.s_store_sk
                                where time_dim.t_hour = 12
                                  and time_dim.t_minute < 30
                                  and ((household_demographics.hd_dep_count = 4
                                        and household_demographics.hd_vehicle_count <= 3+2)
                                       or (household_demographics.hd_dep_count = 2
                                           and household_demographics.hd_vehicle_count<=0+2)
                                       or (household_demographics.hd_dep_count = 0
                                           and household_demographics.hd_vehicle_count<=1+2))
                                  and store.s_store_name = 'ese') s8;


explain
select substr(r_reason_desc, 1, 20) as r,
avg(ws_quantity) as wq,
avg(wr_refunded_cash) as ref,
avg(wr_fee) as fee
from web_sales
join web_returns
on web_sales.ws_item_sk = web_returns.wr_item_sk
and web_sales.ws_order_number = web_returns.wr_order_number
join customer_demographics cd1
on cd1.cd_demo_sk = web_returns.wr_refunded_cdemo_sk
join customer_demographics cd2
on cd2.cd_demo_sk = web_returns.wr_returning_cdemo_sk
join customer_address
on customer_address.ca_address_sk = web_returns.wr_refunded_addr_sk
join date_dim
on web_sales.ws_sold_date_sk = date_dim.d_date_sk
join reason
on reason.r_reason_sk = web_returns.wr_reason_sk
where d_year = 2000
and ((cd1.cd_marital_status = 'M'
and cd1.cd_marital_status = cd2.cd_marital_status
and cd1.cd_education_status = 'Advanced Degree'
and cd1.cd_education_status = cd2.cd_education_status
and ws_sales_price between 100.00 and 150.00)
or
(cd1.cd_marital_status = 'S'
and cd1.cd_marital_status = cd2.cd_marital_status
and cd1.cd_education_status = 'College'
and cd1.cd_education_status = cd2.cd_education_status
and ws_sales_price between 50.00 and 100.00)
or
(cd1.cd_marital_status = 'W'
and cd1.cd_marital_status = cd2.cd_marital_status
and cd1.cd_education_status = '2 yr Degree'
and cd1.cd_education_status = cd2.cd_education_status
and ws_sales_price between 150.00 and 200.00))
and ((ca_country = 'United States'
and ca_state in ('IN', 'OH', 'NJ')
and ws_net_profit between 100 and 200)
or
(ca_country = 'United States'
and ca_state in ('WI', 'CT', 'KY')
and ws_net_profit between 150 and 300)
or
(ca_country = 'United States'
and ca_state in ('LA', 'IA', 'AR')
and ws_net_profit between 50 and 250))
group by r_reason_desc
order by r, wq, ref, fee
limit 100;
select substr(r_reason_desc, 1, 20) as r,
avg(ws_quantity) as wq,
avg(wr_refunded_cash) as ref,
avg(wr_fee) as fee
from web_sales
join web_returns
on web_sales.ws_item_sk = web_returns.wr_item_sk
and web_sales.ws_order_number = web_returns.wr_order_number
join customer_demographics cd1
on cd1.cd_demo_sk = web_returns.wr_refunded_cdemo_sk
join customer_demographics cd2
on cd2.cd_demo_sk = web_returns.wr_returning_cdemo_sk
join customer_address
on customer_address.ca_address_sk = web_returns.wr_refunded_addr_sk
join date_dim
on web_sales.ws_sold_date_sk = date_dim.d_date_sk
join reason
on reason.r_reason_sk = web_returns.wr_reason_sk
where d_year = 2000
and ((cd1.cd_marital_status = 'M'
and cd1.cd_marital_status = cd2.cd_marital_status
and cd1.cd_education_status = 'Advanced Degree'
and cd1.cd_education_status = cd2.cd_education_status
and ws_sales_price between 100.00 and 150.00)
or
(cd1.cd_marital_status = 'S'
and cd1.cd_marital_status = cd2.cd_marital_status
and cd1.cd_education_status = 'College'
and cd1.cd_education_status = cd2.cd_education_status
and ws_sales_price between 50.00 and 100.00)
or
(cd1.cd_marital_status = 'W'
and cd1.cd_marital_status = cd2.cd_marital_status
and cd1.cd_education_status = '2 yr Degree'
and cd1.cd_education_status = cd2.cd_education_status
and ws_sales_price between 150.00 and 200.00))
and ((ca_country = 'United States'
and ca_state in ('IN', 'OH', 'NJ')
and ws_net_profit between 100 and 200)
or
(ca_country = 'United States'
and ca_state in ('WI', 'CT', 'KY')
and ws_net_profit between 150 and 300)
or
(ca_country = 'United States'
and ca_state in ('LA', 'IA', 'AR')
and ws_net_profit between 50 and 250))
group by r_reason_desc
order by r, wq, ref, fee
limit 100;


set hive.auto.convert.anti.join=false;

explain
SELECT cd_gender,
       cd_marital_status,
       cd_education_status,
       Count(*) cnt1,
       cd_purchase_estimate,
       Count(*) cnt2,
       cd_credit_rating,
       Count(*) cnt3
FROM   customer c,
       customer_address ca,
       customer_demographics
WHERE  c.c_current_addr_sk = ca.ca_address_sk
       AND ca_state IN ( 'CO', 'IL', 'MN' )
       AND cd_demo_sk = c.c_current_cdemo_sk
       AND EXISTS (SELECT *
                   FROM   store_sales,
                          date_dim
                   WHERE  c.c_customer_sk = ss_customer_sk
                          AND ss_sold_date_sk = d_date_sk
                          AND d_year = 1999
                          AND d_moy BETWEEN 1 AND 1 + 2)
       AND ( NOT EXISTS (SELECT *
                         FROM   web_sales,
                                date_dim
                         WHERE  c.c_customer_sk = ws_bill_customer_sk
                                AND ws_sold_date_sk = d_date_sk
                                AND d_year = 1999
                                AND d_moy BETWEEN 1 AND 1 + 2)
             AND NOT EXISTS (SELECT *
                             FROM   catalog_sales,
                                    date_dim
                             WHERE  c.c_customer_sk = cs_ship_customer_sk
                                    AND cs_sold_date_sk = d_date_sk
                                    AND d_year = 1999
                                    AND d_moy BETWEEN 1 AND 1 + 2) )
GROUP  BY cd_gender,
          cd_marital_status,
          cd_education_status,
          cd_purchase_estimate,
          cd_credit_rating
ORDER  BY cd_gender,
          cd_marital_status,
          cd_education_status,
          cd_purchase_estimate,
          cd_credit_rating
LIMIT  100;
SELECT cd_gender,
       cd_marital_status,
       cd_education_status,
       Count(*) cnt1,
       cd_purchase_estimate,
       Count(*) cnt2,
       cd_credit_rating,
       Count(*) cnt3
FROM   customer c,
       customer_address ca,
       customer_demographics
WHERE  c.c_current_addr_sk = ca.ca_address_sk
       AND ca_state IN ( 'CO', 'IL', 'MN' )
       AND cd_demo_sk = c.c_current_cdemo_sk
       AND EXISTS (SELECT *
                   FROM   store_sales,
                          date_dim
                   WHERE  c.c_customer_sk = ss_customer_sk
                          AND ss_sold_date_sk = d_date_sk
                          AND d_year = 1999
                          AND d_moy BETWEEN 1 AND 1 + 2)
       AND ( NOT EXISTS (SELECT *
                         FROM   web_sales,
                                date_dim
                         WHERE  c.c_customer_sk = ws_bill_customer_sk
                                AND ws_sold_date_sk = d_date_sk
                                AND d_year = 1999
                                AND d_moy BETWEEN 1 AND 1 + 2)
             AND NOT EXISTS (SELECT *
                             FROM   catalog_sales,
                                    date_dim
                             WHERE  c.c_customer_sk = cs_ship_customer_sk
                                    AND cs_sold_date_sk = d_date_sk
                                    AND d_year = 1999
                                    AND d_moy BETWEEN 1 AND 1 + 2) )
GROUP  BY cd_gender,
          cd_marital_status,
          cd_education_status,
          cd_purchase_estimate,
          cd_credit_rating
ORDER  BY cd_gender,
          cd_marital_status,
          cd_education_status,
          cd_purchase_estimate,
          cd_credit_rating
LIMIT  100;



explain
SELECT cd_gender,
       cd_marital_status,
       cd_education_status,
       Count(*) cnt1,
       cd_purchase_estimate,
       Count(*) cnt2,
       cd_credit_rating,
       Count(*) cnt3
FROM   customer c,
       customer_address ca,
       customer_demographics
WHERE  c.c_current_addr_sk = ca.ca_address_sk
       AND ca_state IN ( 'CO', 'IL', 'MN' )
       AND cd_demo_sk = c.c_current_cdemo_sk
       AND EXISTS (SELECT *
                   FROM   store_sales,
                          date_dim
                   WHERE  c.c_customer_sk = ss_customer_sk
                          AND ss_sold_date_sk = d_date_sk
                          AND d_year = 1999
                          AND d_moy NOT BETWEEN 1 AND 1 + 2)
       AND ( NOT EXISTS (SELECT *
                         FROM   web_sales,
                                date_dim
                         WHERE  c.c_customer_sk = ws_bill_customer_sk
                                AND ws_sold_date_sk = d_date_sk
                                AND d_year = 1999
                                AND d_moy NOT BETWEEN 1 AND 1 + 2)
             AND NOT EXISTS (SELECT *
                             FROM   catalog_sales,
                                    date_dim
                             WHERE  c.c_customer_sk = cs_ship_customer_sk
                                    AND cs_sold_date_sk = d_date_sk
                                    AND d_year = 1999
                                    AND d_moy NOT BETWEEN 1 AND 1 + 2) )
GROUP  BY cd_gender,
          cd_marital_status,
          cd_education_status,
          cd_purchase_estimate,
          cd_credit_rating
ORDER  BY cd_gender,
          cd_marital_status,
          cd_education_status,
          cd_purchase_estimate,
          cd_credit_rating
LIMIT  100;
SELECT cd_gender,
       cd_marital_status,
       cd_education_status,
       Count(*) cnt1,
       cd_purchase_estimate,
       Count(*) cnt2,
       cd_credit_rating,
       Count(*) cnt3
FROM   customer c,
       customer_address ca,
       customer_demographics
WHERE  c.c_current_addr_sk = ca.ca_address_sk
       AND ca_state IN ( 'CO', 'IL', 'MN' )
       AND cd_demo_sk = c.c_current_cdemo_sk
       AND EXISTS (SELECT *
                   FROM   store_sales,
                          date_dim
                   WHERE  c.c_customer_sk = ss_customer_sk
                          AND ss_sold_date_sk = d_date_sk
                          AND d_year = 1999
                          AND d_moy NOT BETWEEN 1 AND 1 + 2)
       AND ( NOT EXISTS (SELECT *
                         FROM   web_sales,
                                date_dim
                         WHERE  c.c_customer_sk = ws_bill_customer_sk
                                AND ws_sold_date_sk = d_date_sk
                                AND d_year = 1999
                                AND d_moy NOT BETWEEN 1 AND 1 + 2)
             AND NOT EXISTS (SELECT *
                             FROM   catalog_sales,
                                    date_dim
                             WHERE  c.c_customer_sk = cs_ship_customer_sk
                                    AND cs_sold_date_sk = d_date_sk
                                    AND d_year = 1999
                                    AND d_moy NOT BETWEEN 1 AND 1 + 2) )
GROUP  BY cd_gender,
          cd_marital_status,
          cd_education_status,
          cd_purchase_estimate,
          cd_credit_rating
ORDER  BY cd_gender,
          cd_marital_status,
          cd_education_status,
          cd_purchase_estimate,
          cd_credit_rating
LIMIT  100;

explain
SELECT ranking
FROM
    (SELECT rank() OVER (PARTITION BY ss_store_sk
        ORDER BY sum(ss_net_profit)) AS ranking
     FROM store_sales
     GROUP BY ss_store_sk) tmp1
WHERE ranking <= 5;

SELECT ranking
FROM
    (SELECT rank() OVER (PARTITION BY ss_store_sk
        ORDER BY sum(ss_net_profit)) AS ranking
     FROM store_sales
     GROUP BY ss_store_sk) tmp1
WHERE ranking <= 5;

set hive.auto.convert.anti.join=true;

DROP TABLE catalog_sales;
DROP TABLE catalog_returns;
DROP TABLE store_sales;
DROP TABLE store_returns;
DROP TABLE web_sales;
DROP TABLE web_returns;
DROP TABLE customer;
DROP TABLE customer_address;
DROP TABLE customer_demographics;
DROP TABLE inventory;
DROP TABLE item;
DROP TABLE warehouse;
DROP TABLE date_dim;
DROP TABLE store;
DROP TABLE household_demographics;
DROP TABLE time_dim;
DROP TABLE reason;

FROM src
SELECT
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_perf','user1','passwd1',
'DROP TABLE CATALOG_SALES' ),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_perf','user1','passwd1',
'DROP TABLE CATALOG_RETURNS' ),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_perf','user1','passwd1',
'DROP TABLE STORE_SALES' ),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_perf','user1','passwd1',
'DROP TABLE STORE_RETURNS' ),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_perf','user1','passwd1',
'DROP TABLE WEB_SALES' ),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_perf','user1','passwd1',
'DROP TABLE WEB_RETURNS' ),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_perf','user1','passwd1',
'DROP TABLE CUSTOMER' ),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_perf','user1','passwd1',
'DROP TABLE CUSTOMER_ADDRESS' ),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_perf','user1','passwd1',
'DROP TABLE CUSTOMER_DEMOGRAPHICS' ),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_perf','user1','passwd1',
'DROP TABLE INVENTORY' ),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_perf','user1','passwd1',
'DROP TABLE ITEM' ),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_perf','user1','passwd1',
'DROP TABLE WAREHOUSE' ),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_perf','user1','passwd1',
'DROP TABLE DATE_DIM' ),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_perf','user1','passwd1',
'DROP TABLE STORE' ),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_perf','user1','passwd1',
'DROP TABLE HOUSEHOLD_DEMOGRAPHICS' ),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_perf','user1','passwd1',
'DROP TABLE TIME_DIM' ),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_perf','user1','passwd1',
'DROP TABLE REASON' )
limit 1;
