drop tablE IF EXISTS impala_tpch_customer;
CREATE TABLE impala_tpch_customer (
  c_custkey BIGINT,
  c_name STRING,
  c_address STRING,
  c_nationkey SMALLINT,
  c_phone STRING,
  c_acctbal DECIMAL(12,2),
  c_mktsegment STRING,
  c_comment STRING
) STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");

DROP TABLE IF EXISTS impala_tpch_lineitem;
CREATE TABLE impala_tpch_lineitem (
   l_orderkey BIGINT,
   l_partkey BIGINT,
   l_suppkey BIGINT,
   l_linenumber INT,
   l_quantity DECIMAL(12,2),
   l_extendedprice DECIMAL(12,2),
   l_discount DECIMAL(12,2),
   l_tax DECIMAL(12,2),
   l_returnflag STRING,
   l_linestatus STRING,
   l_shipdate STRING,
   l_commitdate STRING,
   l_receiptdate STRING,
   l_shipinstruct STRING,
   l_shipmode STRING,
   l_comment STRING
) STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");

DROP TABLE IF EXISTS impala_tpch_nation;
CREATE TABLE impala_tpch_nation (
  n_nationkey SMALLINT,
  n_name STRING,
  n_regionkey SMALLINT,
  n_comment STRING
) STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");

DROP TABLE IF EXISTS impala_tpch_orders;
CREATE TABLE impala_tpch_orders (
  o_orderkey BIGINT,
  o_custkey BIGINT,
  o_orderstatus STRING,
  o_totalprice DECIMAL(12,2),
  o_orderdate STRING,
  o_orderpriority STRING,
  o_clerk STRING,
  o_shippriority INT,
  o_comment STRING
) STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");

DROP TABLE IF EXISTS impala_tpch_part;
CREATE TABLE impala_tpch_part (
  p_partkey BIGINT,
  p_name STRING,
  p_mfgr STRING,
  p_brand STRING,
  p_type STRING,
  p_size INT,
  p_container STRING,
  p_retailprice DECIMAL(12,2),
  p_comment STRING
) STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");

DROP TABLE IF EXISTS impala_tpch_partsupp;
CREATE TABLE impala_tpch_partsupp (
  ps_partkey BIGINT,
  ps_suppkey BIGINT,
  ps_availqty INT,
  ps_supplycost DECIMAL(12,2),
  ps_comment STRING
) STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");

DROP TABLE IF EXISTS impala_tpch_region;
CREATE TABLE impala_tpch_region (
  r_regionkey SMALLINT,
  r_name STRING,
  r_comment STRING
) STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");

DROP TABLE IF EXISTS impala_tpch_supplier;
CREATE TABLE impala_tpch_supplier (
  s_suppkey BIGINT,
  s_name STRING,
  s_address STRING,
  s_nationkey SMALLINT,
  s_phone STRING,
  s_acctbal DECIMAL(12,2),
  s_comment STRING
) STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");

DROP TABLE IF EXISTS impala_tpcds_call_center;
CREATE TABLE impala_tpcds_call_center (
  cc_call_center_sk INT,
  cc_call_center_id STRING,
  cc_rec_start_date STRING,
  cc_rec_end_date STRING,
  cc_closed_date_sk INT,
  cc_open_date_sk INT,
  cc_name STRING,
  cc_class STRING,
  cc_employees INT,
  cc_sq_ft INT,
  cc_hours STRING,
  cc_manager STRING,
  cc_mkt_id INT,
  cc_mkt_class STRING,
  cc_mkt_desc STRING,
  cc_market_manager STRING,
  cc_division INT,
  cc_division_name STRING,
  cc_company INT,
  cc_company_name STRING,
  cc_street_number STRING,
  cc_street_name STRING,
  cc_street_type STRING,
  cc_suite_number STRING,
  cc_city STRING,
  cc_county STRING,
  cc_state STRING,
  cc_zip STRING,
  cc_country STRING,
  cc_gmt_offset DECIMAL(5,2),
  cc_tax_percentage DECIMAL(5,2)
) STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");

DROP TABLE IF EXISTS impala_tpcds_catalog_page;
CREATE TABLE impala_tpcds_catalog_page (
  cp_catalog_page_sk INT,
  cp_catalog_page_id STRING,
  cp_start_date_sk INT,
  cp_end_date_sk INT,
  cp_department STRING,
  cp_catalog_number INT,
  cp_catalog_page_number INT,
  cp_description STRING,
  cp_type STRING
) STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");

DROP TABLE IF EXISTS impala_tpcds_catalog_returns;
CREATE TABLE impala_tpcds_catalog_returns (
  `cr_returned_date_sk` int,
  `cr_returned_time_sk` int,
  `cr_item_sk` bigint,
  `cr_refunded_customer_sk` int,
  `cr_refunded_cdemo_sk` int,
  `cr_refunded_hdemo_sk` int,
  `cr_refunded_addr_sk` int,
  `cr_returning_customer_sk` int,
  `cr_returning_cdemo_sk` int,
  `cr_returning_hdemo_sk` int,
  `cr_returning_addr_sk` int,
  `cr_call_center_sk` int,
  `cr_catalog_page_sk` int,
  `cr_ship_mode_sk` int,
  `cr_warehouse_sk` int,
  `cr_reason_sk` int,
  `cr_order_number` bigint,
  `cr_return_quantity` int,
  `cr_return_amount` decimal(7,2),
  `cr_return_tax` decimal(7,2),
  `cr_return_amt_inc_tax` decimal(7,2),
  `cr_fee` decimal(7,2),
  `cr_return_ship_cost` decimal(7,2),
  `cr_refunded_cash` decimal(7,2),
  `cr_reversed_charge` decimal(7,2),
  `cr_store_credit` decimal(7,2),
  `cr_net_loss` decimal(7,2)
) STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");


DROP TABLE IF EXISTS impala_tpcds_catalog_sales;
CREATE TABLE impala_tpcds_catalog_sales (
   `cs_sold_date_sk` int,
   `cs_sold_time_sk` int,
   `cs_ship_date_sk` int,
   `cs_bill_customer_sk` int,
   `cs_bill_cdemo_sk` int,
   `cs_bill_hdemo_sk` int,
   `cs_bill_addr_sk` int,
   `cs_ship_customer_sk` int,
   `cs_ship_cdemo_sk` int,
   `cs_ship_hdemo_sk` int,
   `cs_ship_addr_sk` int,
   `cs_call_center_sk` int,
   `cs_catalog_page_sk` int,
   `cs_ship_mode_sk` int,
   `cs_warehouse_sk` int,
   `cs_item_sk` bigint,
   `cs_promo_sk` int,
   `cs_order_number` bigint,
   `cs_quantity` int,
   `cs_wholesale_cost` decimal(7,2),
   `cs_list_price` decimal(7,2),
   `cs_sales_price` decimal(7,2),
   `cs_ext_discount_amt` decimal(7,2),
   `cs_ext_sales_price` decimal(7,2),
   `cs_ext_wholesale_cost` decimal(7,2),
   `cs_ext_list_price` decimal(7,2),
   `cs_ext_tax` decimal(7,2),
   `cs_coupon_amt` decimal(7,2),
   `cs_ext_ship_cost` decimal(7,2),
   `cs_net_paid` decimal(7,2),
   `cs_net_paid_inc_tax` decimal(7,2),
   `cs_net_paid_inc_ship` decimal(7,2),
   `cs_net_paid_inc_ship_tax` decimal(7,2),
   `cs_net_profit` decimal(7,2)
) STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");

DROP TABLE IF EXISTS impala_tpcds_customer;
 CREATE TABLE impala_tpcds_customer (
   `c_customer_sk` int,
   `c_customer_id` string,
   `c_current_cdemo_sk` int,
   `c_current_hdemo_sk` int,
   `c_current_addr_sk` int,
   `c_first_shipto_date_sk` int,
   `c_first_sales_date_sk` int,
   `c_salutation` string,
   `c_first_name` string,
   `c_last_name` string,
   `c_preferred_cust_flag` string,
   `c_birth_day` int,
   `c_birth_month` int,
   `c_birth_year` int,
   `c_birth_country` string,
   `c_login` string,
   `c_email_address` string,
   `c_last_review_date` string
) STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");

DROP TABLE IF EXISTS impala_tpcds_customer_address;
 CREATE TABLE impala_tpcds_customer_address (
   ca_address_sk INT,
   ca_address_id STRING,
   ca_street_number STRING,
   ca_street_name STRING,
   ca_street_type STRING,
   ca_suite_number STRING,
   ca_city STRING,
   ca_county STRING,
   ca_state STRING,
   ca_zip STRING,
   ca_country STRING,
   ca_gmt_offset DECIMAL(5,2),
   ca_location_type STRING
) STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");

DROP TABLE IF EXISTS impala_tpcds_customer_demographics;
 CREATE TABLE impala_tpcds_customer_demographics (
   `cd_demo_sk` int,
   `cd_gender` string,
   `cd_marital_status` string,
   `cd_education_status` string,
   `cd_purchase_estimate` int,
   `cd_credit_rating` string,
   `cd_dep_count` int,
   `cd_dep_employed_count` int,
   `cd_dep_college_count` int
) STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");

DROP TABLE IF EXISTS impala_tpcds_date_dim;
 CREATE TABLE impala_tpcds_date_dim (
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
   `d_current_year` string
) STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");

DROP TABLE IF EXISTS impala_tpcds_household_demographics;
 CREATE TABLE impala_tpcds_household_demographics (
   `hd_demo_sk` int,
   `hd_income_band_sk` int,
   `hd_buy_potential` string,
   `hd_dep_count` int,
   `hd_vehicle_count` int
) STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");


DROP TABLE IF EXISTS impala_tpcds_income_band;
 CREATE TABLE impala_tpcds_income_band (
   ib_income_band_sk INT,
   ib_lower_bound INT,
   ib_upper_bound INT
) STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");

DROP TABLE IF EXISTS impala_tpcds_inventory;
 CREATE TABLE impala_tpcds_inventory (
   inv_date_sk INT,
   inv_item_sk BIGINT,
   inv_warehouse_sk INT,
   inv_quantity_on_hand INT
) STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");

DROP TABLE IF EXISTS impala_tpcds_item;
 CREATE TABLE impala_tpcds_item (
   i_item_sk BIGINT,
   i_item_id STRING,
   i_rec_start_date STRING,
   i_rec_end_date STRING,
   i_item_desc STRING,
   i_current_price DECIMAL(7,2),
   i_wholesale_cost DECIMAL(7,2),
   i_brand_id INT,
   i_brand STRING,
   i_class_id INT,
   i_class STRING,
   i_category_id INT,
   i_category STRING,
   i_manufact_id INT,
   i_manufact STRING,
   i_size STRING,
   i_formulation STRING,
   i_color STRING,
   i_units STRING,
   i_container STRING,
   i_manager_id INT,
   i_product_name STRING
) STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");

DROP TABLE IF EXISTS impala_tpcds_promotion;
 CREATE TABLE impala_tpcds_promotion (
   `p_promo_sk` int,
   `p_promo_id` string,
   `p_start_date_sk` int,
   `p_end_date_sk` int,
   `p_item_sk` bigint,
   `p_cost` decimal(15,2),
   `p_response_target` int,
   `p_promo_name` string,
   `p_channel_dmail` string,
   `p_channel_email` string,
   `p_channel_catalog` string,
   `p_channel_tv` string,
   `p_channel_radio` string,
   `p_channel_press` string,
   `p_channel_event` string,
   `p_channel_demo` string,
   `p_channel_details` string,
   `p_purpose` string,
   `p_discount_active` string
) STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");

DROP TABLE IF EXISTS impala_tpcds_store_returns;
 CREATE TABLE impala_tpcds_store_returns (
   sr_returned_date_sk INT,
   sr_return_time_sk INT,
   sr_item_sk BIGINT,
   sr_customer_sk INT,
   sr_cdemo_sk INT,
   sr_hdemo_sk INT,
   sr_addr_sk INT,
   sr_store_sk INT,
   sr_reason_sk INT,
   sr_ticket_number BIGINT,
   sr_return_quantity INT,
   sr_return_amt DECIMAL(7,2),
   sr_return_tax DECIMAL(7,2),
   sr_return_amt_inc_tax DECIMAL(7,2),
   sr_fee DECIMAL(7,2),
   sr_return_ship_cost DECIMAL(7,2),
   sr_refunded_cash DECIMAL(7,2),
   sr_reversed_charge DECIMAL(7,2),
   sr_store_credit DECIMAL(7,2),
   sr_net_loss DECIMAL(7,2)
) STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");

DROP TABLE IF EXISTS impala_tpcds_store_sales;
 CREATE TABLE impala_tpcds_store_sales (
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
PARTITIONED BY (
   `ss_sold_date_sk` int
) STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");

DROP TABLE IF EXISTS impala_tpcds_store;
 CREATE TABLE impala_tpcds_store (
   s_store_sk INT,
   s_store_id STRING,
   s_rec_start_date STRING,
   s_rec_end_date STRING,
   s_closed_date_sk INT,
   s_store_name STRING,
   s_number_employees INT,
   s_floor_space INT,
   s_hours STRING,
   s_manager STRING,
   s_market_id INT,
   s_geography_class STRING,
   s_market_desc STRING,
   s_market_manager STRING,
   s_division_id INT,
   s_division_name STRING,
   s_company_id INT,
   s_company_name STRING,
   s_street_number STRING,
   s_street_name STRING,
   s_street_type STRING,
   s_suite_number STRING,
   s_city STRING,
   s_county STRING,
   s_state STRING,
   s_zip STRING,
   s_country STRING,
   s_gmt_offset DECIMAL(5,2),
   s_tax_precentage DECIMAL(5,2)
) STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");

DROP TABLE IF EXISTS impala_tpcds_warehouse;
 CREATE TABLE impala_tpcds_warehouse (
   w_warehouse_sk INT,
   w_warehouse_id STRING,
   w_warehouse_name STRING,
   w_warehouse_sq_ft INT,
   w_street_number STRING,
   w_street_name STRING,
   w_street_type STRING,
   w_suite_number STRING,
   w_city STRING,
   w_county STRING,
   w_state STRING,
   w_zip STRING,
   w_country STRING,
   w_gmt_offset DECIMAL(5,2)
) STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");

DROP TABLE IF EXISTS impala_tpcds_web_page;
 CREATE TABLE impala_tpcds_web_page (
   wp_web_page_sk INT,
   wp_web_page_id STRING,
   wp_rec_start_date STRING,
   wp_rec_end_date STRING,
   wp_creation_date_sk INT,
   wp_access_date_sk INT,
   wp_autogen_flag STRING,
   wp_customer_sk INT,
   wp_url STRING,
   wp_type STRING,
   wp_char_count INT,
   wp_link_count INT,
   wp_image_count INT,
   wp_max_ad_count INT
) STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");

DROP TABLE IF EXISTS impala_tpcds_web_returns;
 CREATE TABLE impala_tpcds_web_returns (
   wr_returned_date_sk INT,
   wr_returned_time_sk INT,
   wr_item_sk BIGINT,
   wr_refunded_customer_sk INT,
   wr_refunded_cdemo_sk INT,
   wr_refunded_hdemo_sk INT,
   wr_refunded_addr_sk INT,
   wr_returning_customer_sk INT,
   wr_returning_cdemo_sk INT,
   wr_returning_hdemo_sk INT,
   wr_returning_addr_sk INT,
   wr_web_page_sk INT,
   wr_reason_sk INT,
   wr_order_number BIGINT,
   wr_return_quantity INT,
   wr_return_amt DECIMAL(7,2),
   wr_return_tax DECIMAL(7,2),
   wr_return_amt_inc_tax DECIMAL(7,2),
   wr_fee DECIMAL(7,2),
   wr_return_ship_cost DECIMAL(7,2),
   wr_refunded_cash DECIMAL(7,2),
   wr_reversed_charge DECIMAL(7,2),
   wr_account_credit DECIMAL(7,2),
   wr_net_loss DECIMAL(7,2)
) STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");

DROP TABLE IF EXISTS impala_tpcds_web_sales;
 CREATE TABLE impala_tpcds_web_sales (
   `ws_sold_date_sk` int,
   `ws_sold_time_sk` int,
   `ws_ship_date_sk` int,
   `ws_item_sk` bigint,
   `ws_bill_customer_sk` int,
   `ws_bill_cdemo_sk` int,
   `ws_bill_hdemo_sk` int,
   `ws_bill_addr_sk` int,
   `ws_ship_customer_sk` int,
   `ws_ship_cdemo_sk` int,
   `ws_ship_hdemo_sk` int,
   `ws_ship_addr_sk` int,
   `ws_web_page_sk` int,
   `ws_web_site_sk` int,
   `ws_ship_mode_sk` int,
   `ws_warehouse_sk` int,
   `ws_promo_sk` int,
   `ws_order_number` bigint,
   `ws_quantity` int,
   `ws_wholesale_cost` decimal(7,2),
   `ws_list_price` decimal(7,2),
   `ws_sales_price` decimal(7,2),
   `ws_ext_discount_amt` decimal(7,2),
   `ws_ext_sales_price` decimal(7,2),
   `ws_ext_wholesale_cost` decimal(7,2),
   `ws_ext_list_price` decimal(7,2),
   `ws_ext_tax` decimal(7,2),
   `ws_coupon_amt` decimal(7,2),
   `ws_ext_ship_cost` decimal(7,2),
   `ws_net_paid` decimal(7,2),
   `ws_net_paid_inc_tax` decimal(7,2),
   `ws_net_paid_inc_ship` decimal(7,2),
   `ws_net_paid_inc_ship_tax` decimal(7,2),
   `ws_net_profit` decimal(7,2)
) STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");

DROP TABLE IF EXISTS impala_tpcds_web_site;
 CREATE TABLE impala_tpcds_web_site (
   web_site_sk INT,
   web_site_id STRING,
   web_rec_start_date STRING,
   web_rec_end_date STRING,
   web_name STRING,
   web_open_date_sk INT,
   web_close_date_sk INT,
   web_class STRING,
   web_manager STRING,
   web_mkt_id INT,
   web_mkt_class STRING,
   web_mkt_desc STRING,
   web_market_manager STRING,
   web_company_id INT,
   web_company_name STRING,
   web_street_number STRING,
   web_street_name STRING,
   web_street_type STRING,
   web_suite_number STRING,
   web_city STRING,
   web_county STRING,
   web_state STRING,
   web_zip STRING,
   web_country STRING,
   web_gmt_offset DECIMAL(5,2),
   web_tax_percentage DECIMAL(5,2)
) STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");

DROP TABLE IF EXISTS impala_tpcds_time_dim;
CREATE TABLE impala_tpcds_time_dim (
  t_time_sk INT,
  t_time_id STRING,
  t_time INT,
  t_hour INT,
  t_minute INT,
  t_second INT,
  t_am_pm STRING,
  t_shift STRING,
  t_sub_shift STRING,
  t_meal_time STRING
) STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");

DROP TABLE IF EXISTS impala_tpcds_ship_mode;
CREATE TABLE impala_tpcds_ship_mode (
   sm_ship_mode_sk INT,
   sm_ship_mode_id STRING,
   sm_type STRING,
   sm_code STRING,
   sm_carrier STRING,
   sm_contract STRING
) STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");

DROP TABLE IF EXISTS impala_tpcds_reason;
CREATE TABLE impala_tpcds_reason (
  r_reason_sk BIGINT,
  r_reason_id STRING,
  r_reason_desc STRING
) STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");

DROP TABLE IF EXISTS impala_hive_tpcds_catalog_sales;
CREATE TABLE impala_hive_tpcds_catalog_sales (
   `cs_sold_date_sk` int,
   `cs_sold_time_sk` int,
   `cs_ship_date_sk` int,
   `cs_bill_customer_sk` int,
   `cs_bill_cdemo_sk` int,
   `cs_bill_hdemo_sk` int,
   `cs_bill_addr_sk` int,
   `cs_ship_customer_sk` int,
   `cs_ship_cdemo_sk` int,
   `cs_ship_hdemo_sk` int,
   `cs_ship_addr_sk` int,
   `cs_call_center_sk` int,
   `cs_catalog_page_sk` int,
   `cs_ship_mode_sk` int,
   `cs_warehouse_sk` int,
   `cs_item_sk` bigint,
   `cs_promo_sk` int,
   `cs_order_number` bigint,
   `cs_quantity` int,
   `cs_wholesale_cost` decimal(7,2),
   `cs_list_price` decimal(7,2),
   `cs_sales_price` decimal(7,2),
   `cs_ext_discount_amt` decimal(7,2),
   `cs_ext_sales_price` decimal(7,2),
   `cs_ext_wholesale_cost` decimal(7,2),
   `cs_ext_list_price` decimal(7,2),
   `cs_ext_tax` decimal(7,2),
   `cs_coupon_amt` decimal(7,2),
   `cs_ext_ship_cost` decimal(7,2),
   `cs_net_paid` decimal(7,2),
   `cs_net_paid_inc_tax` decimal(7,2),
   `cs_net_paid_inc_ship` decimal(7,2),
   `cs_net_paid_inc_ship_tax` decimal(7,2),
   `cs_net_profit` decimal(7,2)
) STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");

DROP TABLE IF EXISTS impala_hive_tpcds_store_sales;
 CREATE TABLE impala_hive_tpcds_store_sales (
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
PARTITIONED BY (
   `ss_sold_date_sk` int
) STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");

DROP TABLE IF EXISTS impala_hive_tpcds_web_sales;
 CREATE TABLE impala_hive_tpcds_web_sales (
   `ws_sold_date_sk` int,
   `ws_sold_time_sk` int,
   `ws_ship_date_sk` int,
   `ws_item_sk` bigint,
   `ws_bill_customer_sk` int,
   `ws_bill_cdemo_sk` int,
   `ws_bill_hdemo_sk` int,
   `ws_bill_addr_sk` int,
   `ws_ship_customer_sk` int,
   `ws_ship_cdemo_sk` int,
   `ws_ship_hdemo_sk` int,
   `ws_ship_addr_sk` int,
   `ws_web_page_sk` int,
   `ws_web_site_sk` int,
   `ws_ship_mode_sk` int,
   `ws_warehouse_sk` int,
   `ws_promo_sk` int,
   `ws_order_number` bigint,
   `ws_quantity` int,
   `ws_wholesale_cost` decimal(7,2),
   `ws_list_price` decimal(7,2),
   `ws_sales_price` decimal(7,2),
   `ws_ext_discount_amt` decimal(7,2),
   `ws_ext_sales_price` decimal(7,2),
   `ws_ext_wholesale_cost` decimal(7,2),
   `ws_ext_list_price` decimal(7,2),
   `ws_ext_tax` decimal(7,2),
   `ws_coupon_amt` decimal(7,2),
   `ws_ext_ship_cost` decimal(7,2),
   `ws_net_paid` decimal(7,2),
   `ws_net_paid_inc_tax` decimal(7,2),
   `ws_net_paid_inc_ship` decimal(7,2),
   `ws_net_paid_inc_ship_tax` decimal(7,2),
   `ws_net_profit` decimal(7,2)
) STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");

DROP TABLE IF EXISTS impala_hive_tpcds_catalog_returns;
CREATE TABLE impala_hive_tpcds_catalog_returns (
  `cr_returned_date_sk` int,
  `cr_returned_time_sk` int,
  `cr_item_sk` bigint,
  `cr_refunded_customer_sk` int,
  `cr_refunded_cdemo_sk` int,
  `cr_refunded_hdemo_sk` int,
  `cr_refunded_addr_sk` int,
  `cr_returning_customer_sk` int,
  `cr_returning_cdemo_sk` int,
  `cr_returning_hdemo_sk` int,
  `cr_returning_addr_sk` int,
  `cr_call_center_sk` int,
  `cr_catalog_page_sk` int,
  `cr_ship_mode_sk` int,
  `cr_warehouse_sk` int,
  `cr_reason_sk` int,
  `cr_order_number` bigint,
  `cr_return_quantity` int,
  `cr_return_amount` decimal(7,2),
  `cr_return_tax` decimal(7,2),
  `cr_return_amt_inc_tax` decimal(7,2),
  `cr_fee` decimal(7,2),
  `cr_return_ship_cost` decimal(7,2),
  `cr_refunded_cash` decimal(7,2),
  `cr_reversed_charge` decimal(7,2),
  `cr_store_credit` decimal(7,2),
  `cr_net_loss` decimal(7,2)
) STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");

DROP TABLE IF EXISTS impala_hive_tpcds_store_returns;
 CREATE TABLE impala_hive_tpcds_store_returns (
   sr_returned_date_sk INT,
   sr_return_time_sk INT,
   sr_item_sk BIGINT,
   sr_customer_sk INT,
   sr_cdemo_sk INT,
   sr_hdemo_sk INT,
   sr_addr_sk INT,
   sr_store_sk INT,
   sr_reason_sk INT,
   sr_ticket_number BIGINT,
   sr_return_quantity INT,
   sr_return_amt DECIMAL(7,2),
   sr_return_tax DECIMAL(7,2),
   sr_return_amt_inc_tax DECIMAL(7,2),
   sr_fee DECIMAL(7,2),
   sr_return_ship_cost DECIMAL(7,2),
   sr_refunded_cash DECIMAL(7,2),
   sr_reversed_charge DECIMAL(7,2),
   sr_store_credit DECIMAL(7,2),
   sr_net_loss DECIMAL(7,2)
) STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");

DROP TABLE IF EXISTS impala_hive_tpcds_web_returns;
 CREATE TABLE impala_hive_tpcds_web_returns (
   wr_returned_date_sk INT,
   wr_returned_time_sk INT,
   wr_item_sk BIGINT,
   wr_refunded_customer_sk INT,
   wr_refunded_cdemo_sk INT,
   wr_refunded_hdemo_sk INT,
   wr_refunded_addr_sk INT,
   wr_returning_customer_sk INT,
   wr_returning_cdemo_sk INT,
   wr_returning_hdemo_sk INT,
   wr_returning_addr_sk INT,
   wr_web_page_sk INT,
   wr_reason_sk INT,
   wr_order_number BIGINT,
   wr_return_quantity INT,
   wr_return_amt DECIMAL(7,2),
   wr_return_tax DECIMAL(7,2),
   wr_return_amt_inc_tax DECIMAL(7,2),
   wr_fee DECIMAL(7,2),
   wr_return_ship_cost DECIMAL(7,2),
   wr_refunded_cash DECIMAL(7,2),
   wr_reversed_charge DECIMAL(7,2),
   wr_account_credit DECIMAL(7,2),
   wr_net_loss DECIMAL(7,2)
) STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");

DROP TABLE IF EXISTS impala_hive_tpcds_date_dim;
 CREATE TABLE impala_hive_tpcds_date_dim (
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
   `d_current_year` string
) STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");

DROP TABLE IF EXISTS impala_hive_tpcds_store;
 CREATE TABLE impala_hive_tpcds_store (
   s_store_sk INT,
   s_store_id STRING,
   s_rec_start_date STRING,
   s_rec_end_date STRING,
   s_closed_date_sk INT,
   s_store_name STRING,
   s_number_employees INT,
   s_floor_space INT,
   s_hours STRING,
   s_manager STRING,
   s_market_id INT,
   s_geography_class STRING,
   s_market_desc STRING,
   s_market_manager STRING,
   s_division_id INT,
   s_division_name STRING,
   s_company_id INT,
   s_company_name STRING,
   s_street_number STRING,
   s_street_name STRING,
   s_street_type STRING,
   s_suite_number STRING,
   s_city STRING,
   s_county STRING,
   s_state STRING,
   s_zip STRING,
   s_country STRING,
   s_gmt_offset DECIMAL(5,2),
   s_tax_precentage DECIMAL(5,2)
) STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");

DROP TABLE IF EXISTS impala_hive_tpcds_catalog_page;
CREATE TABLE impala_hive_tpcds_catalog_page (
  cp_catalog_page_sk INT,
  cp_catalog_page_id STRING,
  cp_start_date_sk INT,
  cp_end_date_sk INT,
  cp_department STRING,
  cp_catalog_number INT,
  cp_catalog_page_number INT,
  cp_description STRING,
  cp_type STRING
) STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");

DROP TABLE IF EXISTS impala_hive_tpcds_web_site;
 CREATE TABLE impala_hive_tpcds_web_site (
   web_site_sk INT,
   web_site_id STRING,
   web_rec_start_date STRING,
   web_rec_end_date STRING,
   web_name STRING,
   web_open_date_sk INT,
   web_close_date_sk INT,
   web_class STRING,
   web_manager STRING,
   web_mkt_id INT,
   web_mkt_class STRING,
   web_mkt_desc STRING,
   web_market_manager STRING,
   web_company_id INT,
   web_company_name STRING,
   web_street_number STRING,
   web_street_name STRING,
   web_street_type STRING,
   web_suite_number STRING,
   web_city STRING,
   web_county STRING,
   web_state STRING,
   web_zip STRING,
   web_country STRING,
   web_gmt_offset DECIMAL(5,2),
   web_tax_percentage DECIMAL(5,2)
) STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");

DROP TABLE IF EXISTS impala_hive_tpcds_item;
 CREATE TABLE impala_hive_tpcds_item (
   i_item_sk BIGINT,
   i_item_id STRING,
   i_rec_start_date STRING,
   i_rec_end_date STRING,
   i_item_desc STRING,
   i_current_price DECIMAL(7,2),
   i_wholesale_cost DECIMAL(7,2),
   i_brand_id INT,
   i_brand STRING,
   i_class_id INT,
   i_class STRING,
   i_category_id INT,
   i_category STRING,
   i_manufact_id INT,
   i_manufact STRING,
   i_size STRING,
   i_formulation STRING,
   i_color STRING,
   i_units STRING,
   i_container STRING,
   i_manager_id INT,
   i_product_name STRING
) STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");

DROP TABLE IF EXISTS impala_hive_tpcds_promotion;
 CREATE TABLE impala_hive_tpcds_promotion (
   `p_promo_sk` int,
   `p_promo_id` string,
   `p_start_date_sk` int,
   `p_end_date_sk` int,
   `p_item_sk` bigint,
   `p_cost` decimal(15,2),
   `p_response_target` int,
   `p_promo_name` string,
   `p_channel_dmail` string,
   `p_channel_email` string,
   `p_channel_catalog` string,
   `p_channel_tv` string,
   `p_channel_radio` string,
   `p_channel_press` string,
   `p_channel_event` string,
   `p_channel_demo` string,
   `p_channel_details` string,
   `p_purpose` string,
   `p_discount_active` string
) STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");

DROP TABLE IF EXISTS impala_flights;
CREATE TABLE impala_flights (
  Month int,
  DayofMonth int,
  DayOfWeek int,
  DepTime bigint,
  CRSDepTime bigint,
  ArrTime bigint,
  CRSArrTime bigint,
  UniqueCarrier string,
  FlightNum int,
  TailNum string,
  ActualElapsedTime bigint,
  CRSElapsedTime bigint,
  AirTime bigint,
  ArrDelay bigint,
  DepDelay bigint,
  Origin string,
  Dest string,
  Distance bigint,
  TaxiIn bigint,
  TaxiOut bigint,
  Cancelled int,
  CancellationCode varchar(1),
  Diverted varchar(1),
  CarrierDelay bigint,
  WeatherDelay bigint,
  NASDelay bigint,
  SecurityDelay bigint,
  LateAircraftDelay bigint
)
PARTITIONED BY (Year int)
STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");

DROP TABLE IF EXISTS impala_airports;
CREATE TABLE impala_airports (
	iata string,
	airport string,
	city string,
	state string,
	country string,
	lat double,
	lon double
)
STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");

DROP TABLE IF EXISTS impala_airlines;
CREATE TABLE impala_airlines (
	code string,
	description string
)
STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");

DROP TABLE IF EXISTS impala_planes;
CREATE TABLE impala_planes (
	tailnum string,
	owner_type string,
	manufacturer string,
	issue_date string,
	model string,
	status string,
	aircraft_type string,
	engine_type string,
	year int
)
STORED AS PARQUET
TBLPROPERTIES ("transactional"="false");

-- Primary keys
ALTER TABLE impala_airports
  ADD CONSTRAINT airline_ontime_parquet_pk_airports
  PRIMARY KEY (iata) DISABLE RELY;
ALTER TABLE impala_airlines
  ADD CONSTRAINT airline_ontime_parquet_pk_airlines
  PRIMARY KEY (code) DISABLE RELY;
ALTER TABLE impala_planes
  ADD CONSTRAINT airline_ontime_parquet_pk_planes
  PRIMARY KEY (tailnum) DISABLE RELY;

-- Not null
ALTER TABLE impala_flights
  CHANGE COLUMN origin origin STRING
  CONSTRAINT airline_ontime_parquet_nn_flights_origin NOT NULL DISABLE RELY;
ALTER TABLE impala_flights
  CHANGE COLUMN dest dest STRING
  CONSTRAINT airline_ontime_parquet_nn_flights_dest NOT NULL DISABLE RELY;
ALTER TABLE impala_flights
  CHANGE COLUMN uniquecarrier uniquecarrier STRING
  CONSTRAINT airline_ontime_parquet_nn_flights_uniquecarrier NOT NULL DISABLE RELY;
ALTER TABLE impala_flights
  CHANGE COLUMN tailnum tailnum STRING
  CONSTRAINT airline_ontime_parquet_nn_flights_tailnum NOT NULL DISABLE RELY;

-- Foreign keys
ALTER TABLE impala_flights
  ADD CONSTRAINT airline_ontime_parquet_flights_airports1
  FOREIGN KEY (origin) REFERENCES impala_airports (iata) DISABLE RELY;
ALTER TABLE impala_flights
  ADD CONSTRAINT airline_ontime_parquet_flights_airports2
  FOREIGN KEY (dest) REFERENCES impala_airports (iata) DISABLE RELY;
ALTER TABLE impala_flights
  ADD CONSTRAINT airline_ontime_parquet_flights_airlines
  FOREIGN KEY (uniquecarrier) REFERENCES impala_airlines (code) DISABLE RELY;
ALTER TABLE impala_flights
  ADD CONSTRAINT airline_ontime_parquet_flights_planes
  FOREIGN KEY (tailnum) REFERENCES impala_planes (tailnum) DISABLE RELY;

