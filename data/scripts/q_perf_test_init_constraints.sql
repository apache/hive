drop table if exists call_center;
create table call_center
(
    cc_call_center_sk        int,
    cc_call_center_id        string,
    cc_rec_start_date        string,
    cc_rec_end_date          string,
    cc_closed_date_sk        int,
    cc_open_date_sk          int,
    cc_name                  string,
    cc_class                 string,
    cc_employees             int,
    cc_sq_ft                 int,
    cc_hours                 string,
    cc_manager               string,
    cc_mkt_id                int,
    cc_mkt_class             string,
    cc_mkt_desc              string,
    cc_market_manager        string,
    cc_division              int,
    cc_division_name         string,
    cc_company               int,
    cc_company_name          string,
    cc_street_number         string,
    cc_street_name           string,
    cc_street_type           string,
    cc_suite_number          string,
    cc_city                  string,
    cc_county                string,
    cc_state                 string,
    cc_zip                   string,
    cc_country               string,
    cc_gmt_offset            decimal(5,2),
    cc_tax_percentage        decimal(5,2)
)
row format delimited fields terminated by '\t'
STORED AS ORC tblproperties ("transactional"="true", "orc.compress"="ZLIB");

drop table if exists catalog_page;
create table catalog_page
(
    cp_catalog_page_sk       int,
    cp_catalog_page_id       string,
    cp_start_date_sk         int,
    cp_end_date_sk           int,
    cp_department            string,
    cp_catalog_number        int,
    cp_catalog_page_number   int,
    cp_description           string,
    cp_type                  string
)
row format delimited fields terminated by '\t'
STORED AS ORC tblproperties ("transactional"="true", "orc.compress"="ZLIB");


drop table if exists catalog_returns;
create table catalog_returns
(
    cr_returned_date_sk        int,
    cr_returned_time_sk        int,
    cr_item_sk                 int,
    cr_refunded_customer_sk    int,
    cr_refunded_cdemo_sk       int,
    cr_refunded_hdemo_sk       int,
    cr_refunded_addr_sk        int,
    cr_returning_customer_sk   int,
    cr_returning_cdemo_sk      int,
    cr_returning_hdemo_sk      int,
    cr_returning_addr_sk       int,
    cr_call_center_sk          int,
    cr_catalog_page_sk         int,
    cr_ship_mode_sk            int,
    cr_warehouse_sk            int,
    cr_reason_sk               int,
    cr_order_number            int,
    cr_return_quantity         int,
    cr_return_amount           decimal(7,2),
    cr_return_tax              decimal(7,2),
    cr_return_amt_inc_tax      decimal(7,2),
    cr_fee                     decimal(7,2),
    cr_return_ship_cost        decimal(7,2),
    cr_refunded_cash           decimal(7,2),
    cr_reversed_charge         decimal(7,2),
    cr_store_credit            decimal(7,2),
    cr_net_loss                decimal(7,2)
)
row format delimited fields terminated by '\t'
STORED AS ORC tblproperties ("transactional"="true", "orc.compress"="ZLIB");


drop table if exists catalog_sales;
create table catalog_sales
(
    cs_sold_date_sk            int,
    cs_sold_time_sk            int,
    cs_ship_date_sk            int,
    cs_bill_customer_sk        int,
    cs_bill_cdemo_sk           int,
    cs_bill_hdemo_sk           int,
    cs_bill_addr_sk            int,
    cs_ship_customer_sk        int,
    cs_ship_cdemo_sk           int,
    cs_ship_hdemo_sk           int,
    cs_ship_addr_sk            int,
    cs_call_center_sk          int,
    cs_catalog_page_sk         int,
    cs_ship_mode_sk            int,
    cs_warehouse_sk            int,
    cs_item_sk                 int,
    cs_promo_sk                int,
    cs_order_number            int,
    cs_quantity                int,
    cs_wholesale_cost          decimal(7,2),
    cs_list_price              decimal(7,2),
    cs_sales_price             decimal(7,2),
    cs_ext_discount_amt        decimal(7,2),
    cs_ext_sales_price         decimal(7,2),
    cs_ext_wholesale_cost      decimal(7,2),
    cs_ext_list_price          decimal(7,2),
    cs_ext_tax                 decimal(7,2),
    cs_coupon_amt              decimal(7,2),
    cs_ext_ship_cost           decimal(7,2),
    cs_net_paid                decimal(7,2),
    cs_net_paid_inc_tax        decimal(7,2),
    cs_net_paid_inc_ship       decimal(7,2),
    cs_net_paid_inc_ship_tax   decimal(7,2),
    cs_net_profit              decimal(7,2)
)
row format delimited fields terminated by '\t'
STORED AS ORC tblproperties ("transactional"="true", "orc.compress"="ZLIB");


drop table if exists customer;
create table customer
(
    c_customer_sk             int,
    c_customer_id             string,
    c_current_cdemo_sk        int,
    c_current_hdemo_sk        int,
    c_current_addr_sk         int,
    c_first_shipto_date_sk    int,
    c_first_sales_date_sk     int,
    c_salutation              string,
    c_first_name              string,
    c_last_name               string,
    c_preferred_cust_flag     string,
    c_birth_day               int,
    c_birth_month             int,
    c_birth_year              int,
    c_birth_country           string,
    c_login                   string,
    c_email_address           string,
    c_last_review_date        string
)
row format delimited fields terminated by '\t'
STORED AS ORC tblproperties ("transactional"="true", "orc.compress"="ZLIB");


drop table if exists customer_address;
create table customer_address
(
    ca_address_sk             int,
    ca_address_id             string,
    ca_street_number          string,
    ca_street_name            string,
    ca_street_type            string,
    ca_suite_number           string,
    ca_city                   string,
    ca_county                 string,
    ca_state                  string,
    ca_zip                    string,
    ca_country                string,
    ca_gmt_offset             decimal(5,2),
    ca_location_type          string
)
row format delimited fields terminated by '\t'
STORED AS ORC tblproperties ("transactional"="true", "orc.compress"="ZLIB");


drop table if exists customer_demographics;
create table customer_demographics
(
    cd_demo_sk                int,
    cd_gender                 string,
    cd_marital_status         string,
    cd_education_status       string,
    cd_purchase_estimate      int,
    cd_credit_rating          string,
    cd_dep_count              int,
    cd_dep_employed_count     int,
    cd_dep_college_count      int 
)
row format delimited fields terminated by '\t'
STORED AS ORC tblproperties ("transactional"="true", "orc.compress"="ZLIB");


drop table if exists date_dim;
create table date_dim
(
    d_date_sk                 int,
    d_date_id                 string,
    d_date                    string,
    d_month_seq               int,
    d_week_seq                int,
    d_quarter_seq             int,
    d_year                    int,
    d_dow                     int,
    d_moy                     int,
    d_dom                     int,
    d_qoy                     int,
    d_fy_year                 int,
    d_fy_quarter_seq          int,
    d_fy_week_seq             int,
    d_day_name                string,
    d_quarter_name            string,
    d_holiday                 string,
    d_weekend                 string,
    d_following_holiday       string,
    d_first_dom               int,
    d_last_dom                int,
    d_same_day_ly             int,
    d_same_day_lq             int,
    d_current_day             string,
    d_current_week            string,
    d_current_month           string,
    d_current_quarter         string,
    d_current_year            string 
)
row format delimited fields terminated by '\t'
STORED AS ORC tblproperties ("transactional"="true", "orc.compress"="ZLIB");


drop table if exists household_demographics;
create table household_demographics
(
    hd_demo_sk                int,
    hd_income_band_sk         int,
    hd_buy_potential          string,
    hd_dep_count              int,
    hd_vehicle_count          int
)
row format delimited fields terminated by '\t'
STORED AS ORC tblproperties ("transactional"="true", "orc.compress"="ZLIB");


drop table if exists income_band;
create table income_band
(
    ib_income_band_sk        int,
    ib_lower_bound           int,
    ib_upper_bound           int
)
row format delimited fields terminated by '\t'
STORED AS ORC tblproperties ("transactional"="true", "orc.compress"="ZLIB");


drop table if exists inventory;
create table inventory
(
    inv_date_sk                int,
    inv_item_sk                int,
    inv_warehouse_sk           int,
    inv_quantity_on_hand       int
)
row format delimited fields terminated by '\t'
STORED AS ORC tblproperties ("transactional"="true", "orc.compress"="ZLIB");


drop table if exists item;
create table item
(
    i_item_sk                 int,
    i_item_id                 string,
    i_rec_start_date          string,
    i_rec_end_date            string,
    i_item_desc               string,
    i_current_price           decimal(7,2),
    i_wholesale_cost          decimal(7,2),
    i_brand_id                int,
    i_brand                   string,
    i_class_id                int,
    i_class                   string,
    i_category_id             int,
    i_category                string,
    i_manufact_id             int,
    i_manufact                string,
    i_size                    string,
    i_formulation             string,
    i_color                   string,
    i_units                   string,
    i_container               string,
    i_manager_id              int,
    i_product_name            string
)
row format delimited fields terminated by '\t'
STORED AS ORC tblproperties ("transactional"="true", "orc.compress"="ZLIB");


drop table if exists promotion;
create table promotion
(
    p_promo_sk                int,
    p_promo_id                string,
    p_start_date_sk           int,
    p_end_date_sk             int,
    p_item_sk                 int,
    p_cost                    decimal(15,2),
    p_response_target         int,
    p_promo_name              string,
    p_channel_dmail           string,
    p_channel_email           string,
    p_channel_catalog         string,
    p_channel_tv              string,
    p_channel_radio           string,
    p_channel_press           string,
    p_channel_event           string,
    p_channel_demo            string,
    p_channel_details         string,
    p_purpose                 string,
    p_discount_active         string 
)
row format delimited fields terminated by '\t'
STORED AS ORC tblproperties ("transactional"="true", "orc.compress"="ZLIB");


drop table if exists reason;
create table reason
(
    r_reason_sk         int,
    r_reason_id         string,
    r_reason_desc       string
)
row format delimited fields terminated by '\t'
STORED AS ORC tblproperties ("transactional"="true", "orc.compress"="ZLIB");


drop table if exists ship_mode;
create table ship_mode
(
    sm_ship_mode_sk           int,
    sm_ship_mode_id           string,
    sm_type                   string,
    sm_code                   string,
    sm_carrier                string,
    sm_contract               string
)
row format delimited fields terminated by '\t'
STORED AS ORC tblproperties ("transactional"="true", "orc.compress"="ZLIB");


drop table if exists store;
create table store
(
    s_store_sk                int,
    s_store_id                string,
    s_rec_start_date          string,
    s_rec_end_date            string,
    s_closed_date_sk          int,
    s_store_name              string,
    s_number_employees        int,
    s_floor_space             int,
    s_hours                   string,
    s_manager                 string,
    s_market_id               int,
    s_geography_class         string,
    s_market_desc             string,
    s_market_manager          string,
    s_division_id             int,
    s_division_name           string,
    s_company_id              int,
    s_company_name            string,
    s_street_number           string,
    s_street_name             string,
    s_street_type             string,
    s_suite_number            string,
    s_city                    string,
    s_county                  string,
    s_state                   string,
    s_zip                     string,
    s_country                 string,
    s_gmt_offset              decimal(5,2),
    s_tax_precentage          decimal(5,2)
)
row format delimited fields terminated by '\t'
STORED AS ORC tblproperties ("transactional"="true", "orc.compress"="ZLIB");


drop table if exists store_returns;
create table store_returns
(
    sr_returned_date_sk      int,
    sr_return_time_sk        int,
    sr_item_sk               int,
    sr_customer_sk           int,
    sr_cdemo_sk              int,
    sr_hdemo_sk              int,
    sr_addr_sk               int,
    sr_store_sk              int,
    sr_reason_sk             int,
    sr_ticket_number         int,
    sr_return_quantity       int,
    sr_return_amt            decimal(7,2),
    sr_return_tax            decimal(7,2),
    sr_return_amt_inc_tax    decimal(7,2),
    sr_fee                   decimal(7,2),
    sr_return_ship_cost      decimal(7,2),
    sr_refunded_cash         decimal(7,2),
    sr_reversed_charge       decimal(7,2),
    sr_store_credit          decimal(7,2),
    sr_net_loss              decimal(7,2)
)
row format delimited fields terminated by '\t'
STORED AS ORC tblproperties ("transactional"="true", "orc.compress"="ZLIB");


drop table if exists store_sales;
create table store_sales
(
    ss_sold_date_sk           int,
    ss_sold_time_sk           int,
    ss_item_sk                int,
    ss_customer_sk            int,
    ss_cdemo_sk               int,
    ss_hdemo_sk               int,
    ss_addr_sk                int,
    ss_store_sk               int,
    ss_promo_sk               int,
    ss_ticket_number          int,
    ss_quantity               int,
    ss_wholesale_cost         decimal(7,2),
    ss_list_price             decimal(7,2),
    ss_sales_price            decimal(7,2),
    ss_ext_discount_amt       decimal(7,2),
    ss_ext_sales_price        decimal(7,2),
    ss_ext_wholesale_cost     decimal(7,2),
    ss_ext_list_price         decimal(7,2),
    ss_ext_tax                decimal(7,2),
    ss_coupon_amt             decimal(7,2),
    ss_net_paid               decimal(7,2),
    ss_net_paid_inc_tax       decimal(7,2),
    ss_net_profit             decimal(7,2)
)
row format delimited fields terminated by '\t'
STORED AS ORC tblproperties ("transactional"="true", "orc.compress"="ZLIB");


drop table if exists time_dim;
create table time_dim
(
    t_time_sk                 int,
    t_time_id                 string,
    t_time                    int,
    t_hour                    int,
    t_minute                  int,
    t_second                  int,
    t_am_pm                   string,
    t_shift                   string,
    t_sub_shift               string,
    t_meal_time               string
)
row format delimited fields terminated by '\t'
STORED AS ORC tblproperties ("transactional"="true", "orc.compress"="ZLIB");


drop table if exists warehouse;
create table warehouse
(
    w_warehouse_sk            int,
    w_warehouse_id            string,
    w_warehouse_name          string,
    w_warehouse_sq_ft         int,
    w_street_number           string,
    w_street_name             string,
    w_street_type             string,
    w_suite_number            string,
    w_city                    string,
    w_county                  string,
    w_state                   string,
    w_zip                     string,
    w_country                 string,
    w_gmt_offset              decimal(5,2)
)
row format delimited fields terminated by '\t'
STORED AS ORC tblproperties ("transactional"="true", "orc.compress"="ZLIB");


drop table if exists web_page;
create table web_page
(
    wp_web_page_sk           int,
    wp_web_page_id           string,
    wp_rec_start_date        string,
    wp_rec_end_date          string,
    wp_creation_date_sk      int,
    wp_access_date_sk        int,
    wp_autogen_flag          string,
    wp_customer_sk           int,
    wp_url                   string,
    wp_type                  string,
    wp_char_count            int,
    wp_link_count            int,
    wp_image_count           int,
    wp_max_ad_count          int
)
row format delimited fields terminated by '\t'
STORED AS ORC tblproperties ("transactional"="true", "orc.compress"="ZLIB");


drop table if exists web_returns;
create table web_returns
(
    wr_returned_date_sk        int,
    wr_returned_time_sk        int,
    wr_item_sk                 int,
    wr_refunded_customer_sk    int,
    wr_refunded_cdemo_sk       int,
    wr_refunded_hdemo_sk       int,
    wr_refunded_addr_sk        int,
    wr_returning_customer_sk   int,
    wr_returning_cdemo_sk      int,
    wr_returning_hdemo_sk      int,
    wr_returning_addr_sk       int,
    wr_web_page_sk             int,
    wr_reason_sk               int,
    wr_order_number            int,
    wr_return_quantity         int,
    wr_return_amt              decimal(7,2),
    wr_return_tax              decimal(7,2),
    wr_return_amt_inc_tax      decimal(7,2),
    wr_fee                     decimal(7,2),
    wr_return_ship_cost        decimal(7,2),
    wr_refunded_cash           decimal(7,2),
    wr_reversed_charge         decimal(7,2),
    wr_account_credit          decimal(7,2),
    wr_net_loss                decimal(7,2)
)
row format delimited fields terminated by '\t'
STORED AS ORC tblproperties ("transactional"="true", "orc.compress"="ZLIB");


drop table if exists web_sales;
create table web_sales
(
    ws_sold_date_sk           int,
    ws_sold_time_sk           int,
    ws_ship_date_sk           int,
    ws_item_sk                int,
    ws_bill_customer_sk       int,
    ws_bill_cdemo_sk          int,
    ws_bill_hdemo_sk          int,
    ws_bill_addr_sk           int,
    ws_ship_customer_sk       int,
    ws_ship_cdemo_sk          int,
    ws_ship_hdemo_sk          int,
    ws_ship_addr_sk           int,
    ws_web_page_sk            int,
    ws_web_site_sk            int,
    ws_ship_mode_sk           int,
    ws_warehouse_sk           int,
    ws_promo_sk               int,
    ws_order_number           int,
    ws_quantity               int,
    ws_wholesale_cost         decimal(7,2),
    ws_list_price             decimal(7,2),
    ws_sales_price            decimal(7,2),
    ws_ext_discount_amt       decimal(7,2),
    ws_ext_sales_price        decimal(7,2),
    ws_ext_wholesale_cost     decimal(7,2),
    ws_ext_list_price         decimal(7,2),
    ws_ext_tax                decimal(7,2),
    ws_coupon_amt             decimal(7,2),
    ws_ext_ship_cost          decimal(7,2),
    ws_net_paid               decimal(7,2),
    ws_net_paid_inc_tax       decimal(7,2),
    ws_net_paid_inc_ship      decimal(7,2),
    ws_net_paid_inc_ship_tax  decimal(7,2),
    ws_net_profit             decimal(7,2)
)
row format delimited fields terminated by '\t'
STORED AS ORC tblproperties ("transactional"="true", "orc.compress"="ZLIB");


drop table if exists web_site;
create table web_site
(
    web_site_sk             int,
    web_site_id             string,
    web_rec_start_date      string,
    web_rec_end_date        string,
    web_name                string,
    web_open_date_sk        int,
    web_close_date_sk       int,
    web_class               string,
    web_manager             string,
    web_mkt_id              int,
    web_mkt_class           string,
    web_mkt_desc            string,
    web_market_manager      string,
    web_company_id          int,
    web_company_name        string,
    web_street_number       string,
    web_street_name         string,
    web_street_type         string,
    web_suite_number        string,
    web_city                string,
    web_county              string,
    web_state               string,
    web_zip                 string,
    web_country             string,
    web_gmt_offset          decimal(5,2),
    web_tax_percentage      decimal(5,2)
)
row format delimited fields terminated by '\t'
STORED AS ORC tblproperties ("transactional"="true", "orc.compress"="ZLIB");


-- CONSTRAINTS
alter table customer_address add constraint pk_ca primary key (ca_address_sk) disable novalidate rely;
alter table customer_demographics add constraint pk_cd primary key (cd_demo_sk) disable novalidate rely;
alter table date_dim add constraint pk_dd primary key (d_date_sk) disable novalidate rely;
alter table warehouse add constraint pk_w primary key (w_warehouse_sk) disable novalidate rely;
alter table ship_mode add constraint pk_sm primary key (sm_ship_mode_sk) disable novalidate rely;
alter table time_dim add constraint pk_td primary key (t_time_sk) disable novalidate rely;
alter table reason add constraint pk_r primary key (r_reason_sk) disable novalidate rely;
alter table income_band add constraint pk_ib primary key (ib_income_band_sk) disable novalidate rely;
alter table item add constraint pk_i primary key (i_item_sk) disable novalidate rely;
alter table store add constraint pk_s primary key (s_store_sk) disable novalidate rely;
alter table call_center add constraint pk_cc primary key (cc_call_center_sk) disable novalidate rely;
alter table customer add constraint pk_c primary key (c_customer_sk) disable novalidate rely;
alter table web_site add constraint pk_ws primary key (web_site_sk) disable novalidate rely;
alter table store_returns add constraint pk_sr primary key (sr_item_sk, sr_ticket_number) disable novalidate rely;
alter table household_demographics add constraint pk_hd primary key (hd_demo_sk) disable novalidate rely;
alter table web_page add constraint pk_wp primary key (wp_web_page_sk) disable novalidate rely;
alter table promotion add constraint pk_p primary key (p_promo_sk) disable novalidate rely;
alter table catalog_page add constraint pk_cp primary key (cp_catalog_page_sk) disable novalidate rely;
-- partition_col case
alter table inventory add constraint pk_in primary key (inv_date_sk, inv_item_sk, inv_warehouse_sk) disable novalidate rely;
alter table catalog_returns add constraint pk_cr primary key (cr_item_sk, cr_order_number) disable novalidate rely;
alter table web_returns add constraint pk_wr primary key (wr_item_sk, wr_order_number) disable novalidate rely;
alter table web_sales add constraint pk_ws2 primary key (ws_item_sk, ws_order_number) disable novalidate rely;
alter table catalog_sales add constraint pk_cs primary key (cs_item_sk, cs_order_number) disable novalidate rely;
alter table store_sales add constraint pk_ss primary key (ss_item_sk, ss_ticket_number) disable novalidate rely;

alter table call_center add constraint cc_d1 foreign key  (cc_closed_date_sk) references date_dim (d_date_sk) disable novalidate rely;
alter table call_center add constraint cc_d2 foreign key  (cc_open_date_sk) references date_dim (d_date_sk) disable novalidate rely;
alter table catalog_page add constraint cp_d1 foreign key  (cp_end_date_sk) references date_dim (d_date_sk) disable novalidate rely;
alter table catalog_page add constraint cp_d2 foreign key  (cp_start_date_sk) references date_dim (d_date_sk) disable novalidate rely;
alter table catalog_returns add constraint cr_cc foreign key  (cr_call_center_sk) references call_center (cc_call_center_sk) disable novalidate rely;
alter table catalog_returns add constraint cr_cp foreign key  (cr_catalog_page_sk) references catalog_page (cp_catalog_page_sk) disable novalidate rely;
alter table catalog_returns add constraint cr_cs foreign key  (cr_item_sk, cr_order_number) references catalog_sales (cs_item_sk, cs_order_number) disable novalidate rely;
alter table catalog_returns add constraint cr_i foreign key  (cr_item_sk) references item (i_item_sk) disable novalidate rely;
alter table catalog_returns add constraint cr_r foreign key  (cr_reason_sk) references reason (r_reason_sk) disable novalidate rely;
alter table catalog_returns add constraint cr_a1 foreign key  (cr_refunded_addr_sk) references customer_address (ca_address_sk) disable novalidate rely;
alter table catalog_returns add constraint cr_cd1 foreign key  (cr_refunded_cdemo_sk) references customer_demographics (cd_demo_sk) disable novalidate rely;
alter table catalog_returns add constraint cr_c1 foreign key  (cr_refunded_customer_sk) references customer (c_customer_sk) disable novalidate rely;
alter table catalog_returns add constraint cr_hd1 foreign key  (cr_refunded_hdemo_sk) references household_demographics (hd_demo_sk) disable novalidate rely;
-- partition_col case
alter table catalog_returns add constraint cr_d1 foreign key  (cr_returned_date_sk) references date_dim (d_date_sk) disable novalidate rely;
alter table catalog_returns add constraint cr_t foreign key  (cr_returned_time_sk) references time_dim (t_time_sk) disable novalidate rely;
alter table catalog_returns add constraint cr_a2 foreign key  (cr_returning_addr_sk) references customer_address (ca_address_sk) disable novalidate rely;
alter table catalog_returns add constraint cr_cd2 foreign key  (cr_returning_cdemo_sk) references customer_demographics (cd_demo_sk) disable novalidate rely;
alter table catalog_returns add constraint cr_c2 foreign key  (cr_returning_customer_sk) references customer (c_customer_sk) disable novalidate rely;
alter table catalog_returns add constraint cr_hd2 foreign key  (cr_returning_hdemo_sk) references household_demographics (hd_demo_sk) disable novalidate rely;
-- alter table catalog_returns add constraint cr_d2 foreign key  (cr_ship_date_sk) references date_dim (d_date_sk) disable novalidate rely;
alter table catalog_returns add constraint cr_sm foreign key  (cr_ship_mode_sk) references ship_mode (sm_ship_mode_sk) disable novalidate rely;
alter table catalog_returns add constraint cr_w2 foreign key  (cr_warehouse_sk) references warehouse (w_warehouse_sk) disable novalidate rely;
alter table catalog_sales add constraint cs_b_a foreign key  (cs_bill_addr_sk) references customer_address (ca_address_sk) disable novalidate rely;
alter table catalog_sales add constraint cs_b_cd foreign key  (cs_bill_cdemo_sk) references customer_demographics (cd_demo_sk) disable novalidate rely;
alter table catalog_sales add constraint cs_b_c foreign key  (cs_bill_customer_sk) references customer (c_customer_sk) disable novalidate rely;
alter table catalog_sales add constraint cs_b_hd foreign key  (cs_bill_hdemo_sk) references household_demographics (hd_demo_sk) disable novalidate rely;
alter table catalog_sales add constraint cs_cc foreign key  (cs_call_center_sk) references call_center (cc_call_center_sk) disable novalidate rely;
alter table catalog_sales add constraint cs_cp foreign key  (cs_catalog_page_sk) references catalog_page (cp_catalog_page_sk) disable novalidate rely;
alter table catalog_sales add constraint cs_i foreign key  (cs_item_sk) references item (i_item_sk) disable novalidate rely;
alter table catalog_sales add constraint cs_p foreign key  (cs_promo_sk) references promotion (p_promo_sk) disable novalidate rely;
alter table catalog_sales add constraint cs_s_a foreign key  (cs_ship_addr_sk) references customer_address (ca_address_sk) disable novalidate rely;
alter table catalog_sales add constraint cs_s_cd foreign key  (cs_ship_cdemo_sk) references customer_demographics (cd_demo_sk) disable novalidate rely;
alter table catalog_sales add constraint cs_s_c foreign key  (cs_ship_customer_sk) references customer (c_customer_sk) disable novalidate rely;
alter table catalog_sales add constraint cs_d1 foreign key  (cs_ship_date_sk) references date_dim (d_date_sk) disable novalidate rely;
alter table catalog_sales add constraint cs_s_hd foreign key  (cs_ship_hdemo_sk) references household_demographics (hd_demo_sk) disable novalidate rely;
alter table catalog_sales add constraint cs_sm foreign key  (cs_ship_mode_sk) references ship_mode (sm_ship_mode_sk) disable novalidate rely;
-- partition_col case
alter table catalog_sales add constraint cs_d2 foreign key  (cs_sold_date_sk) references date_dim (d_date_sk) disable novalidate rely;
alter table catalog_sales add constraint cs_t foreign key  (cs_sold_time_sk) references time_dim (t_time_sk) disable novalidate rely;
alter table catalog_sales add constraint cs_w foreign key  (cs_warehouse_sk) references warehouse (w_warehouse_sk) disable novalidate rely;
alter table customer add constraint c_a foreign key  (c_current_addr_sk) references customer_address (ca_address_sk) disable novalidate rely;
alter table customer add constraint c_cd foreign key  (c_current_cdemo_sk) references customer_demographics (cd_demo_sk) disable novalidate rely;
alter table customer add constraint c_hd foreign key  (c_current_hdemo_sk) references household_demographics (hd_demo_sk) disable novalidate rely;
alter table customer add constraint c_fsd foreign key  (c_first_sales_date_sk) references date_dim (d_date_sk) disable novalidate rely;
alter table customer add constraint c_fsd2 foreign key  (c_first_shipto_date_sk) references date_dim (d_date_sk) disable novalidate rely;
alter table household_demographics add constraint hd_ib foreign key  (hd_income_band_sk) references income_band (ib_income_band_sk) disable novalidate rely;
-- partition_col case
alter table inventory add constraint inv_d foreign key  (inv_date_sk) references date_dim (d_date_sk) disable novalidate rely;
alter table inventory add constraint inv_i foreign key  (inv_item_sk) references item (i_item_sk) disable novalidate rely;
alter table inventory add constraint inv_w foreign key  (inv_warehouse_sk) references warehouse (w_warehouse_sk) disable novalidate rely;
alter table promotion add constraint p_end_date foreign key  (p_end_date_sk) references date_dim (d_date_sk) disable novalidate rely;
alter table promotion add constraint p_i foreign key  (p_item_sk) references item (i_item_sk) disable novalidate rely;
alter table promotion add constraint p_start_date foreign key  (p_start_date_sk) references date_dim (d_date_sk) disable novalidate rely;
alter table store add constraint s_close_date foreign key  (s_closed_date_sk) references date_dim (d_date_sk) disable novalidate rely;
alter table store_returns add constraint sr_a foreign key  (sr_addr_sk) references customer_address (ca_address_sk) disable novalidate rely;
alter table store_returns add constraint sr_cd foreign key  (sr_cdemo_sk) references customer_demographics (cd_demo_sk) disable novalidate rely;
alter table store_returns add constraint sr_c foreign key  (sr_customer_sk) references customer (c_customer_sk) disable novalidate rely;
alter table store_returns add constraint sr_hd foreign key  (sr_hdemo_sk) references household_demographics (hd_demo_sk) disable novalidate rely;
alter table store_returns add constraint sr_i foreign key  (sr_item_sk) references item (i_item_sk) disable novalidate rely;
alter table store_returns add constraint sr_r foreign key  (sr_reason_sk) references reason (r_reason_sk) disable novalidate rely;
-- partition_col case
alter table store_returns add constraint sr_ret_d foreign key  (sr_returned_date_sk) references date_dim (d_date_sk) disable novalidate rely;
alter table store_returns add constraint sr_t foreign key  (sr_return_time_sk) references time_dim (t_time_sk) disable novalidate rely;
alter table store_returns add constraint sr_s foreign key  (sr_store_sk) references store (s_store_sk) disable novalidate rely;
alter table store_returns add constraint sr_ss foreign key  (sr_item_sk, sr_ticket_number) references store_sales (ss_item_sk, ss_ticket_number) disable novalidate rely;
alter table store_sales add constraint ss_a foreign key  (ss_addr_sk) references customer_address (ca_address_sk) disable novalidate rely;
alter table store_sales add constraint ss_cd foreign key  (ss_cdemo_sk) references customer_demographics (cd_demo_sk) disable novalidate rely;
alter table store_sales add constraint ss_c foreign key  (ss_customer_sk) references customer (c_customer_sk) disable novalidate rely;
alter table store_sales add constraint ss_hd foreign key  (ss_hdemo_sk) references household_demographics (hd_demo_sk) disable novalidate rely;
alter table store_sales add constraint ss_i foreign key  (ss_item_sk) references item (i_item_sk) disable novalidate rely;
alter table store_sales add constraint ss_p foreign key  (ss_promo_sk) references promotion (p_promo_sk) disable novalidate rely;
-- partition_col case
alter table store_sales add constraint ss_d foreign key  (ss_sold_date_sk) references date_dim (d_date_sk) disable novalidate rely;
alter table store_sales add constraint ss_t foreign key  (ss_sold_time_sk) references time_dim (t_time_sk) disable novalidate rely;
alter table store_sales add constraint ss_s foreign key  (ss_store_sk) references store (s_store_sk) disable novalidate rely;
alter table web_page add constraint wp_ad foreign key  (wp_access_date_sk) references date_dim (d_date_sk) disable novalidate rely;
alter table web_page add constraint wp_cd foreign key  (wp_creation_date_sk) references date_dim (d_date_sk) disable novalidate rely;
alter table web_returns add constraint wr_i foreign key  (wr_item_sk) references item (i_item_sk) disable novalidate rely;
alter table web_returns add constraint wr_r foreign key  (wr_reason_sk) references reason (r_reason_sk) disable novalidate rely;
alter table web_returns add constraint wr_ref_a foreign key  (wr_refunded_addr_sk) references customer_address (ca_address_sk) disable novalidate rely;
alter table web_returns add constraint wr_ref_cd foreign key  (wr_refunded_cdemo_sk) references customer_demographics (cd_demo_sk) disable novalidate rely;
alter table web_returns add constraint wr_ref_c foreign key  (wr_refunded_customer_sk) references customer (c_customer_sk) disable novalidate rely;
alter table web_returns add constraint wr_ref_hd foreign key  (wr_refunded_hdemo_sk) references household_demographics (hd_demo_sk) disable novalidate rely;
-- partition_col case
alter table web_returns add constraint wr_ret_d foreign key  (wr_returned_date_sk) references date_dim (d_date_sk) disable novalidate rely;
alter table web_returns add constraint wr_ret_t foreign key  (wr_returned_time_sk) references time_dim (t_time_sk) disable novalidate rely;
alter table web_returns add constraint wr_ret_a foreign key  (wr_returning_addr_sk) references customer_address (ca_address_sk) disable novalidate rely;
alter table web_returns add constraint wr_ret_cd foreign key  (wr_returning_cdemo_sk) references customer_demographics (cd_demo_sk) disable novalidate rely;
alter table web_returns add constraint wr_ret_c foreign key  (wr_returning_customer_sk) references customer (c_customer_sk) disable novalidate rely;
alter table web_returns add constraint wr_ret_hd foreign key  (wr_returning_hdemo_sk) references household_demographics (hd_demo_sk) disable novalidate rely;
alter table web_returns add constraint wr_ws foreign key  (wr_item_sk, wr_order_number) references web_sales (ws_item_sk, ws_order_number) disable novalidate rely;
alter table web_returns add constraint wr_wp foreign key  (wr_web_page_sk) references web_page (wp_web_page_sk) disable novalidate rely;
alter table web_sales add constraint ws_b_a foreign key  (ws_bill_addr_sk) references customer_address (ca_address_sk) disable novalidate rely;
alter table web_sales add constraint ws_b_cd foreign key  (ws_bill_cdemo_sk) references customer_demographics (cd_demo_sk) disable novalidate rely;
alter table web_sales add constraint ws_b_c foreign key  (ws_bill_customer_sk) references customer (c_customer_sk) disable novalidate rely;
alter table web_sales add constraint ws_b_hd foreign key  (ws_bill_hdemo_sk) references household_demographics (hd_demo_sk) disable novalidate rely;
alter table web_sales add constraint ws_i foreign key  (ws_item_sk) references item (i_item_sk) disable novalidate rely;
alter table web_sales add constraint ws_p foreign key  (ws_promo_sk) references promotion (p_promo_sk) disable novalidate rely;
alter table web_sales add constraint ws_s_a foreign key  (ws_ship_addr_sk) references customer_address (ca_address_sk) disable novalidate rely;
alter table web_sales add constraint ws_s_cd foreign key  (ws_ship_cdemo_sk) references customer_demographics (cd_demo_sk) disable novalidate rely;
alter table web_sales add constraint ws_s_c foreign key  (ws_ship_customer_sk) references customer (c_customer_sk) disable novalidate rely;
alter table web_sales add constraint ws_s_d foreign key  (ws_ship_date_sk) references date_dim (d_date_sk) disable novalidate rely;
alter table web_sales add constraint ws_s_hd foreign key  (ws_ship_hdemo_sk) references household_demographics (hd_demo_sk) disable novalidate rely;
alter table web_sales add constraint ws_sm foreign key  (ws_ship_mode_sk) references ship_mode (sm_ship_mode_sk) disable novalidate rely;
-- partition_col case
alter table web_sales add constraint ws_d2 foreign key  (ws_sold_date_sk) references date_dim (d_date_sk) disable novalidate rely;
alter table web_sales add constraint ws_t foreign key  (ws_sold_time_sk) references time_dim (t_time_sk) disable novalidate rely;
alter table web_sales add constraint ws_w2 foreign key  (ws_warehouse_sk) references warehouse (w_warehouse_sk) disable novalidate rely;
alter table web_sales add constraint ws_wp foreign key  (ws_web_page_sk) references web_page (wp_web_page_sk) disable novalidate rely;
alter table web_sales add constraint ws_ws foreign key  (ws_web_site_sk) references web_site (web_site_sk) disable novalidate rely;
alter table web_site add constraint web_d1 foreign key  (web_close_date_sk) references date_dim (d_date_sk) disable novalidate rely;
alter table web_site add constraint web_d2 foreign key (web_open_date_sk) references date_dim (d_date_sk) disable novalidate rely;

alter table store change column s_store_id s_store_id string constraint strid_nn not null disable novalidate rely;
alter table call_center change column cc_call_center_id cc_call_center_id string constraint ccid_nn not null disable novalidate rely;
alter table catalog_page change column cp_catalog_page_id cp_catalog_page_id string constraint cpid_nn not null disable novalidate rely;
alter table web_site change column web_site_id web_site_id string constraint wsid_nn not null disable novalidate rely;
alter table web_page change column wp_web_page_id wp_web_page_id string constraint wpid_nn not null disable novalidate rely;
alter table warehouse change column w_warehouse_id w_warehouse_id string constraint wid_nn not null disable novalidate rely;
alter table customer change column c_customer_id c_customer_id string constraint cid_nn not null disable novalidate rely;
alter table customer_address change column ca_address_id ca_address_id string constraint caid_nn not null disable novalidate rely;
alter table date_dim change column d_date_id d_date_id string constraint did_nn not null disable novalidate rely;
alter table item change column i_item_id i_item_id string constraint itid_nn not null disable novalidate rely;
alter table promotion change column p_promo_id p_promo_id string constraint pid_nn not null disable novalidate rely;
alter table reason change column r_reason_id r_reason_id string constraint rid_nn not null disable novalidate rely;
alter table ship_mode change column sm_ship_mode_id sm_ship_mode_id string constraint smid_nn not null disable novalidate rely;
alter table time_dim change column t_time_id t_time_id string constraint tid_nn not null disable novalidate rely;
