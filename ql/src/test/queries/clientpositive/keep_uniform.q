set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.mapred.mode=nonstrict;


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


explain
vectorization detail
with ws_wh as
(select ws1.ws_order_number,ws1.ws_warehouse_sk wh1,ws2.ws_warehouse_sk wh2
 from web_sales ws1,web_sales ws2
 where ws1.ws_order_number = ws2.ws_order_number
   and ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)
 select
   count(distinct ws_order_number) as `order count`
  ,sum(ws_ext_ship_cost) as `total shipping cost`
  ,sum(ws_net_profit) as `total net profit`
from
   web_sales ws1
  ,date_dim
  ,customer_address
  ,web_site
where
    d_date between '1999-5-01' and
           (cast('1999-5-01' as date) + 60 days)
and ws1.ws_ship_date_sk = d_date_sk
and ws1.ws_ship_addr_sk = ca_address_sk
and ca_state = 'TX'
and ws1.ws_web_site_sk = web_site_sk
and web_company_name = 'pri'
and ws1.ws_order_number in (select ws_order_number
                            from ws_wh)
and ws1.ws_order_number in (select wr_order_number
                            from web_returns,ws_wh
                            where wr_order_number = ws_wh.ws_order_number)
order by count(distinct ws_order_number)
limit 100;
