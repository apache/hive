SET hive.vectorized.execution.enabled=true;

create table store_sales_txt
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
    ss_wholesale_cost         float,
    ss_list_price             float,
    ss_sales_price            float,
    ss_ext_discount_amt       float,
    ss_ext_sales_price        float,
    ss_ext_wholesale_cost     float,
    ss_ext_list_price         float,
    ss_ext_tax                float,
    ss_coupon_amt             float,
    ss_net_paid               float,
    ss_net_paid_inc_tax       float,
    ss_net_profit             float                  
)
row format delimited fields terminated by '|' 
stored as textfile;

LOAD DATA LOCAL INPATH '../../data/files/store_sales.txt' OVERWRITE INTO TABLE store_sales_txt;

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
    ss_wholesale_cost         float,
    ss_list_price             float,
    ss_sales_price            float,
    ss_ext_discount_amt       float,
    ss_ext_sales_price        float,
    ss_ext_wholesale_cost     float,
    ss_ext_list_price         float,
    ss_ext_tax                float,
    ss_coupon_amt             float,
    ss_net_paid               float,
    ss_net_paid_inc_tax       float,
    ss_net_profit             float
)
stored as orc
tblproperties ("orc.stripe.size"="33554432", "orc.compress.size"="16384");

set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table store_sales
select 
ss_sold_date_sk           ,
    ss_sold_time_sk       ,
    ss_item_sk            ,
    ss_customer_sk        ,
    ss_cdemo_sk           ,
    ss_hdemo_sk           ,
    ss_addr_sk            ,
    ss_store_sk           ,
    ss_promo_sk           ,
    ss_ticket_number      ,
    ss_quantity           ,
    ss_wholesale_cost     ,
    ss_list_price         ,
    ss_sales_price        ,
    ss_ext_discount_amt   ,
    ss_ext_sales_price    ,
    ss_ext_wholesale_cost ,
    ss_ext_list_price     ,
    ss_ext_tax            ,
    ss_coupon_amt         ,
    ss_net_paid           ,
    ss_net_paid_inc_tax   ,
    ss_net_profit         
 from store_sales_txt;

explain
select 
  ss_ticket_number
from
  store_sales
group by ss_ticket_number
limit 20;

select 
  ss_ticket_number
from
  store_sales
group by ss_ticket_number
limit 20;

explain
select 
    min(ss_ticket_number)
from
    (select 
        ss_ticket_number
    from
        store_sales
    group by ss_ticket_number) a
group by ss_ticket_number
limit 20;

select 
    min(ss_ticket_number)
from
    (select 
        ss_ticket_number
    from
        store_sales
    group by ss_ticket_number) a
group by ss_ticket_number
limit 20;

