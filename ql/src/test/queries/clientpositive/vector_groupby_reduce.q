--! qt:disabled:Disabled in HIVE-21396

set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

create table store_sales_txt_n0
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
    ss_wholesale_cost         double,
    ss_list_price             double,
    ss_sales_price            double,
    ss_ext_discount_amt       double,
    ss_ext_sales_price        double,
    ss_ext_wholesale_cost     double,
    ss_ext_list_price         double,
    ss_ext_tax                double,
    ss_coupon_amt             double,
    ss_net_paid               double,
    ss_net_paid_inc_tax       double,
    ss_net_profit             double
)
row format delimited fields terminated by '|'
stored as textfile;

LOAD DATA LOCAL INPATH '../../data/files/store_sales.txt' OVERWRITE INTO TABLE store_sales_txt_n0;

create table store_sales_n3
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
    ss_wholesale_cost         double,
    ss_wholesale_cost_decimal     decimal(38,18),
    ss_list_price             double,
    ss_sales_price            double,
    ss_ext_discount_amt       double,
    ss_ext_sales_price        double,
    ss_ext_wholesale_cost     double,
    ss_ext_list_price         double,
    ss_ext_tax                double,
    ss_coupon_amt             double,
    ss_net_paid               double,
    ss_net_paid_inc_tax       double,
    ss_net_profit             double
)
stored as orc
tblproperties ("orc.stripe.size"="33554432", "orc.compress.size"="16384");


insert overwrite table store_sales_n3
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
    cast(ss_wholesale_cost as decimal(38,18)),
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
 from store_sales_txt_n0;

explain vectorization expression
select 
  ss_ticket_number
from
  store_sales_n3
group by ss_ticket_number
order by ss_ticket_number
limit 20;

select 
  ss_ticket_number
from
  store_sales_n3
group by ss_ticket_number
order by ss_ticket_number
limit 20;



explain vectorization expression
select 
    min(ss_ticket_number) m
from
    (select 
        ss_ticket_number
    from
        store_sales_n3
    group by ss_ticket_number) a
group by ss_ticket_number
order by m;

select 
    min(ss_ticket_number) m
from
    (select 
        ss_ticket_number
    from
        store_sales_n3
    group by ss_ticket_number) a
group by ss_ticket_number
order by m;



explain vectorization expression
select
    ss_ticket_number, sum(ss_item_sk), sum(q), avg(q), sum(np), avg(np), sum(decwc), avg(decwc)
from
    (select
        ss_ticket_number, ss_item_sk, min(ss_quantity) q, max(ss_net_profit) np, max(ss_wholesale_cost_decimal) decwc
    from
        store_sales_n3
    where ss_ticket_number = 1
    group by ss_ticket_number, ss_item_sk) a
group by ss_ticket_number
order by ss_ticket_number;

select
    ss_ticket_number, sum(ss_item_sk), sum(q), avg(q), sum(np), avg(np), sum(decwc), avg(decwc)
from
    (select
        ss_ticket_number, ss_item_sk, min(ss_quantity) q, max(ss_net_profit) np, max(ss_wholesale_cost_decimal) decwc
    from
        store_sales_n3
    where ss_ticket_number = 1
    group by ss_ticket_number, ss_item_sk) a
group by ss_ticket_number
order by ss_ticket_number;


explain vectorization expression
select
    ss_ticket_number, ss_item_sk, sum(q), avg(q), sum(np), avg(np), sum(decwc), avg(decwc)
from
    (select
        ss_ticket_number, ss_item_sk, min(ss_quantity) q, max(ss_net_profit) np, max(ss_wholesale_cost_decimal) decwc
    from
        store_sales_n3
    group by ss_ticket_number, ss_item_sk) a
group by ss_ticket_number, ss_item_sk
order by ss_ticket_number, ss_item_sk;

select
    ss_ticket_number, ss_item_sk, sum(q), avg(q), sum(wc), avg(wc), sum(decwc), avg(decwc)
from
    (select
        ss_ticket_number, ss_item_sk, min(ss_quantity) q, max(ss_wholesale_cost) wc, max(ss_wholesale_cost_decimal) decwc
    from
        store_sales_n3
    group by ss_ticket_number, ss_item_sk) a
group by ss_ticket_number, ss_item_sk
order by ss_ticket_number, ss_item_sk;