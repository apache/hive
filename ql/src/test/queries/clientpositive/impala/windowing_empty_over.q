--! qt:dataset:impala_dataset
drop table if exists tab_sales_n1;
create table tab_sales_n1
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
STORED AS PARQUET;


explain cbo physical
select sum(ss_quantity) over() from tab_sales_n1;

explain
select sum(ss_quantity) over() from tab_sales_n1;


explain cbo physical
select sum(ss_net_profit) over() from tab_sales_n1;

explain
select sum(ss_net_profit) over() from tab_sales_n1;


drop table if exists tab_sales_n1;
