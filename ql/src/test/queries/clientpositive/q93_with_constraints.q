drop table if exists store_sales;
create external table store_sales
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
STORED AS ORC tblproperties ("orc.compress"="ZLIB");
alter table store_sales update statistics set ('numRows'='575995635');

drop table if exists store_returns;
create external table store_returns
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
STORED AS ORC tblproperties ("orc.compress"="ZLIB");
alter table store_returns update statistics set ('numRows'='57591150');

drop table if exists reason;
create external table reason
(
    r_reason_sk         int,
    r_reason_id         string,
    r_reason_desc       string
)
row format delimited fields terminated by '\t'
STORED AS ORC tblproperties ("orc.compress"="ZLIB");
alter table reason update statistics set ('numRows'='72');

alter table store_returns add constraint tpcds_pk_sr primary key (sr_item_sk, sr_ticket_number) disable novalidate rely;

explain
select  ss_customer_sk
            ,sum(act_sales) sumsales
      from (select ss_item_sk
                  ,ss_ticket_number
                  ,ss_customer_sk
                  ,case when sr_return_quantity is not null then (ss_quantity-sr_return_quantity)*ss_sales_price
                                                            else (ss_quantity*ss_sales_price) end act_sales
            from store_sales left outer join store_returns on (sr_item_sk = ss_item_sk
                                                               and sr_ticket_number = ss_ticket_number)
                ,reason
            where sr_reason_sk = r_reason_sk
              and r_reason_desc = 'Did not like the warranty') t
      group by ss_customer_sk
      order by sumsales, ss_customer_sk
limit 100;
