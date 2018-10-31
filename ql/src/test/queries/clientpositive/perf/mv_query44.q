set hive.mapred.mode=nonstrict;
set hive.materializedview.rewriting.time.window=-1;

-- start query 1 in stream 0 using template query44.tpl and seed 1819994127

CREATE MATERIALIZED VIEW mv_store_sales_item_customer PARTITIONED ON (ss_sold_date_sk)
AS
  select ss_item_sk, ss_store_sk, ss_customer_sk,  ss_sold_date_sk, count(*) cnt, sum(ss_quantity) as ss_quantity, sum(ss_ext_wholesale_cost) as ss_ext_wholesale_cost,sum(ss_net_paid) as ss_net_paid,sum(ss_net_profit) as ss_net_profit, sum(ss_ext_sales_price) as ss_ext_sales_price, sum(ss_coupon_amt) amt, sum(ss_sales_price) ss_sales_price, sum(ss_quantity*ss_sales_price) ssales
  from store_sales
  group by ss_store_sk, 
  ss_item_sk,  ss_customer_sk, ss_sold_date_sk;

explain
select  asceding.rnk, i1.i_product_name best_performing, i2.i_product_name worst_performing
from(select *
     from (select item_sk,rank() over (order by rank_col asc) rnk
           from (select ss_item_sk item_sk,avg(ss_net_profit) rank_col 
                 from store_sales ss1
                 where ss_store_sk = 410
                 group by ss_item_sk
                 having avg(ss_net_profit) > 0.9*(select avg(ss_net_profit) rank_col
                                                  from store_sales
                                                  where ss_store_sk = 410
                                                    and ss_hdemo_sk is null
                                                  group by ss_store_sk))V1)V11
     where rnk  < 11) asceding,
    (select *
     from (select item_sk,rank() over (order by rank_col desc) rnk
           from (select ss_item_sk item_sk,avg(ss_net_profit) rank_col
                 from store_sales ss1
                 where ss_store_sk = 410
                 group by ss_item_sk
                 having avg(ss_net_profit) > 0.9*(select avg(ss_net_profit) rank_col
                                                  from store_sales
                                                  where ss_store_sk = 410
                                                    and ss_hdemo_sk is null
                                                  group by ss_store_sk))V2)V21
     where rnk  < 11) descending,
item i1,
item i2
where asceding.rnk = descending.rnk 
  and i1.i_item_sk=asceding.item_sk
  and i2.i_item_sk=descending.item_sk
order by asceding.rnk
limit 100;

-- end query 1 in stream 0 using template query44.tpl
