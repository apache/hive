set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=10000;
-- start query 1 in stream 0 using template query19.tpl and seed 1930872976
create table store
(
    s_store_sk                int,
    s_store_id                string,
    s_rec_start_date          string,
    s_rec_end_date            string,
    s_closed_date_sk          int
)
row format delimited fields terminated by '\t'
STORED AS ORC;
create table store_sales
(
    ss_item_sk                int,
    ss_ext_sales_price        decimal(7,2)
)
row format delimited fields terminated by '\t'
STORED AS ORC;

insert into store values(1,'ramesh','ramesh','ramesh',1);
insert into store_sales values(1,1.1);

explain vectorization detail
select  s_store_id brand_id, s_rec_start_date brand, s_rec_end_date, s_closed_date_sk,
       sum(ss_ext_sales_price) ext_price
 from store_sales, store
 where ss_item_sk = s_store_sk
 group by s_store_id,
          s_rec_start_date,
          s_rec_end_date,
          s_closed_date_sk;

select  s_store_id brand_id, s_rec_start_date brand, s_rec_end_date, s_closed_date_sk,
       sum(ss_ext_sales_price) ext_price
 from store_sales, store
 where ss_item_sk = s_store_sk
 group by s_store_id,
          s_rec_start_date,
          s_rec_end_date,
          s_closed_date_sk;
-- end query 1 in stream 0 using template query19.tpl
