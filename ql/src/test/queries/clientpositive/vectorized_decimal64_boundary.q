set hive.mapred.mode=nonstrict;

drop table if exists mini_store;
create table mini_store
(
    s_store_sk                int,
    s_store_id                string
)
row format delimited fields terminated by '\t'
STORED AS ORC;

drop table if exists mini_sales;
create table mini_sales
(
    ss_store_sk               int,
    ss_quantity               int,
    ss_sales_price            decimal(7,2)
)
row format delimited fields terminated by '\t'
STORED AS ORC;

insert into mini_store values (1, 'store');
insert into mini_sales values (1, 2, 1.2);

explain vectorization detail
select s_store_id, coalesce(ss_sales_price*ss_quantity,0) sumsales
            from mini_sales, mini_store
       where ss_store_sk = s_store_sk;

select s_store_id, coalesce(ss_sales_price*ss_quantity,0) sumsales
from mini_sales, mini_store
where ss_store_sk = s_store_sk;
