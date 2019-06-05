set hive.auto.convert.join=true;
drop table if exists store_sales_s0;
drop table if exists store_s0;

CREATE TABLE store_sales_s0 (ss_item_sk int,payload string,payload2 string,payload3 string) PARTITIONED BY (ss_store_sk int) stored as orc TBLPROPERTIES( 'transactional'='false');
CREATE TABLE store_s0 (s_item_sk int,s_store_sk int,s_state string) stored as orc TBLPROPERTIES( 'transactional'='false');

insert into store_s0 values
    (1,10,'XX'),
    (2,20,'AA'),
    (3,30,'ZZ')
    ;
insert into store_sales_s0 partition(ss_store_sk=9) values (1,'xxx','xxx','xxx'),(2,'xxx','xxx','xxx'),(3,'xxx','xxx','xxx'),(4,'xxx','xxx','xxx'),(5,'xxx','xxx','xxx');
insert into store_sales_s0 partition(ss_store_sk=39) values (1,'xxx','xxx','xxx'),(2,'xxx','xxx','xxx'),(3,'xxx','xxx','xxx'),(4,'xxx','xxx','xxx'),(5,'xxx','xxx','xxx');

explain select grouping(s_state) from store_s0, store_sales_s0 where ss_store_sk = s_store_sk and s_state in ('SD','FL', 'MI', 'LA', 'MO', 'SC') group by rollup(ss_item_sk, s_state);
select grouping(s_state) from store_s0, store_sales_s0 where ss_store_sk = s_store_sk and s_state in ('SD','FL', 'MI', 'LA', 'MO', 'SC') group by rollup(ss_item_sk, s_state);

