--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.auto.convert.join=true;
set hive.tez.cartesian-product.enabled=true;

create table X_n1 as
select distinct * from src order by key limit 10;

explain select * from X_n1 as A, X_n1 as B;
select * from X_n1 as A, X_n1 as B order by A.key, B.key;