set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.auto.convert.join=true;
set hive.tez.cartesian-product.enabled=true;

create table X as
select distinct * from src order by key limit 10;

explain select * from X as A, X as B;
select * from X as A, X as B order by A.key, B.key;