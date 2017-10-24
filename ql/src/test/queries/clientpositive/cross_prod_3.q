set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.tez.cartesian-product.enabled=true;
set hive.auto.convert.join=true;
set hive.convert.join.bucket.mapjoin.tez=true;

create table X (key string, value string) clustered by (key) into 2 buckets;
insert overwrite table X select distinct * from src order by key limit 10;

create table Y as
select * from src order by key limit 1;

explain select * from Y, (select * from X as A join X as B on A.key=B.key) as C where Y.key=C.key;
