--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.tez.cartesian-product.enabled=true;
set hive.auto.convert.join=true;
set hive.convert.join.bucket.mapjoin.tez=true;

create table X_n2 (key string, value string) clustered by (key) into 2 buckets;
insert overwrite table X_n2 select distinct * from src order by key limit 10;

create table Y_n0 as
select * from src order by key limit 1;

-- HIVE-29580: the derived table's columns are aliased explicitly because "select *" over the
-- self-join projects two columns named key (and value), making the C.key reference ambiguous.
explain select * from Y_n0, (select A.key, A.value, B.key as key2, B.value as value2 from X_n2 as A join X_n2 as B on A.key=B.key) as C where Y_n0.key=C.key;
