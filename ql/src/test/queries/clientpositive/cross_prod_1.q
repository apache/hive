--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.tez.cartesian-product.enabled=true;

create table X_n0 as
select distinct * from src order by key limit 10;

explain select * from X_n0 as A, X_n0 as B order by A.key, B.key;
select * from X_n0 as A, X_n0 as B order by A.key, B.key;

explain select * from X_n0 as A join X_n0 as B on A.key<B.key;
select * from X_n0 as A join X_n0 as B on A.key<B.key order by A.key, B.key;

explain select * from X_n0 as A join X_n0 as B on A.key between "103" and "105";
select * from X_n0 as A join X_n0 as B on A.key between "103" and "105" order by A.key, B.key;

explain select * from X_n0 as A, X_n0 as B, X_n0 as C;
select * from X_n0 as A, X_n0 as B, X_n0 as C order by A.key, B.key, C.key;

explain select * from X_n0 as A join X_n0 as B on A.key in ("103", "104", "105");
select * from X_n0 as A join X_n0 as B on A.key in ("103", "104", "105") order by A.key, B.key;

explain select A.key, count(*)  from X_n0 as A, X_n0 as B group by A.key;
select A.key, count(*)  from X_n0 as A, X_n0 as B group by A.key order by A.key;

explain select * from X_n0 as A left outer join X_n0 as B on (A.key = B.key or A.value between "val_103" and "val_105");
explain select * from X_n0 as A right outer join X_n0 as B on (A.key = B.key or A.value between "val_103" and "val_105");
explain select * from X_n0 as A full outer join X_n0 as B on (A.key = B.key or A.value between "val_103" and "val_105");

explain select * from (select X_n0.key, count(*) from X_n0 group by X_n0.key) as A, (select X_n0.key, count(*) from X_n0 group by X_n0.key) as B;
select * from (select X_n0.key, count(*) from X_n0 group by X_n0.key) as A, (select X_n0.key, count(*) from X_n0 group by X_n0.key) as B order by A.key, B.key;

explain select * from (select * from X_n0 union all select * from X_n0 as y) a join X_n0;
select * from (select * from X_n0 union all select * from X_n0 as y) a join X_n0 order by a.key, X_n0.key;