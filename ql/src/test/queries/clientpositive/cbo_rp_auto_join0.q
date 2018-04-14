--! qt:dataset:cbo_t3
--! qt:dataset:cbo_t2
--! qt:dataset:cbo_t1
set hive.mapred.mode=nonstrict;
set hive.cbo.returnpath.hiveop=true;
set hive.stats.fetch.column.stats=true;
set hive.auto.convert.join = true;

explain 
select sum(hash(a.k1,a.v1,a.k2, a.v2))
from (
SELECT cbo_t1.key as k1, cbo_t1.value as v1, 
       cbo_t2.key as k2, cbo_t2.value as v2 FROM 
  (SELECT * FROM cbo_t3 WHERE cbo_t3.key < 10) cbo_t1 
    JOIN 
  (SELECT * FROM cbo_t3 WHERE cbo_t3.key < 10) cbo_t2
  SORT BY k1, v1, k2, v2
) a;

explain 
select sum(hash(a.k1,a.v1,a.k2, a.v2))
from (
SELECT cbo_t1.key as k1, cbo_t1.value as v1, 
       cbo_t2.key as k2, cbo_t2.value as v2 FROM 
  (SELECT * FROM cbo_t3 WHERE cbo_t3.key < 10) cbo_t1 
    JOIN 
  (SELECT * FROM cbo_t3 WHERE cbo_t3.key < 10) cbo_t2
  SORT BY k1, v1, k2, v2
) a;
