--! qt:dataset:srcpart
--! qt:dataset:src1
--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=30000;
set hive.metastore.aggregate.stats.cache.enabled=false;
set hive.stats.fetch.column.stats=false;
-- Since the inputs are small, it should be automatically converted to mapjoin

-- SORT_QUERY_RESULTS

explain extended select srcpart.key from srcpart join src on (srcpart.value=src.value) join src1 on (srcpart.key=src1.key);

explain
select srcpart.key from srcpart join src on (srcpart.value=src.value) join src1 on (srcpart.key=src1.key) where srcpart.value > 'val_450';

explain
select count(*) from srcpart join src on (srcpart.value=src.value) join src src1 on (srcpart.key=src1.key) group by ds;

set hive.mapjoin.optimized.hashtable=false;

select srcpart.key from srcpart join src on (srcpart.value=src.value) join src1 on (srcpart.key=src1.key) where srcpart.value > 'val_450';

select count(*) from srcpart join src on (srcpart.value=src.value) join src src1 on (srcpart.key=src1.key) group by ds;

set hive.mapjoin.optimized.hashtable=true;

select srcpart.key from srcpart join src on (srcpart.value=src.value) join src1 on (srcpart.key=src1.key) where srcpart.value > 'val_450';

select count(*) from srcpart join src on (srcpart.value=src.value) join src src1 on (srcpart.key=src1.key) group by ds;
