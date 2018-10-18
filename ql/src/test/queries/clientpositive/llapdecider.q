--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.stats.fetch.column.stats=true;
set hive.execution.mode=llap;
set hive.llap.execution.mode=auto;

-- simple query with multiple reduce stages
EXPLAIN SELECT key, count(value) as cnt FROM src GROUP BY key ORDER BY cnt;

create table src_orc_n2 stored as orc as select * from src;

EXPLAIN SELECT key, count(value) as cnt FROM src_orc_n2 GROUP BY key ORDER BY cnt;

set hive.llap.auto.enforce.stats=false;

EXPLAIN SELECT key, count(value) as cnt FROM src_orc_n2 GROUP BY key ORDER BY cnt;

set hive.llap.auto.enforce.stats=true;

analyze table src_orc_n2 compute statistics for columns;

EXPLAIN SELECT key, count(value) as cnt FROM src_orc_n2 GROUP BY key ORDER BY cnt;

EXPLAIN SELECT * from src_orc_n2 join src on (src_orc_n2.key = src.key) order by src.value;

EXPLAIN SELECT * from src_orc_n2 s1 join src_orc_n2 s2 on (s1.key = s2.key) order by s2.value;

set hive.llap.auto.enforce.tree=false;

EXPLAIN SELECT * from src_orc_n2 join src on (src_orc_n2.key = src.key) order by src.value;

set hive.llap.auto.enforce.tree=true;

set hive.llap.auto.max.input.size=10;

EXPLAIN SELECT * from src_orc_n2 s1 join src_orc_n2 s2 on (s1.key = s2.key) order by s2.value;

set hive.llap.auto.max.input.size=1000000000;
set hive.llap.auto.max.output.size=10;

EXPLAIN SELECT * from src_orc_n2 s1 join src_orc_n2 s2 on (s1.key = s2.key) order by s2.value;

set hive.llap.auto.max.output.size=1000000000;

set hive.llap.execution.mode=map;

EXPLAIN SELECT * from src_orc_n2 s1 join src_orc_n2 s2 on (s1.key = s2.key) order by s2.value;

set hive.llap.execution.mode=none;

EXPLAIN SELECT * from src_orc_n2 s1 join src_orc_n2 s2 on (s1.key = s2.key) order by s2.value;

set hive.llap.execution.mode=all;

EXPLAIN SELECT * from src_orc_n2 s1 join src_orc_n2 s2 on (s1.key = s2.key) order by s2.value;

CREATE TEMPORARY FUNCTION test_udf_get_java_string AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFTestGetJavaString';

set hive.llap.execution.mode=auto;

EXPLAIN SELECT sum(cast(key as int) + 1) from src_orc_n2 where cast(key as int) > 1;
EXPLAIN SELECT sum(cast(test_udf_get_java_string(cast(key as string)) as int) + 1) from src_orc_n2 where cast(key as int) > 1;
EXPLAIN SELECT sum(cast(key as int) + 1) from src_orc_n2 where cast(test_udf_get_java_string(cast(key as string)) as int) > 1;

set hive.llap.skip.compile.udf.check=true;

EXPLAIN SELECT sum(cast(test_udf_get_java_string(cast(key as string)) as int) + 1) from src_orc_n2 where cast(key as int) > 1;

set hive.execution.mode=container;