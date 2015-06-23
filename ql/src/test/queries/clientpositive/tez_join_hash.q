set hive.explain.user=false;
-- SORT_QUERY_RESULTS

create table orc_src (key string, value string) STORED AS ORC;
insert into table orc_src select * from src;

set hive.execution.engine=tez;
set hive.vectorized.execution.enabled=true;
set hive.auto.convert.join.noconditionaltask.size=1;
set hive.exec.reducers.bytes.per.reducer=20000;

explain
SELECT count(*) FROM src, orc_src where src.key=orc_src.key;

SELECT count(*) FROM src, orc_src where src.key=orc_src.key;

set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=3000;
set hive.mapjoin.hybridgrace.minwbsize=350;
set hive.mapjoin.hybridgrace.minnumpartitions=8;

explain
select count(*) from (select x.key as key, y.value as value from
srcpart x join srcpart y on (x.key = y.key)
union all
select key, value from srcpart z) a join src b on (a.value = b.value) group by a.key, a.value;

select key, count(*) from (select x.key as key, y.value as value from
srcpart x join srcpart y on (x.key = y.key)
union all
select key, value from srcpart z) a join src b on (a.value = b.value) group by a.key, a.value;

set hive.execution.engine=mr;
select key, count(*) from (select x.key as key, y.value as value from
srcpart x join srcpart y on (x.key = y.key)
union all
select key, value from srcpart z) a join src b on (a.value = b.value) group by a.key, a.value;


