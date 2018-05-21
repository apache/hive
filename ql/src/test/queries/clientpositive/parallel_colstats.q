--! qt:dataset:src
set hive.explain.user=false;
set mapred.job.name='test_parallel';
set hive.exec.parallel=true;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
set hive.stats.column.autogather=true;

-- SORT_QUERY_RESULTS

create table if not exists src_a_n0 like src;
create table if not exists src_b_n1 like src;

explain
from (select key, value from src group by key, value) s
insert overwrite table src_a_n0 select s.key, s.value group by s.key, s.value
insert overwrite table src_b_n1 select s.key, s.value group by s.key, s.value;

from (select key, value from src group by key, value) s
insert overwrite table src_a_n0 select s.key, s.value group by s.key, s.value
insert overwrite table src_b_n1 select s.key, s.value group by s.key, s.value;

select * from src_a_n0;
select * from src_b_n1;


set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

from (select key, value from src group by key, value) s
insert overwrite table src_a_n0 select s.key, s.value group by s.key, s.value
insert overwrite table src_b_n1 select s.key, s.value group by s.key, s.value;

select * from src_a_n0;
select * from src_b_n1;
