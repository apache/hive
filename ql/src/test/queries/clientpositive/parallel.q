set mapred.job.name='test_parallel';
set hive.exec.parallel=true;
set hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;

-- SORT_QUERY_RESULTS

create table if not exists src_a like src;
create table if not exists src_b like src;

explain
from (select key, value from src group by key, value) s
insert overwrite table src_a select s.key, s.value group by s.key, s.value
insert overwrite table src_b select s.key, s.value group by s.key, s.value;

from (select key, value from src group by key, value) s
insert overwrite table src_a select s.key, s.value group by s.key, s.value
insert overwrite table src_b select s.key, s.value group by s.key, s.value;

select * from src_a;
select * from src_b;


set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

from (select key, value from src group by key, value) s
insert overwrite table src_a select s.key, s.value group by s.key, s.value
insert overwrite table src_b select s.key, s.value group by s.key, s.value;

select * from src_a;
select * from src_b;
