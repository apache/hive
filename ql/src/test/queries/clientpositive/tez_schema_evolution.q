--! qt:dataset:src
SET hive.vectorized.execution.enabled=false;
set hive.mapred.mode=nonstrict;
-- SORT_QUERY_RESULTS

create table test_n9 (key int, value string) partitioned by (p int) stored as textfile;

insert into table test_n9 partition (p=1) select * from src order by key limit 10;

alter table test_n9 set fileformat orc;

insert into table test_n9 partition (p=2) select * from src order by key limit 10;

describe test_n9;

select * from test_n9 where p=1 and key > 0 order by key;
select * from test_n9 where p=2 and key > 0 order by key;
select * from test_n9 where key > 0 order by key;

