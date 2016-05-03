SET hive.vectorized.execution.enabled=false;
set hive.mapred.mode=nonstrict;
-- SORT_QUERY_RESULTS

create table test (key int, value string) partitioned by (p int) stored as textfile;

insert into table test partition (p=1) select * from src order by key limit 10;

alter table test set fileformat orc;

insert into table test partition (p=2) select * from src order by key limit 10;

describe test;

select * from test where p=1 and key > 0 order by key;
select * from test where p=2 and key > 0 order by key;
select * from test where key > 0 order by key;

