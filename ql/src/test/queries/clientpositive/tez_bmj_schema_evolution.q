set hive.enforce.bucketing=true;
set hive.enforce.sorting = true;
set hive.optimize.bucketingsorting=false;
set hive.auto.convert.join.noconditionaltask.size=10000;

create table test (key int, value string) partitioned by (p int) clustered by (key) into 2 buckets stored as textfile;
create table test1 (key int, value string) stored as textfile;

insert into table test partition (p=1) select * from src;

alter table test set fileformat orc;

insert into table test partition (p=2) select * from src;
insert into table test1 select * from src;

describe test;
set hive.auto.convert.join = true;
set hive.convert.join.bucket.mapjoin.tez = true;

explain select test.key, test.value from test join test1 on (test.key = test1.key) order by test.key;

select test.key, test.value from test join test1 on (test.key = test1.key) order by test.key;

