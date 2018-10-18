--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;


set hive.optimize.bucketingsorting=false;
set hive.auto.convert.join.noconditionaltask.size=10000;

create table test_n1 (key int, value string) partitioned by (p int) clustered by (key) into 2 buckets stored as textfile;
create table test1 (key int, value string) stored as textfile;

insert into table test_n1 partition (p=1) select * from src;

alter table test_n1 set fileformat orc;

insert into table test_n1 partition (p=2) select * from src;
insert into table test1 select * from src;

describe test_n1;
set hive.auto.convert.join = true;
set hive.convert.join.bucket.mapjoin.tez = true;

explain select test_n1.key, test_n1.value from test_n1 join test1 on (test_n1.key = test1.key) order by test_n1.key;

select test_n1.key, test_n1.value from test_n1 join test1 on (test_n1.key = test1.key) order by test_n1.key;

