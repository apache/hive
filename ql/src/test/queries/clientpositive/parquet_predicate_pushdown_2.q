SET hive.optimize.ppd=true;
SET hive.optimize.index.filter=true;

create table test_parq(a int, b int) partitioned by (p int) stored as parquet;
insert overwrite table test_parq partition (p=1) values (1, 1);
select * from test_parq where a=1 and p=1;
select * from test_parq where (a=1 and p=1) or (a=999 and p=999);
