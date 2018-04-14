--! qt:dataset:src
-- INCLUDE_HADOOP_MAJOR_VERSIONS(0.20, 0.20S)


create table t1_n77(key string, value string) partitioned by (ds string);
create table t2_n46(key string, value string) partitioned by (ds string);

insert overwrite table t1_n77 partition (ds='1')
select key, value from src;

insert overwrite table t1_n77 partition (ds='2')
select key, value from src;

insert overwrite table t2_n46 partition (ds='1')
select key, value from src;

set hive.test.mode=true;
set hive.mapred.mode=strict;
set mapred.job.tracker=localhost:58;
set hive.exec.mode.local.auto=true;

explain
select count(1) from t1_n77 join t2_n46 on t1_n77.key=t2_n46.key where t1_n77.ds='1' and t2_n46.ds='1';

select count(1) from t1_n77 join t2_n46 on t1_n77.key=t2_n46.key where t1_n77.ds='1' and t2_n46.ds='1';

set hive.test.mode=false;
set mapred.job.tracker;



