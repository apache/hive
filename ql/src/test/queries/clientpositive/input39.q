--! qt:dataset:src


create table t1_n121(key string, value string) partitioned by (ds string);
create table t2_n71(key string, value string) partitioned by (ds string);

insert overwrite table t1_n121 partition (ds='1')
select key, value from src;

insert overwrite table t1_n121 partition (ds='2')
select key, value from src;

insert overwrite table t2_n71 partition (ds='1')
select key, value from src;

set hive.test.mode=true;
set hive.mapred.mode=strict;
set mapreduce.framework.name=yarn;
set mapreduce.jobtracker.address=localhost:58;
set hive.exec.mode.local.auto=true;

explain
select count(1) from t1_n121 join t2_n71 on t1_n121.key=t2_n71.key where t1_n121.ds='1' and t2_n71.ds='1';

select count(1) from t1_n121 join t2_n71 on t1_n121.key=t2_n71.key where t1_n121.ds='1' and t2_n71.ds='1';

set hive.test.mode=false;
set mapreduce.framework.name;
set mapreduce.jobtracker.address;



