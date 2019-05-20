set hive.mapred.mode=strict;
select "${hiveconf:hive.mapred.mode}";
create table t (a int);
analyze table t compute statistics;
select "${hiveconf:hive.mapred.mode}";
