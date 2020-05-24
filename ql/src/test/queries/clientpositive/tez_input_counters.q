--! qt:dataset:src
set hive.compute.query.using.stats=false;
set hive.exec.max.dynamic.partitions=400;
set hive.exec.max.dynamic.partitions.pernode=400;
set hive.mapred.mode=nonstrict;
set hive.fetch.task.conversion=none;
set hive.map.aggr=false;
-- disabling map side aggregation as that can lead to different intermediate record counts
set hive.tez.exec.print.summary=true;
set hive.optimize.sort.dynamic.partition.threshold=-1;

create table testpart (k int) partitioned by (v string);
insert overwrite table testpart partition(v) select * from src;
insert into table testpart partition(v) select * from src;

set hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.PostExecTezSummaryPrinter;
select sum(hash(*)) from testpart;
select sum(hash(*)) from testpart where v < 'val_100';
select sum(hash(*)) from testpart where v < 'val_200';

set hive.tez.dynamic.partition.pruning=true;
create table testpart1 like testpart;
insert overwrite table testpart1 partition(v) select * from testpart where v < 'val_200';

explain select sum(hash(*)) from testpart t1 join testpart1 t2 on t1.v = t2.v;
select sum(hash(*)) from testpart t1 join testpart1 t2 on t1.v = t2.v;
