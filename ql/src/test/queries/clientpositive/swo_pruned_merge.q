--! qt:dataset:srcpart
set hive.tez.dynamic.partition.pruning=false;
set hive.auto.convert.join=false;

create table t (ds string,hr int);
insert into t values ('2008-04-08',11);

select 'involved partitions differ: expected 2 separate scans (could be improved)';
explain
select count(*) from srcpart where ds = '2008-04-08' and hr = 11
union 
select count(*) from srcpart where key > '3';


select 'expected 1 as both scans all the partitions';
explain
select count(*) from srcpart 
  join t on (srcpart.ds = t.ds and srcpart.hr = t.hr)
union 
select count(*) from srcpart where key > '3';

