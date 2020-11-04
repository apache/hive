--! qt:dataset:srcpart


create table t (ds string,hr int);
insert into t values ('2008-04-08',11);

set hive.tez.dynamic.partition.pruning=true;

select '2 scans';
explain
select count(*) from srcpart where ds = '2008-04-08' and hr = 11
union 
select count(*) from srcpart where key > '3';


select '???';
explain
select count(*) from srcpart 
  join t on (srcpart.ds = t.ds and srcpart.hr = t.hr)
union 
select count(*) from srcpart where key > '3';

