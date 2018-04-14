--! qt:dataset:src
set hive.stats.column.autogather=false;
set hive.stats.fetch.column.stats=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.auto.convert.join=true;
set hive.join.emit.interval=2;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=10000;
set hive.auto.convert.sortmerge.join.bigtable.selection.policy = org.apache.hadoop.hive.ql.optimizer.TableSizeBasedBigTableSelectorForAutoSMJ;
set hive.optimize.bucketingsorting=false;

drop table src_multi1_n6;

create table src_multi1_n6 like src;

analyze table src_multi1_n6 compute statistics for columns key;

describe formatted src_multi1_n6;

set hive.stats.column.autogather=true;

insert into table src_multi1_n6 select * from src;

describe formatted src_multi1_n6;


set hive.stats.column.autogather=false;

drop table nzhang_part14_n2;

create table if not exists nzhang_part14_n2 (key string, value string)
  partitioned by (ds string, hr string);

describe formatted nzhang_part14_n2;

insert into table nzhang_part14_n2 partition(ds, hr) 
select key, value, ds, hr from (
  select * from (select 'k1' as key, cast(null as string) as value, '1' as ds, '2' as hr from src limit 2)a 
  union all
  select * from (select 'k2' as key, '' as value, '1' as ds, '3' as hr from src limit 2)b
  union all 
  select * from (select 'k3' as key, ' ' as value, '2' as ds, '1' as hr from src limit 2)c
) T;

desc formatted nzhang_part14_n2 partition(ds='1', hr='3');

analyze table nzhang_part14_n2 partition(ds='1', hr='3') compute statistics for columns value;

desc formatted nzhang_part14_n2 partition(ds='1', hr='3');

desc formatted nzhang_part14_n2 partition(ds='2', hr='1');

set hive.stats.column.autogather=true;

insert into table nzhang_part14_n2 partition(ds, hr)
select key, value, ds, hr from (
  select * from (select 'k1' as key, cast(null as string) as value, '1' as ds, '2' as hr from src limit 2)a
  union all
  select * from (select 'k2' as key, '' as value, '1' as ds, '3' as hr from src limit 2)b
  union all
  select * from (select 'k3' as key, ' ' as value, '2' as ds, '1' as hr from src limit 2)c
) T;

desc formatted nzhang_part14_n2 partition(ds='1', hr='3');

desc formatted nzhang_part14_n2 partition(ds='2', hr='1');

