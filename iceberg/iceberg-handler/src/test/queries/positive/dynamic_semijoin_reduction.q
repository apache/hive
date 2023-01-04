--! qt:dataset:srcpart
--! qt:dataset:alltypesorc
set hive.compute.query.using.stats=false;
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.optimize.ppd=true;
set hive.ppd.remove.duplicatefilters=true;
set hive.tez.dynamic.partition.pruning=true;
set hive.tez.dynamic.semijoin.reduction=true;
set hive.optimize.metadataonly=false;
set hive.optimize.index.filter=true;
set hive.stats.autogather=true;
set hive.tez.bigtable.minsize.semijoin.reduction=1;
set hive.tez.min.bloom.filter.entries=1;
set hive.stats.fetch.column.stats=true;
set hive.tez.bloom.filter.factor=1.0f;

-- Create Tables
create table srcpart_date_n7 (key string, value string) partitioned by (ds string ) stored by iceberg;
CREATE TABLE srcpart_small_n3(key1 STRING, value1 STRING) partitioned by (ds string) stored by iceberg;

-- Add Partitions
--alter table srcpart_date_n7 add partition (ds = "2008-04-08");
--alter table srcpart_date_n7 add partition (ds = "2008-04-09");

--alter table srcpart_small_n3 add partition (ds = "2008-04-08");
--alter table srcpart_small_n3 add partition (ds = "2008-04-09");

-- Load
insert overwrite table srcpart_date_n7   select key, value, ds from srcpart where ds = "2008-04-08";
insert overwrite table srcpart_date_n7  select key, value, ds from srcpart where ds = "2008-04-09";
insert overwrite table srcpart_small_n3  select key, value, ds from srcpart where ds = "2008-04-09" limit 20;

EXPLAIN select count(*) from srcpart_date_n7 join srcpart_small_n3 on (srcpart_date_n7.key = srcpart_small_n3.key1);
select count(*) from srcpart_date_n7 join srcpart_small_n3 on (srcpart_date_n7.key = srcpart_small_n3.key1);

drop table srcpart_date_n7;
drop table srcpart_small_n3;