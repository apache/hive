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
set hive.cbo.enable=false;
set hive.reorder.nway.joins=false;
set hive.merge.nway.joins=false;

-- Create Tables
create table alltypesorc_int_n0 ( cint int, cstring string ) stored as ORC;
create table srcpart_date_n6 (key string, value string) partitioned by (ds string ) stored as ORC;
CREATE TABLE srcpart_small_n2(key1 STRING, value1 STRING) partitioned by (ds1 string) STORED as ORC;

-- Add Partitions
alter table srcpart_date_n6 add partition (ds = "2008-04-08");
alter table srcpart_date_n6 add partition (ds = "2008-04-09");

alter table srcpart_small_n2 add partition (ds1 = "2008-04-08");
alter table srcpart_small_n2 add partition (ds1 = "2008-04-09");

-- Load
insert overwrite table alltypesorc_int_n0 select cint, cstring1 from alltypesorc;
insert overwrite table srcpart_date_n6 partition (ds = "2008-04-08" ) select key, value from srcpart where ds = "2008-04-08";
insert overwrite table srcpart_date_n6 partition (ds = "2008-04-09") select key, value from srcpart where ds = "2008-04-09";
insert overwrite table srcpart_small_n2 partition (ds1 = "2008-04-09") select key, value from srcpart where ds = "2008-04-09" limit 20;

set hive.tez.dynamic.semijoin.reduction=false;

analyze table alltypesorc_int_n0 compute statistics for columns;
analyze table srcpart_date_n6 compute statistics for columns;
analyze table srcpart_small_n2 compute statistics for columns;

set hive.tez.dynamic.semijoin.reduction=true;
EXPLAIN
SELECT count(*)
  FROM (SELECT * FROM srcpart_date_n6 WHERE ds = "2008-04-09") `srcpart_date_n6`
  JOIN (SELECT * FROM srcpart_small_n2 WHERE ds1 = "2008-04-08") `srcpart_small_n2`
    ON (srcpart_date_n6.key = srcpart_small_n2.key1)
  JOIN (
    SELECT *
    FROM (SELECT * FROM alltypesorc_int_n0 WHERE cint = 10) `alltypesorc_int_n0`
    JOIN (SELECT * FROM srcpart_small_n2) `srcpart_small_n2`
      ON (alltypesorc_int_n0.cstring = srcpart_small_n2.key1)) b
    ON (srcpart_small_n2.key1 = b.cstring);

drop table srcpart_date_n6;
drop table srcpart_small_n2;
drop table alltypesorc_int_n0;
