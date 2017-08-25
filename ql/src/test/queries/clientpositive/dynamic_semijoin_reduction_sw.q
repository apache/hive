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

-- Create Tables
create table alltypesorc_int ( cint int, cstring string ) stored as ORC;
create table srcpart_date (key string, value string) partitioned by (ds string ) stored as ORC;
CREATE TABLE srcpart_small(key1 STRING, value1 STRING) partitioned by (ds1 string) STORED as ORC;

-- Add Partitions
alter table srcpart_date add partition (ds = "2008-04-08");
alter table srcpart_date add partition (ds = "2008-04-09");

alter table srcpart_small add partition (ds1 = "2008-04-08");
alter table srcpart_small add partition (ds1 = "2008-04-09");

-- Load
insert overwrite table alltypesorc_int select cint, cstring1 from alltypesorc;
insert overwrite table srcpart_date partition (ds = "2008-04-08" ) select key, value from srcpart where ds = "2008-04-08";
insert overwrite table srcpart_date partition (ds = "2008-04-09") select key, value from srcpart where ds = "2008-04-09";
insert overwrite table srcpart_small partition (ds1 = "2008-04-09") select key, value from srcpart where ds = "2008-04-09" limit 20;

set hive.tez.dynamic.semijoin.reduction=false;

analyze table alltypesorc_int compute statistics for columns;
analyze table srcpart_date compute statistics for columns;
analyze table srcpart_small compute statistics for columns;

set hive.tez.dynamic.semijoin.reduction=true;
EXPLAIN
SELECT count(*)
FROM (
  SELECT *
  FROM (SELECT * FROM srcpart_date WHERE ds = "2008-04-09") `srcpart_date`
  JOIN (SELECT * FROM srcpart_small WHERE ds1 = "2008-04-08") `srcpart_small`
    ON (srcpart_date.key = srcpart_small.key1)
  JOIN alltypesorc_int
    ON (srcpart_small.key1 = alltypesorc_int.cstring)) a
JOIN (
  SELECT *
  FROM (SELECT * FROM srcpart_date WHERE ds = "2008-04-08") `srcpart_date`
  JOIN (SELECT * FROM srcpart_small WHERE ds1 = "2008-04-08") `srcpart_small`
    ON (srcpart_date.key = srcpart_small.key1)
  JOIN alltypesorc_int
    ON (srcpart_small.key1 = alltypesorc_int.cstring)) b
ON ('1' = '1');

drop table srcpart_date;
drop table srcpart_small;
drop table alltypesorc_int;
