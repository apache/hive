set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.cbo.enable=true;
set hive.compute.query.using.stats=false;
set hive.mapred.mode=nonstrict;
set hive.optimize.ppd=true;
set hive.ppd.remove.duplicatefilters=true;
set hive.tez.dynamic.partition.pruning=true;
set hive.tez.dynamic.semijoin.reduction=true;
set hive.optimize.metadataonly=false;
set hive.optimize.index.filter=true;
set hive.stats.autogather=true;
set hive.tez.bigtable.minsize.semijoin.reduction=1;
set hive.tez.min.bloom.filter.entries=1;
set hive.tez.dynamic.semijoin.reduction.threshold=-999999999999;

-- Create Tables
create table alltypesorc_int ( cint int, cstring string ) stored as ORC;
create table srcpart_date (str string, value string) partitioned by (ds string ) stored as ORC;
CREATE TABLE srcpart_small(key1 STRING, value1 STRING) partitioned by (ds string) STORED as ORC;

-- Add Partitions
alter table srcpart_date add partition (ds = "2008-04-08");
alter table srcpart_date add partition (ds = "2008-04-09");

alter table srcpart_small add partition (ds = "2008-04-08");
alter table srcpart_small add partition (ds = "2008-04-09");

-- Load
insert overwrite table alltypesorc_int select cint, cstring1 from alltypesorc;
insert overwrite table srcpart_date partition (ds = "2008-04-08" ) select key, value from srcpart where ds = "2008-04-08";
insert overwrite table srcpart_date partition (ds = "2008-04-09") select key, value from srcpart where ds = "2008-04-09";
insert overwrite table srcpart_small partition (ds = "2008-04-09") select key, value from srcpart where ds = "2008-04-09";
analyze table alltypesorc_int compute statistics for columns;
analyze table srcpart_date compute statistics for columns;
analyze table srcpart_small compute statistics for columns;

set hive.cbo.returnpath.hiveop=true;

create table srccc as select * from src;

EXPLAIN select  /*+ semi(k, str, 5000)*/ count(*) from srcpart_date k join srcpart_small v on (k.str = v.key1) join alltypesorc_int i on (k.value = i.cstring);
EXPLAIN select  /*+ semi(i, 3000)*/ count(*) from srcpart_date k join srcpart_small v on (k.str = v.key1) join alltypesorc_int i on (v.key1 = i.cstring);

explain select /*+ semi(k, str, 1000)*/ count(*) from srcpart_date k join srcpart_small v on (k.str = v.key1);

set hive.cbo.returnpath.hiveop=false;

explain select /*+ semi(k, 1000)*/ count(*) from srcpart_date k join srcpart_small v on (k.str = v.key1);

set hive.cbo.enable=false;

explain select /*+ semi(k, str, 1000)*/ count(*) from srcpart_date k join srcpart_small v on (k.str = v.key1);

