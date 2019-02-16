--! qt:dataset:srcpart
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
set hive.disable.unsafe.external.table.operations=true;

-- Create Tables
create table srcpart_date_n1 (key string, value string) partitioned by (ds string ) stored as ORC;
CREATE TABLE srcpart_small_n0(key1 STRING, value1 STRING) partitioned by (ds string) STORED as ORC;
CREATE TABLE srcpart_medium_n0(key2 STRING, value2 STRING) partitioned by (ds string) STORED as ORC;
create external table srcpart_date_ext (key string, value string) partitioned by (ds string ) stored as ORC;
CREATE external TABLE srcpart_small_ext(key1 STRING, value1 STRING) partitioned by (ds string) STORED as ORC;

-- Add Partitions
alter table srcpart_date_n1 add partition (ds = "2008-04-08");
alter table srcpart_date_n1 add partition (ds = "2008-04-09");

alter table srcpart_small_n0 add partition (ds = "2008-04-08");
alter table srcpart_small_n0 add partition (ds = "2008-04-09");

alter table srcpart_medium_n0 add partition (ds = "2008-04-08");

alter table srcpart_date_ext add partition (ds = "2008-04-08");
alter table srcpart_date_ext add partition (ds = "2008-04-09");

alter table srcpart_small_ext add partition (ds = "2008-04-08");
alter table srcpart_small_ext add partition (ds = "2008-04-09");

-- Load
insert overwrite table srcpart_date_n1 partition (ds = "2008-04-08" ) select key, value from srcpart where ds = "2008-04-08";
insert overwrite table srcpart_date_n1 partition (ds = "2008-04-09") select key, value from srcpart where ds = "2008-04-09";
insert overwrite table srcpart_small_n0 partition (ds = "2008-04-09") select key, value from srcpart where ds = "2008-04-09" limit 20;
insert overwrite table srcpart_medium_n0 partition (ds = "2008-04-08") select key, value from srcpart where ds = "2008-04-09" limit 50;

insert overwrite table srcpart_date_ext partition (ds = "2008-04-08" ) select key, value from srcpart where ds = "2008-04-08";
insert overwrite table srcpart_date_ext partition (ds = "2008-04-09") select key, value from srcpart where ds = "2008-04-09";
insert overwrite table srcpart_small_ext partition (ds = "2008-04-09") select key, value from srcpart where ds = "2008-04-09" limit 20;

analyze table srcpart_date_n1 compute statistics for columns;
analyze table srcpart_small_n0 compute statistics for columns;
analyze table srcpart_medium_n0 compute statistics for columns;

analyze table srcpart_date_ext compute statistics for columns;
analyze table srcpart_small_ext compute statistics for columns;


-- single column, single key
set test.comment=This query should use semijoin reduction optimization;
set test.comment;
EXPLAIN select count(*) from srcpart_date_n1 join srcpart_small_n0 on (srcpart_date_n1.key = srcpart_small_n0.key1);
-- multiple sources, single key
EXPLAIN select count(*) from srcpart_date_n1 join srcpart_small_n0 on (srcpart_date_n1.key = srcpart_small_n0.key1) join srcpart_medium_n0 on (srcpart_medium_n0.key2 = srcpart_date_n1.key);

set test.comment=Big table is external table - no semijoin reduction opt;
set test.comment;
EXPLAIN select count(*) from srcpart_date_ext join srcpart_small_n0 on (srcpart_date_ext.key = srcpart_small_n0.key1);

set test.comment=Small table is external table - no semijoin reduction opt;
set test.comment;
EXPLAIN select count(*) from srcpart_date_n1 join srcpart_small_ext on (srcpart_date_n1.key = srcpart_small_ext.key1);

set test.comment=Small table is external table - no semijoin reduction opt for ext table but semijoin reduction opt for regular table;
set test.comment;
EXPLAIN select count(*) from srcpart_date_n1 join srcpart_small_ext on (srcpart_date_n1.key = srcpart_small_ext.key1)  join srcpart_medium_n0 on (srcpart_medium_n0.key2 = srcpart_date_n1.key);

