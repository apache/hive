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
create table alltypesorc_int ( cint int, cstring string ) stored as ORC;
create table srcpart_date (key string, value string) partitioned by (ds string ) stored as ORC;
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
insert overwrite table srcpart_small partition (ds = "2008-04-09") select key, value from srcpart where ds = "2008-04-09" limit 20;

set hive.tez.dynamic.semijoin.reduction=false;

analyze table alltypesorc_int compute statistics for columns;
analyze table srcpart_date compute statistics for columns;
analyze table srcpart_small compute statistics for columns;

-- single column, single key
EXPLAIN select count(*) from srcpart_date join srcpart_small on (srcpart_date.key = srcpart_small.key1);
select count(*) from srcpart_date join srcpart_small on (srcpart_date.key = srcpart_small.key1);
set hive.tez.dynamic.semijoin.reduction=true;
EXPLAIN select count(*) from srcpart_date join srcpart_small on (srcpart_date.key = srcpart_small.key1);
select count(*) from srcpart_date join srcpart_small on (srcpart_date.key = srcpart_small.key1);
set hive.tez.dynamic.semijoin.reduction=true;

-- Mix dynamic partition pruning(DPP) and min/max bloom filter optimizations. Should pick the DPP.
EXPLAIN select count(*) from srcpart_date join srcpart_small on (srcpart_date.key = srcpart_small.ds);
select count(*) from srcpart_date join srcpart_small on (srcpart_date.key = srcpart_small.ds);
set hive.tez.dynamic.semijoin.reduction=false;

--multiple sources, single key
EXPLAIN select count(*) from srcpart_date join srcpart_small on (srcpart_date.key = srcpart_small.key1) join alltypesorc_int on (srcpart_small.key1 = alltypesorc_int.cstring);
select count(*) from srcpart_date join srcpart_small on (srcpart_date.key = srcpart_small.key1) join alltypesorc_int on (srcpart_small.key1 = alltypesorc_int.cstring);
set hive.tez.dynamic.semijoin.reduction=true;
EXPLAIN select count(*) from srcpart_date join srcpart_small on (srcpart_date.key = srcpart_small.key1) join alltypesorc_int on (srcpart_small.key1 = alltypesorc_int.cstring);
select count(*) from srcpart_date join srcpart_small on (srcpart_date.key = srcpart_small.key1) join alltypesorc_int on (srcpart_small.key1 = alltypesorc_int.cstring);
set hive.tez.dynamic.semijoin.reduction=false;

-- single source, multiple keys
EXPLAIN select count(*) from srcpart_date join srcpart_small on (srcpart_date.key = srcpart_small.key1 and srcpart_date.value = srcpart_small.value1);
select count(*) from srcpart_date join srcpart_small on (srcpart_date.key = srcpart_small.key1 and srcpart_date.value = srcpart_small.value1);
set hive.tez.dynamic.semijoin.reduction=true;
EXPLAIN select count(*) from srcpart_date join srcpart_small on (srcpart_date.key = srcpart_small.key1 and srcpart_date.value = srcpart_small.value1);
select count(*) from srcpart_date join srcpart_small on (srcpart_date.key = srcpart_small.key1 and srcpart_date.value = srcpart_small.value1);
set hive.tez.dynamic.semijoin.reduction=false;

-- multiple sources, different  keys
EXPLAIN select count(*) from srcpart_date join srcpart_small on (srcpart_date.key = srcpart_small.key1) join alltypesorc_int on (srcpart_date.value = alltypesorc_int.cstring);
select count(*) from srcpart_date join srcpart_small on (srcpart_date.key = srcpart_small.key1) join alltypesorc_int on (srcpart_date.value = alltypesorc_int.cstring);
set hive.tez.dynamic.semijoin.reduction=true;
EXPLAIN select count(*) from srcpart_date join srcpart_small on (srcpart_date.key = srcpart_small.key1) join alltypesorc_int on (srcpart_date.value = alltypesorc_int.cstring);
select count(*) from srcpart_date join srcpart_small on (srcpart_date.key = srcpart_small.key1) join alltypesorc_int on (srcpart_date.value = alltypesorc_int.cstring);

-- Explain extended to verify fast start for Reducer in semijoin branch
EXPLAIN extended select count(*) from srcpart_date join srcpart_small on (srcpart_date.key = srcpart_small.key1);
set hive.tez.dynamic.semijoin.reduction=false;

-- With Mapjoins, there shouldn't be any semijoin parallel to mapjoin.
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=100000000000;

EXPLAIN select count(*) from srcpart_date join srcpart_small on (srcpart_date.key = srcpart_small.key1);
select count(*) from srcpart_date join srcpart_small on (srcpart_date.key = srcpart_small.key1);
set hive.tez.dynamic.semijoin.reduction=true;
EXPLAIN select count(*) from srcpart_date join srcpart_small on (srcpart_date.key = srcpart_small.key1);
select count(*) from srcpart_date join srcpart_small on (srcpart_date.key = srcpart_small.key1);
set hive.tez.dynamic.semijoin.reduction.for.mapjoin=true;
-- Enable semijoin parallel to mapjoins.
EXPLAIN select count(*) from srcpart_date join srcpart_small on (srcpart_date.key = srcpart_small.key1);
select count(*) from srcpart_date join srcpart_small on (srcpart_date.key = srcpart_small.key1);
set hive.tez.dynamic.semijoin.reduction.for.mapjoin=false;
set hive.tez.dynamic.semijoin.reduction=false;

-- multiple sources, different  keys
EXPLAIN select count(*) from srcpart_date join srcpart_small on (srcpart_date.key = srcpart_small.key1) join alltypesorc_int on (srcpart_date.value = alltypesorc_int.cstring);
select count(*) from srcpart_date join srcpart_small on (srcpart_date.key = srcpart_small.key1) join alltypesorc_int on (srcpart_date.value = alltypesorc_int.cstring);
set hive.tez.dynamic.semijoin.reduction=true;
EXPLAIN select count(*) from srcpart_date join srcpart_small on (srcpart_date.key = srcpart_small.key1) join alltypesorc_int on (srcpart_date.value = alltypesorc_int.cstring);
select count(*) from srcpart_date join srcpart_small on (srcpart_date.key = srcpart_small.key1) join alltypesorc_int on (srcpart_date.value = alltypesorc_int.cstring);
set hive.tez.dynamic.semijoin.reduction.for.mapjoin=true;
-- Enable semijoin parallel to mapjoins.
EXPLAIN select count(*) from srcpart_date join srcpart_small on (srcpart_date.key = srcpart_small.key1) join alltypesorc_int on (srcpart_date.value = alltypesorc_int.cstring);
select count(*) from srcpart_date join srcpart_small on (srcpart_date.key = srcpart_small.key1) join alltypesorc_int on (srcpart_date.value = alltypesorc_int.cstring);
set hive.tez.dynamic.semijoin.reduction.for.mapjoin=false;
-- HIVE-17323 - with DPP, the 1st mapjoin is on a map with DPP and 2nd mapjoin is on a map which had semijoin but still removed.
create table alltypesorc_int40 as select * from alltypesorc_int limit 40;
set hive.tez.dynamic.semijoin.reduction=false;
EXPLAIN select count(*) from srcpart_date join srcpart_small on (srcpart_date.ds = srcpart_small.ds) join alltypesorc_int40 on (srcpart_date.value = alltypesorc_int40.cstring);
select count(*) from srcpart_date join srcpart_small on (srcpart_date.ds = srcpart_small.ds) join alltypesorc_int40 on (srcpart_date.value = alltypesorc_int40.cstring);
set hive.tez.dynamic.semijoin.reduction=true;
EXPLAIN select count(*) from srcpart_date join srcpart_small on (srcpart_date.ds = srcpart_small.ds) join alltypesorc_int40 on (srcpart_date.value = alltypesorc_int40.cstring);
select count(*) from srcpart_date join srcpart_small on (srcpart_date.ds = srcpart_small.ds) join alltypesorc_int40 on (srcpart_date.value = alltypesorc_int40.cstring);

-- HIVE-17399
create table srcpart_small10 as select * from srcpart_small limit 10;
analyze table srcpart_small10 compute statistics for columns;
set hive.tez.dynamic.semijoin.reduction=false;
EXPLAIN select count(*) from srcpart_small10, srcpart_small, srcpart_date where srcpart_small.key1 = srcpart_small10.key1 and srcpart_date.ds = srcpart_small.ds;
select count(*) from srcpart_small10, srcpart_small, srcpart_date where srcpart_small.key1 = srcpart_small10.key1 and srcpart_date.ds = srcpart_small.ds;
set hive.tez.dynamic.semijoin.reduction=true;
set hive.llap.object.cache.enabled=false;
EXPLAIN select count(*) from srcpart_small10, srcpart_small, srcpart_date where srcpart_small.key1 = srcpart_small10.key1 and srcpart_date.ds = srcpart_small.ds;
select count(*) from srcpart_small10, srcpart_small, srcpart_date where srcpart_small.key1 = srcpart_small10.key1 and srcpart_date.ds = srcpart_small.ds;

-- HIVE-17936
set hive.tez.dynamic.semijoin.reduction.for.dpp.factor = 0.75;
EXPLAIN select count(*) from srcpart_small10, srcpart_small, srcpart_date where srcpart_small.key1 = srcpart_small10.key1 and srcpart_date.ds = srcpart_small.ds;
-- semijoin branch should be removed.
set hive.tez.dynamic.semijoin.reduction.for.dpp.factor = 0.4;
EXPLAIN select count(*) from srcpart_small10, srcpart_small, srcpart_date where srcpart_small.key1 = srcpart_small10.key1 and srcpart_date.ds = srcpart_small.ds;

-- With unions
explain select * from alltypesorc_int join
                                      (select srcpart_date.key as key from srcpart_date
                                       union all
                                       select srcpart_small.key1 as key from srcpart_small) unionsrc on (alltypesorc_int.cstring = unionsrc.key);



drop table srcpart_date;
drop table srcpart_small;
drop table alltypesorc_int;
