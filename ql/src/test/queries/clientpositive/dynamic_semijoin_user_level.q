--! qt:dataset:srcpart
--! qt:dataset:alltypesorc
SET hive.vectorized.execution.enabled=false;
set hive.explain.user=true;
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
set hive.stats.fetch.column.stats=true;
set hive.llap.memory.oversubscription.max.executors.per.query=0;

-- Create Tables
create table alltypesorc_int_n2 ( cint int, cstring string ) stored as ORC;
create table srcpart_date_n9 (key string, value string) partitioned by (ds string ) stored as ORC;
CREATE TABLE srcpart_small_n4(key1 STRING, value1 STRING) partitioned by (ds string) STORED as ORC;

-- Add Partitions
alter table srcpart_date_n9 add partition (ds = "2008-04-08");
alter table srcpart_date_n9 add partition (ds = "2008-04-09");

alter table srcpart_small_n4 add partition (ds = "2008-04-08");
alter table srcpart_small_n4 add partition (ds = "2008-04-09");

-- Load
insert overwrite table alltypesorc_int_n2 select cint, cstring1 from alltypesorc;
insert overwrite table srcpart_date_n9 partition (ds = "2008-04-08" ) select key, value from srcpart where ds = "2008-04-08";
insert overwrite table srcpart_date_n9 partition (ds = "2008-04-09") select key, value from srcpart where ds = "2008-04-09";
insert overwrite table srcpart_small_n4 partition (ds = "2008-04-09") select key, value from srcpart where ds = "2008-04-09" limit 20;

set hive.tez.dynamic.semijoin.reduction=false;

analyze table alltypesorc_int_n2 compute statistics for columns;
analyze table srcpart_date_n9 compute statistics for columns;
analyze table srcpart_small_n4 compute statistics for columns;

-- single column, single key
set hive.tez.dynamic.semijoin.reduction=true;
EXPLAIN select count(*) from srcpart_date_n9 join srcpart_small_n4 on (srcpart_date_n9.key = srcpart_small_n4.key1);
select count(*) from srcpart_date_n9 join srcpart_small_n4 on (srcpart_date_n9.key = srcpart_small_n4.key1);
set hive.tez.dynamic.semijoin.reduction=true;

-- Mix dynamic partition pruning(DPP) and min/max bloom filter optimizations. Should pick the DPP.
EXPLAIN select count(*) from srcpart_date_n9 join srcpart_small_n4 on (srcpart_date_n9.key = srcpart_small_n4.ds);
select count(*) from srcpart_date_n9 join srcpart_small_n4 on (srcpart_date_n9.key = srcpart_small_n4.ds);
set hive.tez.dynamic.semijoin.reduction=false;

--multiple sources, single key
EXPLAIN select count(*) from srcpart_date_n9 join srcpart_small_n4 on (srcpart_date_n9.key = srcpart_small_n4.key1) join alltypesorc_int_n2 on (srcpart_small_n4.key1 = alltypesorc_int_n2.cstring);
select count(*) from srcpart_date_n9 join srcpart_small_n4 on (srcpart_date_n9.key = srcpart_small_n4.key1) join alltypesorc_int_n2 on (srcpart_small_n4.key1 = alltypesorc_int_n2.cstring);
set hive.tez.dynamic.semijoin.reduction=true;
EXPLAIN select count(*) from srcpart_date_n9 join srcpart_small_n4 on (srcpart_date_n9.key = srcpart_small_n4.key1) join alltypesorc_int_n2 on (srcpart_small_n4.key1 = alltypesorc_int_n2.cstring);
select count(*) from srcpart_date_n9 join srcpart_small_n4 on (srcpart_date_n9.key = srcpart_small_n4.key1) join alltypesorc_int_n2 on (srcpart_small_n4.key1 = alltypesorc_int_n2.cstring);
set hive.tez.dynamic.semijoin.reduction=false;

-- single source, multiple keys
EXPLAIN select count(*) from srcpart_date_n9 join srcpart_small_n4 on (srcpart_date_n9.key = srcpart_small_n4.key1 and srcpart_date_n9.value = srcpart_small_n4.value1);
select count(*) from srcpart_date_n9 join srcpart_small_n4 on (srcpart_date_n9.key = srcpart_small_n4.key1 and srcpart_date_n9.value = srcpart_small_n4.value1);
set hive.tez.dynamic.semijoin.reduction=true;
EXPLAIN select count(*) from srcpart_date_n9 join srcpart_small_n4 on (srcpart_date_n9.key = srcpart_small_n4.key1 and srcpart_date_n9.value = srcpart_small_n4.value1);
select count(*) from srcpart_date_n9 join srcpart_small_n4 on (srcpart_date_n9.key = srcpart_small_n4.key1 and srcpart_date_n9.value = srcpart_small_n4.value1);
set hive.tez.dynamic.semijoin.reduction=false;

-- multiple sources, different  keys
EXPLAIN select count(*) from srcpart_date_n9 join srcpart_small_n4 on (srcpart_date_n9.key = srcpart_small_n4.key1) join alltypesorc_int_n2 on (srcpart_date_n9.value = alltypesorc_int_n2.cstring);
select count(*) from srcpart_date_n9 join srcpart_small_n4 on (srcpart_date_n9.key = srcpart_small_n4.key1) join alltypesorc_int_n2 on (srcpart_date_n9.value = alltypesorc_int_n2.cstring);
set hive.tez.dynamic.semijoin.reduction=true;
EXPLAIN select count(*) from srcpart_date_n9 join srcpart_small_n4 on (srcpart_date_n9.key = srcpart_small_n4.key1) join alltypesorc_int_n2 on (srcpart_date_n9.value = alltypesorc_int_n2.cstring);
select count(*) from srcpart_date_n9 join srcpart_small_n4 on (srcpart_date_n9.key = srcpart_small_n4.key1) join alltypesorc_int_n2 on (srcpart_date_n9.value = alltypesorc_int_n2.cstring);

-- Explain extended to verify fast start for Reducer in semijoin branch
EXPLAIN extended select count(*) from srcpart_date_n9 join srcpart_small_n4 on (srcpart_date_n9.key = srcpart_small_n4.key1);
set hive.tez.dynamic.semijoin.reduction=false;

-- With Mapjoins.
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=100000000000;

EXPLAIN select count(*) from srcpart_date_n9 join srcpart_small_n4 on (srcpart_date_n9.key = srcpart_small_n4.key1);
select count(*) from srcpart_date_n9 join srcpart_small_n4 on (srcpart_date_n9.key = srcpart_small_n4.key1);
set hive.tez.dynamic.semijoin.reduction=true;
EXPLAIN select count(*) from srcpart_date_n9 join srcpart_small_n4 on (srcpart_date_n9.key = srcpart_small_n4.key1);
select count(*) from srcpart_date_n9 join srcpart_small_n4 on (srcpart_date_n9.key = srcpart_small_n4.key1);
set hive.tez.dynamic.semijoin.reduction=false;

-- multiple sources, different  keys
EXPLAIN select count(*) from srcpart_date_n9 join srcpart_small_n4 on (srcpart_date_n9.key = srcpart_small_n4.key1) join alltypesorc_int_n2 on (srcpart_date_n9.value = alltypesorc_int_n2.cstring);
select count(*) from srcpart_date_n9 join srcpart_small_n4 on (srcpart_date_n9.key = srcpart_small_n4.key1) join alltypesorc_int_n2 on (srcpart_date_n9.value = alltypesorc_int_n2.cstring);
set hive.tez.dynamic.semijoin.reduction=true;
EXPLAIN select count(*) from srcpart_date_n9 join srcpart_small_n4 on (srcpart_date_n9.key = srcpart_small_n4.key1) join alltypesorc_int_n2 on (srcpart_date_n9.value = alltypesorc_int_n2.cstring);
select count(*) from srcpart_date_n9 join srcpart_small_n4 on (srcpart_date_n9.key = srcpart_small_n4.key1) join alltypesorc_int_n2 on (srcpart_date_n9.value = alltypesorc_int_n2.cstring);
--set hive.tez.dynamic.semijoin.reduction=false;

-- With unions
explain select * from alltypesorc_int_n2 join
                                      (select srcpart_date_n9.key as key from srcpart_date_n9
                                       union all
                                       select srcpart_small_n4.key1 as key from srcpart_small_n4) unionsrc on (alltypesorc_int_n2.cstring = unionsrc.key);


drop table srcpart_date_n9;
drop table srcpart_small_n4;
drop table alltypesorc_int_n2;
