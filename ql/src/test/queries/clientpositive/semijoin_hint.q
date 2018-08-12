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
set hive.metastore.aggregate.stats.cache.enabled=false;

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

create table srccc as select * from src;

set hive.cbo.returnpath.hiveop=true;

-- disabling this test case for returnpath true as the aliases in case of union are mangled due to which hints are not excercised.
--explain select /*+ semi(k, str, 5000)*/ count(*) from srcpart_date k join srcpart_small s on (k.str = s.key1)
--        union all
--        select /*+ semi(v, key1, 5000)*/ count(*) from srcpart_date d join srcpart_small v on (d.str = v.key1);

-- Query which creates semijoin
explain select count(*) from srcpart_date k join srcpart_small v on (k.str = v.key1);
-- Skip semijoin by using keyword "None" as argument
explain select /*+ semi(None)*/ count(*) from srcpart_date k join srcpart_small v on (k.str = v.key1);

EXPLAIN select  /*+ semi(srcpart_date, str, v, 5000)*/ count(*) from srcpart_date join srcpart_small v on (srcpart_date.str = v.key1) join alltypesorc_int i on (srcpart_date.value = i.cstring);
EXPLAIN select  /*+ semi(i, cstring, v, 3000)*/ count(*) from srcpart_date k join srcpart_small v on (k.str = v.key1) join alltypesorc_int i on (v.key1 = i.cstring);

explain select /*+ semi(k, str, v, 5000)*/ count(*) from srcpart_date k join srcpart_small v on (k.str = v.key1);

-- This should NOT create a semijoin
explain select /*+ semi(k, str, v, 5000)*/ count(*) from srcpart_date k join srcpart_small v on (k.value = v.key1);

set hive.cbo.returnpath.hiveop=false;

explain select /*+ semi(k, str, s, 5000)*/ count(*) from srcpart_date k join srcpart_small s on (k.str = s.key1)
        union all
        select /*+ semi(v, key1, d, 5000)*/ count(*) from srcpart_date d join srcpart_small v on (d.str = v.key1);

-- Query which creates semijoin
explain select count(*) from srcpart_date k join srcpart_small v on (k.str = v.key1);
-- Skip semijoin by using keyword "None" as argument
explain select /*+ semi(None)*/ count(*) from srcpart_date k join srcpart_small v on (k.str = v.key1);

EXPLAIN select  /*+ semi(srcpart_date, str, v, 5000)*/ count(*) from srcpart_date join srcpart_small v on (srcpart_date.str = v.key1) join alltypesorc_int i on (srcpart_date.value = i.cstring);
EXPLAIN select  /*+ semi(i, cstring, v, 3000)*/ count(*) from srcpart_date k join srcpart_small v on (k.str = v.key1) join alltypesorc_int i on (v.key1 = i.cstring);

explain select /*+ semi(k, str, v, 5000)*/ count(*) from srcpart_date k join srcpart_small v on (k.str = v.key1);

-- This should NOT create a semijoin
explain select /*+ semi(k, str, v, 5000)*/ count(*) from srcpart_date k join srcpart_small v on (k.value = v.key1);



set hive.cbo.enable=false;

explain select /*+ semi(k, str, s, 5000)*/ count(*) from srcpart_date k join srcpart_small s on (k.str = s.key1)
        union all
        select /*+ semi(v, key1, d, 5000)*/ count(*) from srcpart_date d join srcpart_small v on (d.str = v.key1);

-- Query which creates semijoin
explain select count(*) from srcpart_date k join srcpart_small v on (k.str = v.key1);
-- Skip semijoin by using keyword "None" as argument
explain select /*+ semi(None)*/ count(*) from srcpart_date k join srcpart_small v on (k.str = v.key1);

EXPLAIN select  /*+ semi(srcpart_date, str, v, 5000)*/ count(*) from srcpart_date join srcpart_small v on (srcpart_date.str = v.key1) join alltypesorc_int i on (srcpart_date.value = i.cstring);
EXPLAIN select  /*+ semi(i, cstring, v, 3000)*/ count(*) from srcpart_date k join srcpart_small v on (k.str = v.key1) join alltypesorc_int i on (v.key1 = i.cstring);

explain select /*+ semi(k, str, v, 5000)*/ count(*) from srcpart_date k join srcpart_small v on (k.str = v.key1);

-- This should NOT create a semijoin
explain select /*+ semi(k, str, v, 5000)*/ count(*) from srcpart_date k join srcpart_small v on (k.value = v.key1);


-- Make sure hints work with merge
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.explain.user=false;
set hive.merge.cardinality.check=true;

create table acidTbl(a int, b int) clustered by (a) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');
create table nonAcidOrcTbl(a int, b int) clustered by (a) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='false');

--without hint, the semijoin is still made, note the difference in bloom filter entries.
explain merge into acidTbl as t using nonAcidOrcTbl s ON t.a = s.a
WHEN MATCHED AND s.a > 8 THEN DELETE
WHEN MATCHED THEN UPDATE SET b = 7
WHEN NOT MATCHED THEN INSERT VALUES(s.a, s.b);
-- with hint, the bloom filter entries become 1000 due to hint.
explain merge  /*+ semi(s, a, t, 1000)*/  into acidTbl as t using nonAcidOrcTbl s ON t.a = s.a
WHEN MATCHED AND s.a > 8 THEN DELETE
WHEN MATCHED THEN UPDATE SET b = 7
WHEN NOT MATCHED THEN INSERT VALUES(s.a, s.b);