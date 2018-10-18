--! qt:dataset:srcpart
--! qt:dataset:part
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
create table srcpart_date_n5 (key string, value string) partitioned by (ds string ) stored as ORC;
CREATE TABLE srcpart_small_n1(key1 STRING, value1 STRING) partitioned by (ds string) STORED as ORC;

-- Add Partitions
alter table srcpart_date_n5 add partition (ds = "2008-04-08");
alter table srcpart_date_n5 add partition (ds = "2008-04-09");

alter table srcpart_small_n1 add partition (ds = "2008-04-08");
alter table srcpart_small_n1 add partition (ds = "2008-04-09");

-- Load
insert overwrite table srcpart_date_n5 partition (ds = "2008-04-08" ) select key, value from srcpart where ds = "2008-04-08";
insert overwrite table srcpart_date_n5 partition (ds = "2008-04-09") select key, value from srcpart where ds = "2008-04-09";
insert overwrite table srcpart_small_n1 partition (ds = "2008-04-09") select key, value from srcpart where ds = "2008-04-09" limit 20;


analyze table srcpart_date_n5 compute statistics for columns;
analyze table srcpart_small_n1 compute statistics for columns;

set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=100000000000;

--HIVE-17475
EXPLAIN select /*+ mapjoin(None)*/ count(*) from srcpart_date_n5 join srcpart_small_n1 on (srcpart_date_n5.key = srcpart_small_n1.key1);
EXPLAIN select count(*) from srcpart_date_n5 join srcpart_small_n1 on (srcpart_date_n5.key = srcpart_small_n1.key1);


-- Ensure that hint works even with CBO on, on a query with subquery.
create table tnull_n1(i int, c char(2));
insert into tnull_n1 values(NULL, NULL), (NULL, NULL);

create table tempty_n1(c char(2));

CREATE TABLE part_null_n1(
p_partkey INT,
p_name STRING,
p_mfgr STRING,
p_brand STRING,
p_type STRING,
p_size INT,
p_container STRING,
p_retailprice DOUBLE,
p_comment STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
;

LOAD DATA LOCAL INPATH '../../data/files/part_tiny_nulls.txt' overwrite into table part_null_n1;

insert into part_null_n1 values(78487,NULL,'Manufacturer#6','Brand#52','LARGE BRUSHED BRASS', 23, 'MED BAG',1464.48,'hely blith');

explain select /*+ mapjoin(None)*/ * from part where p_name = (select p_name from part_null_n1 where p_name is null);
explain select * from part where p_name = (select p_name from part_null_n1 where p_name is null);
