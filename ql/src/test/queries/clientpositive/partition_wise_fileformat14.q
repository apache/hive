--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.exec.reducers.max = 1;

CREATE TABLE tbl1_n8(key int, value string) PARTITIONED by (ds string)
CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS STORED AS rcfile;
CREATE TABLE tbl2_n7(key int, value string) PARTITIONED by (ds string)
CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS STORED AS rcfile;

alter table tbl1_n8 set serde 'org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe';
alter table tbl2_n7 set serde 'org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe';

insert overwrite table tbl1_n8 partition (ds='1') select * from src where key < 10;
insert overwrite table tbl2_n7 partition (ds='1') select * from src where key < 10;

alter table tbl1_n8 change key key int;
insert overwrite table tbl1_n8 partition (ds='2') select * from src where key < 10;

alter table tbl1_n8 change key key string;

-- The subquery itself is being map-joined. Multiple partitions of tbl1_n8 with different schemas are being read for tbl2_n7
select /*+mapjoin(subq1)*/ count(*) from 
  (select a.key as key, a.value as value from tbl1_n8 a where key < 6) subq1 
    join
  (select a.key as key, a.value as value from tbl2_n7 a where key < 6) subq2
  on subq1.key = subq2.key;

set hive.optimize.bucketmapjoin = true;
set hive.input.format = org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;

-- The subquery itself is being map-joined. Since the sub-query only contains selects and filters, it should 
-- be converted to a bucketized mapside join. Multiple partitions of tbl1_n8 with different schemas are being read for each
-- bucket of tbl2_n7
select /*+mapjoin(subq1)*/ count(*) from 
  (select a.key as key, a.value as value from tbl1_n8 a where key < 6) subq1 
    join
  (select a.key as key, a.value as value from tbl2_n7 a where key < 6) subq2
  on subq1.key = subq2.key;

set hive.optimize.bucketmapjoin.sortedmerge = true;

-- The subquery itself is being map-joined. Since the sub-query only contains selects and filters, it should 
-- be converted to a sort-merge join. Multiple partitions of tbl1_n8 with different schemas are being read for a
-- given file of tbl2_n7
select /*+mapjoin(subq1)*/ count(*) from 
  (select a.key as key, a.value as value from tbl1_n8 a where key < 6) subq1 
    join
  (select a.key as key, a.value as value from tbl2_n7 a where key < 6) subq2
  on subq1.key = subq2.key;

-- Since the join key is modified by the sub-query, neither sort-merge join not bucketized map-side
-- join should be performed.  Multiple partitions of tbl1_n8 with different schemas are being read for tbl2_n7
select /*+mapjoin(subq1)*/ count(*) from 
  (select a.key+1 as key, concat(a.value, a.value) as value from tbl1_n8 a) subq1 
    join
  (select a.key+1 as key, concat(a.value, a.value) as value from tbl2_n7 a) subq2
  on subq1.key = subq2.key;
