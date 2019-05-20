--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
;

set hive.exec.reducers.max = 1;

CREATE TABLE tbl1_n7(key int, value string) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;
CREATE TABLE tbl2_n6(key int, value string) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;

insert overwrite table tbl1_n7 select * from src where key < 20;
insert overwrite table tbl2_n6 select * from src where key < 10;

set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;
set hive.auto.convert.sortmerge.join.to.mapjoin=true;
set hive.auto.convert.sortmerge.join=true;
set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;
set hive.auto.convert.join=true;
-- disable hash joins
set hive.auto.convert.join.noconditionaltask.size=10;

-- Since tbl1_n7 is the bigger table, tbl1_n7 Left Outer Join tbl2_n6 can be performed
explain
select count(*) FROM tbl1_n7 a LEFT OUTER JOIN tbl2_n6 b ON a.key = b.key;
select count(*) FROM tbl1_n7 a LEFT OUTER JOIN tbl2_n6 b ON a.key = b.key;

insert overwrite table tbl2_n6 select * from src where key < 200;

-- Since tbl2_n6 is the bigger table, tbl1_n7 Right Outer Join tbl2_n6 can be performed
explain
select count(*) FROM tbl1_n7 a RIGHT OUTER JOIN tbl2_n6 b ON a.key = b.key;
select count(*) FROM tbl1_n7 a RIGHT OUTER JOIN tbl2_n6 b ON a.key = b.key;
