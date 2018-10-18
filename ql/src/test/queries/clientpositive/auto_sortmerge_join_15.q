--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
;

set hive.exec.reducers.max = 1;

CREATE TABLE tbl1_n11(key int, value string) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;
CREATE TABLE tbl2_n10(key int, value string) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;

insert overwrite table tbl1_n11 select * from src where key < 20;
insert overwrite table tbl2_n10 select * from src where key < 10;

set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;
set hive.auto.convert.sortmerge.join.to.mapjoin=true;
set hive.auto.convert.sortmerge.join=true;
set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;
set hive.auto.convert.join=true;
-- disable hash joins
set hive.auto.convert.join.noconditionaltask.size=10;

explain
select count(*) FROM tbl1_n11 a LEFT OUTER JOIN tbl2_n10 b ON a.key = b.key;

explain
select count(*) FROM tbl1_n11 a RIGHT OUTER JOIN tbl2_n10 b ON a.key = b.key;
