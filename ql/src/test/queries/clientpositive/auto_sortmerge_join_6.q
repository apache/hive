set hive.enforce.bucketing = true;
set hive.enforce.sorting = true;
set hive.exec.reducers.max = 1;

CREATE TABLE tbl1(key int, value string) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;
CREATE TABLE tbl2(key int, value string) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;
CREATE TABLE tbl3(key int, value string) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;
CREATE TABLE tbl4(key int, value string) CLUSTERED BY (value) SORTED BY (value) INTO 2 BUCKETS;

insert overwrite table tbl1 select * from src;
insert overwrite table tbl2 select * from src;
insert overwrite table tbl3 select * from src;
insert overwrite table tbl4 select * from src;

set hive.auto.convert.sortmerge.join=true;
set hive.optimize.bucketmapjoin = true;
set hive.optimize.bucketmapjoin.sortedmerge = true;
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=200;

-- A SMB join is being followed by a regular join on a non-bucketed table on a different key
explain select count(*) FROM tbl1 a JOIN tbl2 b ON a.key = b.key join src c on c.value = a.value;
select count(*) FROM tbl1 a JOIN tbl2 b ON a.key = b.key join src c on c.value = a.value;

-- A SMB join is being followed by a regular join on a non-bucketed table on the same key
explain select count(*) FROM tbl1 a JOIN tbl2 b ON a.key = b.key join src c on c.key = a.key;
select count(*) FROM tbl1 a JOIN tbl2 b ON a.key = b.key join src c on c.key = a.key;

-- A SMB join is being followed by a regular join on a bucketed table on the same key
explain select count(*) FROM tbl1 a JOIN tbl2 b ON a.key = b.key join tbl3 c on c.key = a.key;
select count(*) FROM tbl1 a JOIN tbl2 b ON a.key = b.key join tbl3 c on c.key = a.key;

-- A SMB join is being followed by a regular join on a bucketed table on a different key
explain select count(*) FROM tbl1 a JOIN tbl2 b ON a.key = b.key join tbl4 c on c.value = a.value;
select count(*) FROM tbl1 a JOIN tbl2 b ON a.key = b.key join tbl4 c on c.value = a.value;
