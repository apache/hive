-- SORT_QUERY_RESULTS

CREATE TABLE myinput1(key int, value int);
LOAD DATA LOCAL INPATH '../../data/files/in8.txt' INTO TABLE myinput1;

-- merging
explain select * from myinput1 a join myinput1 b on a.key<=>b.value;
-- SORT_QUERY_RESULTS
select * from myinput1 a join myinput1 b on a.key<=>b.value;

explain select * from myinput1 a join myinput1 b on a.key<=>b.value join myinput1 c on a.key=c.key;
select * from myinput1 a join myinput1 b on a.key<=>b.value join myinput1 c on a.key=c.key;

explain select * from myinput1 a join myinput1 b on a.key<=>b.value join myinput1 c on a.key<=>c.key;
select * from myinput1 a join myinput1 b on a.key<=>b.value join myinput1 c on a.key<=>c.key;

explain select * from myinput1 a join myinput1 b on a.key<=>b.value AND a.value=b.key join myinput1 c on a.key<=>c.key AND a.value=c.value;

select * from myinput1 a join myinput1 b on a.key<=>b.value AND a.value=b.key join myinput1 c on a.key<=>c.key AND a.value=c.value;

explain select * from myinput1 a join myinput1 b on a.key<=>b.value AND a.value<=>b.key join myinput1 c on a.key<=>c.key AND a.value<=>c.value;
select * from myinput1 a join myinput1 b on a.key<=>b.value AND a.value<=>b.key join myinput1 c on a.key<=>c.key AND a.value<=>c.value;

-- outer joins
SELECT * FROM myinput1 a LEFT OUTER JOIN myinput1 b ON a.key<=>b.value;
SELECT * FROM myinput1 a RIGHT OUTER JOIN myinput1 b ON a.key<=>b.value;
SELECT * FROM myinput1 a FULL OUTER JOIN myinput1 b ON a.key<=>b.value;

-- map joins
SELECT /*+ MAPJOIN(a) */ * FROM myinput1 a JOIN myinput1 b ON a.key<=>b.value;
SELECT /*+ MAPJOIN(b) */ * FROM myinput1 a JOIN myinput1 b ON a.key<=>b.value;

CREATE TABLE smb_input(key int, value int);
LOAD DATA LOCAL INPATH '../../data/files/in4.txt' into table smb_input;
LOAD DATA LOCAL INPATH '../../data/files/in5.txt' into table smb_input;

set hive.enforce.sorting = true;
set hive.enforce.bucketing = true;

-- smbs
CREATE TABLE smb_input1(key int, value int) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;
CREATE TABLE smb_input2(key int, value int) CLUSTERED BY (value) SORTED BY (value) INTO 2 BUCKETS;

from smb_input
insert overwrite table smb_input1 select *
insert overwrite table smb_input2 select *;

SET hive.optimize.bucketmapjoin = true;
SET hive.optimize.bucketmapjoin.sortedmerge = true;
SET hive.input.format = org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;

SELECT /*+ MAPJOIN(a) */ * FROM smb_input1 a JOIN smb_input1 b ON a.key <=> b.key;
SELECT /*+ MAPJOIN(a) */ * FROM smb_input1 a JOIN smb_input1 b ON a.key <=> b.key AND a.value <=> b.value;
SELECT /*+ MAPJOIN(a) */ * FROM smb_input1 a RIGHT OUTER JOIN smb_input1 b ON a.key <=> b.key;
SELECT /*+ MAPJOIN(b) */ * FROM smb_input1 a JOIN smb_input1 b ON a.key <=> b.key;
SELECT /*+ MAPJOIN(b) */ * FROM smb_input1 a LEFT OUTER JOIN smb_input1 b ON a.key <=> b.key;

SELECT /*+ MAPJOIN(a) */ * FROM smb_input1 a JOIN smb_input2 b ON a.key <=> b.value;
SELECT /*+ MAPJOIN(b) */ * FROM smb_input1 a JOIN smb_input2 b ON a.key <=> b.value;
SELECT /*+ MAPJOIN(b) */ * FROM smb_input1 a LEFT OUTER JOIN smb_input2 b ON a.key <=> b.value;
SELECT /*+ MAPJOIN(a) */ * FROM smb_input1 a RIGHT OUTER JOIN smb_input2 b ON a.key <=> b.value;

SELECT /*+ MAPJOIN(a) */ * FROM smb_input2 a JOIN smb_input2 b ON a.value <=> b.value;
SELECT /*+ MAPJOIN(a) */ * FROM smb_input2 a RIGHT OUTER JOIN smb_input2 b ON a.value <=> b.value;
SELECT /*+ MAPJOIN(b) */ * FROM smb_input2 a JOIN smb_input2 b ON a.value <=> b.value;
SELECT /*+ MAPJOIN(b) */ * FROM smb_input2 a LEFT OUTER JOIN smb_input2 b ON a.value <=> b.value;

--HIVE-3315 join predicate transitive
explain select * from myinput1 a join myinput1 b on a.key<=>b.value AND a.key is NULL;
select * from myinput1 a join myinput1 b on a.key<=>b.value AND a.key is NULL;
