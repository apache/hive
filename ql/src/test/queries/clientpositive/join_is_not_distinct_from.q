set hive.explain.user=false;
-- SORT_QUERY_RESULTS

CREATE TABLE myinput1_n10(key int, value int);
LOAD DATA LOCAL INPATH '../../data/files/in8.txt' INTO TABLE myinput1_n10;

-- merging
explain select * from myinput1_n10 a join myinput1_n10 b on a.key is not distinct from b.value;
-- SORT_QUERY_RESULTS
select * from myinput1_n10 a join myinput1_n10 b on a.key is not distinct from b.value;

explain select * from myinput1_n10 a join myinput1_n10 b on a.key is not distinct from b.value join myinput1_n10 c on a.key=c.key;
select * from myinput1_n10 a join myinput1_n10 b on a.key is not distinct from b.value join myinput1_n10 c on a.key=c.key;

explain select * from myinput1_n10 a join myinput1_n10 b on a.key is not distinct from b.value join myinput1_n10 c on a.key is not distinct from c.key;
select * from myinput1_n10 a join myinput1_n10 b on a.key is not distinct from b.value join myinput1_n10 c on a.key is not distinct from c.key;

explain select * from myinput1_n10 a join myinput1_n10 b on a.key is not distinct from b.value AND a.value=b.key join myinput1_n10 c on a.key is not distinct from c.key AND a.value=c.value;

select * from myinput1_n10 a join myinput1_n10 b on a.key is not distinct from b.value AND a.value=b.key join myinput1_n10 c on a.key is not distinct from c.key AND a.value=c.value;

explain select * from myinput1_n10 a join myinput1_n10 b on a.key is not distinct from b.value AND a.value is not distinct from b.key join myinput1_n10 c on a.key is not distinct from c.key AND a.value is not distinct from c.value;
select * from myinput1_n10 a join myinput1_n10 b on a.key is not distinct from b.value AND a.value is not distinct from b.key join myinput1_n10 c on a.key is not distinct from c.key AND a.value is not distinct from c.value;

-- outer joins
SELECT * FROM myinput1_n10 a LEFT OUTER JOIN myinput1_n10 b ON a.key is not distinct from b.value;
SELECT * FROM myinput1_n10 a RIGHT OUTER JOIN myinput1_n10 b ON a.key is not distinct from b.value;
SELECT * FROM myinput1_n10 a FULL OUTER JOIN myinput1_n10 b ON a.key is not distinct from b.value;

-- map joins
SELECT /*+ MAPJOIN(a) */ * FROM myinput1_n10 a JOIN myinput1_n10 b ON a.key is not distinct from b.value;
SELECT /*+ MAPJOIN(b) */ * FROM myinput1_n10 a JOIN myinput1_n10 b ON a.key is not distinct from b.value;

CREATE TABLE smb_input_n2(key int, value int);
LOAD DATA LOCAL INPATH '../../data/files/in4.txt' into table smb_input_n2;
LOAD DATA LOCAL INPATH '../../data/files/in5.txt' into table smb_input_n2;


;

-- smbs
CREATE TABLE smb_input1_n5(key int, value int) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;
CREATE TABLE smb_input2_n5(key int, value int) CLUSTERED BY (value) SORTED BY (value) INTO 2 BUCKETS;

from smb_input_n2
insert overwrite table smb_input1_n5 select *
insert overwrite table smb_input2_n5 select *;

SET hive.optimize.bucketmapjoin = true;
SET hive.optimize.bucketmapjoin.sortedmerge = true;
SET hive.input.format = org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;

SELECT /*+ MAPJOIN(a) */ * FROM smb_input1_n5 a JOIN smb_input1_n5 b ON a.key  is not distinct from  b.key;
SELECT /*+ MAPJOIN(a) */ * FROM smb_input1_n5 a JOIN smb_input1_n5 b ON a.key  is not distinct from  b.key AND a.value  is not distinct from  b.value;
SELECT /*+ MAPJOIN(a) */ * FROM smb_input1_n5 a RIGHT OUTER JOIN smb_input1_n5 b ON a.key  is not distinct from  b.key;
SELECT /*+ MAPJOIN(b) */ * FROM smb_input1_n5 a JOIN smb_input1_n5 b ON a.key  is not distinct from  b.key;
SELECT /*+ MAPJOIN(b) */ * FROM smb_input1_n5 a LEFT OUTER JOIN smb_input1_n5 b ON a.key  is not distinct from  b.key;

SELECT /*+ MAPJOIN(a) */ * FROM smb_input1_n5 a JOIN smb_input2_n5 b ON a.key  is not distinct from  b.value;
SELECT /*+ MAPJOIN(b) */ * FROM smb_input1_n5 a JOIN smb_input2_n5 b ON a.key  is not distinct from  b.value;
SELECT /*+ MAPJOIN(b) */ * FROM smb_input1_n5 a LEFT OUTER JOIN smb_input2_n5 b ON a.key  is not distinct from  b.value;
SELECT /*+ MAPJOIN(a) */ * FROM smb_input1_n5 a RIGHT OUTER JOIN smb_input2_n5 b ON a.key  is not distinct from  b.value;

SELECT /*+ MAPJOIN(a) */ * FROM smb_input2_n5 a JOIN smb_input2_n5 b ON a.value  is not distinct from  b.value;
SELECT /*+ MAPJOIN(a) */ * FROM smb_input2_n5 a RIGHT OUTER JOIN smb_input2_n5 b ON a.value  is not distinct from  b.value;
SELECT /*+ MAPJOIN(b) */ * FROM smb_input2_n5 a JOIN smb_input2_n5 b ON a.value  is not distinct from  b.value;
SELECT /*+ MAPJOIN(b) */ * FROM smb_input2_n5 a LEFT OUTER JOIN smb_input2_n5 b ON a.value  is not distinct from  b.value;

--HIVE-3315 join predicate transitive
explain select * from myinput1_n10 a join myinput1_n10 b on a.key is not distinct from b.value AND a.key is NULL;
select * from myinput1_n10 a join myinput1_n10 b on a.key is not distinct from b.value AND a.key is NULL;
