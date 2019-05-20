set hive.explain.user=false;
-- SORT_QUERY_RESULTS

CREATE TABLE myinput1_n9(key int, value int);
LOAD DATA LOCAL INPATH '../../data/files/in8.txt' INTO TABLE myinput1_n9;

-- merging
explain select * from myinput1_n9 a join myinput1_n9 b on a.key<=>b.value;
-- SORT_QUERY_RESULTS
select * from myinput1_n9 a join myinput1_n9 b on a.key<=>b.value;

explain select * from myinput1_n9 a join myinput1_n9 b on a.key<=>b.value join myinput1_n9 c on a.key=c.key;
select * from myinput1_n9 a join myinput1_n9 b on a.key<=>b.value join myinput1_n9 c on a.key=c.key;

explain select * from myinput1_n9 a join myinput1_n9 b on a.key<=>b.value join myinput1_n9 c on a.key<=>c.key;
select * from myinput1_n9 a join myinput1_n9 b on a.key<=>b.value join myinput1_n9 c on a.key<=>c.key;

explain select * from myinput1_n9 a join myinput1_n9 b on a.key<=>b.value AND a.value=b.key join myinput1_n9 c on a.key<=>c.key AND a.value=c.value;

select * from myinput1_n9 a join myinput1_n9 b on a.key<=>b.value AND a.value=b.key join myinput1_n9 c on a.key<=>c.key AND a.value=c.value;

explain select * from myinput1_n9 a join myinput1_n9 b on a.key<=>b.value AND a.value<=>b.key join myinput1_n9 c on a.key<=>c.key AND a.value<=>c.value;
select * from myinput1_n9 a join myinput1_n9 b on a.key<=>b.value AND a.value<=>b.key join myinput1_n9 c on a.key<=>c.key AND a.value<=>c.value;

-- outer joins
SELECT * FROM myinput1_n9 a LEFT OUTER JOIN myinput1_n9 b ON a.key<=>b.value;
SELECT * FROM myinput1_n9 a RIGHT OUTER JOIN myinput1_n9 b ON a.key<=>b.value;
SELECT * FROM myinput1_n9 a FULL OUTER JOIN myinput1_n9 b ON a.key<=>b.value;

-- map joins
SELECT /*+ MAPJOIN(a) */ * FROM myinput1_n9 a JOIN myinput1_n9 b ON a.key<=>b.value;
SELECT /*+ MAPJOIN(b) */ * FROM myinput1_n9 a JOIN myinput1_n9 b ON a.key<=>b.value;

CREATE TABLE smb_input_n1(key int, value int);
LOAD DATA LOCAL INPATH '../../data/files/in4.txt' into table smb_input_n1;
LOAD DATA LOCAL INPATH '../../data/files/in5.txt' into table smb_input_n1;


;

-- smbs
CREATE TABLE smb_input1_n4(key int, value int) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS;
CREATE TABLE smb_input2_n4(key int, value int) CLUSTERED BY (value) SORTED BY (value) INTO 2 BUCKETS;

from smb_input_n1
insert overwrite table smb_input1_n4 select *
insert overwrite table smb_input2_n4 select *;

SET hive.optimize.bucketmapjoin = true;
SET hive.optimize.bucketmapjoin.sortedmerge = true;
SET hive.input.format = org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;

SELECT /*+ MAPJOIN(a) */ * FROM smb_input1_n4 a JOIN smb_input1_n4 b ON a.key <=> b.key;
SELECT /*+ MAPJOIN(a) */ * FROM smb_input1_n4 a JOIN smb_input1_n4 b ON a.key <=> b.key AND a.value <=> b.value;
SELECT /*+ MAPJOIN(a) */ * FROM smb_input1_n4 a RIGHT OUTER JOIN smb_input1_n4 b ON a.key <=> b.key;
SELECT /*+ MAPJOIN(b) */ * FROM smb_input1_n4 a JOIN smb_input1_n4 b ON a.key <=> b.key;
SELECT /*+ MAPJOIN(b) */ * FROM smb_input1_n4 a LEFT OUTER JOIN smb_input1_n4 b ON a.key <=> b.key;

SELECT /*+ MAPJOIN(a) */ * FROM smb_input1_n4 a JOIN smb_input2_n4 b ON a.key <=> b.value;
SELECT /*+ MAPJOIN(b) */ * FROM smb_input1_n4 a JOIN smb_input2_n4 b ON a.key <=> b.value;
SELECT /*+ MAPJOIN(b) */ * FROM smb_input1_n4 a LEFT OUTER JOIN smb_input2_n4 b ON a.key <=> b.value;
SELECT /*+ MAPJOIN(a) */ * FROM smb_input1_n4 a RIGHT OUTER JOIN smb_input2_n4 b ON a.key <=> b.value;

SELECT /*+ MAPJOIN(a) */ * FROM smb_input2_n4 a JOIN smb_input2_n4 b ON a.value <=> b.value;
SELECT /*+ MAPJOIN(a) */ * FROM smb_input2_n4 a RIGHT OUTER JOIN smb_input2_n4 b ON a.value <=> b.value;
SELECT /*+ MAPJOIN(b) */ * FROM smb_input2_n4 a JOIN smb_input2_n4 b ON a.value <=> b.value;
SELECT /*+ MAPJOIN(b) */ * FROM smb_input2_n4 a LEFT OUTER JOIN smb_input2_n4 b ON a.value <=> b.value;

--HIVE-3315 join predicate transitive
explain select * from myinput1_n9 a join myinput1_n9 b on a.key<=>b.value AND a.key is NULL;
select * from myinput1_n9 a join myinput1_n9 b on a.key<=>b.value AND a.key is NULL;
