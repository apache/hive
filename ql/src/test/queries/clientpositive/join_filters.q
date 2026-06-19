set hive.mapred.mode=nonstrict;
-- SORT_AND_HASH_QUERY_RESULTS

CREATE TABLE myinput1_n8(key int, value int);
LOAD DATA LOCAL INPATH '../../data/files/in3.txt' INTO TABLE myinput1_n8;

SELECT * FROM myinput1_n8 a JOIN myinput1_n8 b on a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT * FROM myinput1_n8 a LEFT OUTER JOIN myinput1_n8 b on a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT * FROM myinput1_n8 a RIGHT OUTER JOIN myinput1_n8 b on a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT * FROM myinput1_n8 a FULL OUTER JOIN myinput1_n8 b on a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;

SELECT * FROM myinput1_n8 a JOIN myinput1_n8 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT * FROM myinput1_n8 a JOIN myinput1_n8 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT * FROM myinput1_n8 a JOIN myinput1_n8 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT * FROM myinput1_n8 a JOIN myinput1_n8 b ON a.value = b.value and a.key=b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;

SELECT * FROM myinput1_n8 a LEFT OUTER JOIN myinput1_n8 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT * FROM myinput1_n8 a LEFT OUTER JOIN myinput1_n8 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT * FROM myinput1_n8 a LEFT OUTER JOIN myinput1_n8 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT * FROM myinput1_n8 a LEFT OUTER JOIN myinput1_n8 b ON a.key = b.key and a.value=b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;

SELECT * FROM myinput1_n8 a RIGHT OUTER JOIN myinput1_n8 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT * FROM myinput1_n8 a RIGHT OUTER JOIN myinput1_n8 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT * FROM myinput1_n8 a RIGHT OUTER JOIN myinput1_n8 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT * FROM myinput1_n8 a RIGHT OUTER JOIN myinput1_n8 b ON a.key=b.key and a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;

SELECT * FROM myinput1_n8 a FULL OUTER JOIN myinput1_n8 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT * FROM myinput1_n8 a FULL OUTER JOIN myinput1_n8 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT * FROM myinput1_n8 a FULL OUTER JOIN myinput1_n8 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT * FROM myinput1_n8 a FULL OUTER JOIN myinput1_n8 b ON a.value = b.value and a.key=b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;

SELECT * from myinput1_n8 a LEFT OUTER JOIN myinput1_n8 b ON (a.value=b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value) RIGHT OUTER JOIN myinput1_n8 c ON (b.value=c.value AND c.key > 40 AND c.value > 50 AND c.key = c.value AND b.key > 40 AND b.value > 50 AND b.key = b.value);
SELECT * from myinput1_n8 a RIGHT OUTER JOIN myinput1_n8 b ON (a.value=b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value) LEFT OUTER JOIN myinput1_n8 c ON (b.value=c.value AND c.key > 40 AND c.value > 50 AND c.key = c.value AND b.key > 40 AND b.value > 50 AND b.key = b.value);
SELECT * FROM myinput1_n8 a LEFT OUTER JOIN myinput1_n8 b RIGHT OUTER JOIN myinput1_n8 c ON a.value = b.value and b.value = c.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value AND c.key > 40 AND c.value > 50 AND c.key = c.value;
SELECT * from myinput1_n8 a LEFT OUTER JOIN myinput1_n8 b ON (a.value=b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value) RIGHT OUTER JOIN myinput1_n8 c ON (b.key=c.key AND c.key > 40 AND c.value > 50 AND c.key = c.value AND b.key > 40 AND b.value > 50 AND b.key = b.value);
SELECT * from myinput1_n8 a RIGHT OUTER JOIN myinput1_n8 b ON (a.value=b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value) LEFT OUTER JOIN myinput1_n8 c ON (b.key=c.key AND c.key > 40 AND c.value > 50 AND c.key = c.value AND b.key > 40 AND b.value > 50 AND b.key = b.value);
SELECT * FROM myinput1_n8 a LEFT OUTER JOIN myinput1_n8 b RIGHT OUTER JOIN myinput1_n8 c ON a.value = b.value and b.key = c.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value AND c.key > 40 AND c.value > 50 AND c.key = c.value;

SELECT /*+ MAPJOIN(a) */ * FROM myinput1_n8 a JOIN myinput1_n8 b on a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT /*+ MAPJOIN(a) */ * FROM myinput1_n8 a JOIN myinput1_n8 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT /*+ MAPJOIN(a) */ * FROM myinput1_n8 a JOIN myinput1_n8 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT /*+ MAPJOIN(a) */ * FROM myinput1_n8 a JOIN myinput1_n8 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT /*+ MAPJOIN(a) */ * FROM myinput1_n8 a JOIN myinput1_n8 b ON a.value = b.value and a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;

SELECT /*+ MAPJOIN(b) */ * FROM myinput1_n8 a JOIN myinput1_n8 b on a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT /*+ MAPJOIN(b) */ * FROM myinput1_n8 a JOIN myinput1_n8 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT /*+ MAPJOIN(b) */ * FROM myinput1_n8 a JOIN myinput1_n8 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT /*+ MAPJOIN(b) */ * FROM myinput1_n8 a JOIN myinput1_n8 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT /*+ MAPJOIN(b) */ * FROM myinput1_n8 a JOIN myinput1_n8 b ON a.value = b.value and a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;

SELECT /*+ MAPJOIN(b) */ * FROM myinput1_n8 a LEFT OUTER JOIN myinput1_n8 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT /*+ MAPJOIN(b) */ * FROM myinput1_n8 a LEFT OUTER JOIN myinput1_n8 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT /*+ MAPJOIN(b) */ * FROM myinput1_n8 a LEFT OUTER JOIN myinput1_n8 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;

SELECT /*+ MAPJOIN(a) */ * FROM myinput1_n8 a RIGHT OUTER JOIN myinput1_n8 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT /*+ MAPJOIN(a) */ * FROM myinput1_n8 a RIGHT OUTER JOIN myinput1_n8 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT /*+ MAPJOIN(a) */ * FROM myinput1_n8 a RIGHT OUTER JOIN myinput1_n8 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;

CREATE TABLE smb_input1_n3(key int, value int) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS; 
CREATE TABLE smb_input2_n3(key int, value int) CLUSTERED BY (value) SORTED BY (value) INTO 2 BUCKETS;
LOAD DATA LOCAL INPATH '../../data/files/in/000000_0' into table smb_input1_n3;
LOAD DATA LOCAL INPATH '../../data/files/in/000001_0' into table smb_input1_n3;
LOAD DATA LOCAL INPATH '../../data/files/in/000000_0' into table smb_input2_n3;
LOAD DATA LOCAL INPATH '../../data/files/in/000001_0' into table smb_input2_n3;

SET hive.optimize.bucketmapjoin = true;
SET hive.optimize.bucketmapjoin.sortedmerge = true;
SET hive.input.format = org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;

SELECT /*+ MAPJOIN(a) */ * FROM smb_input1_n3 a JOIN smb_input1_n3 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT /*+ MAPJOIN(a) */ * FROM smb_input1_n3 a JOIN smb_input2_n3 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT /*+ MAPJOIN(a) */ * FROM smb_input2_n3 a JOIN smb_input2_n3 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT /*+ MAPJOIN(a) */ * FROM smb_input1_n3 a JOIN smb_input1_n3 b ON a.key = b.key AND a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;

SELECT /*+ MAPJOIN(b) */ * FROM smb_input1_n3 a JOIN smb_input1_n3 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT /*+ MAPJOIN(b) */ * FROM smb_input1_n3 a JOIN smb_input2_n3 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT /*+ MAPJOIN(b) */ * FROM smb_input2_n3 a JOIN smb_input2_n3 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT /*+ MAPJOIN(b) */ * FROM smb_input2_n3 a JOIN smb_input2_n3 b ON a.key = b.key AND a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;

SELECT /*+ MAPJOIN(b) */ * FROM smb_input1_n3 a LEFT OUTER JOIN smb_input1_n3 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT /*+ MAPJOIN(b) */ * FROM smb_input1_n3 a LEFT OUTER JOIN smb_input2_n3 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT /*+ MAPJOIN(b) */ * FROM smb_input2_n3 a LEFT OUTER JOIN smb_input2_n3 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;

SELECT /*+ MAPJOIN(a) */ * FROM smb_input1_n3 a RIGHT OUTER JOIN smb_input1_n3 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT /*+ MAPJOIN(a) */ * FROM smb_input1_n3 a RIGHT OUTER JOIN smb_input2_n3 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT /*+ MAPJOIN(a) */ * FROM smb_input2_n3 a RIGHT OUTER JOIN smb_input2_n3 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;

SELECT * FROM myinput1_n8 a JOIN myinput1_n8 b on a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT * FROM myinput1_n8 a LEFT OUTER JOIN myinput1_n8 b on a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT * FROM myinput1_n8 a RIGHT OUTER JOIN myinput1_n8 b on a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT * FROM myinput1_n8 a FULL OUTER JOIN myinput1_n8 b on a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;

SELECT * FROM myinput1_n8 a JOIN myinput1_n8 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT * FROM myinput1_n8 a JOIN myinput1_n8 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT * FROM myinput1_n8 a JOIN myinput1_n8 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT * FROM myinput1_n8 a JOIN myinput1_n8 b ON a.value = b.value and a.key=b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;

SELECT * FROM myinput1_n8 a LEFT OUTER JOIN myinput1_n8 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT * FROM myinput1_n8 a LEFT OUTER JOIN myinput1_n8 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT * FROM myinput1_n8 a LEFT OUTER JOIN myinput1_n8 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT * FROM myinput1_n8 a LEFT OUTER JOIN myinput1_n8 b ON a.key = b.key and a.value=b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;

SELECT * FROM myinput1_n8 a RIGHT OUTER JOIN myinput1_n8 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT * FROM myinput1_n8 a RIGHT OUTER JOIN myinput1_n8 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT * FROM myinput1_n8 a RIGHT OUTER JOIN myinput1_n8 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT * FROM myinput1_n8 a RIGHT OUTER JOIN myinput1_n8 b ON a.key=b.key and a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;

SELECT * FROM myinput1_n8 a FULL OUTER JOIN myinput1_n8 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT * FROM myinput1_n8 a FULL OUTER JOIN myinput1_n8 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT * FROM myinput1_n8 a FULL OUTER JOIN myinput1_n8 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT * FROM myinput1_n8 a FULL OUTER JOIN myinput1_n8 b ON a.value = b.value and a.key=b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;

SELECT * from myinput1_n8 a LEFT OUTER JOIN myinput1_n8 b ON (a.value=b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value) RIGHT OUTER JOIN myinput1_n8 c ON (b.value=c.value AND c.key > 40 AND c.value > 50 AND c.key = c.value AND b.key > 40 AND b.value > 50 AND b.key = b.value);
SELECT * from myinput1_n8 a RIGHT OUTER JOIN myinput1_n8 b ON (a.value=b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value) LEFT OUTER JOIN myinput1_n8 c ON (b.value=c.value AND c.key > 40 AND c.value > 50 AND c.key = c.value AND b.key > 40 AND b.value > 50 AND b.key = b.value);
SELECT * FROM myinput1_n8 a LEFT OUTER JOIN myinput1_n8 b RIGHT OUTER JOIN myinput1_n8 c ON a.value = b.value and b.value = c.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value AND c.key > 40 AND c.value > 50 AND c.key = c.value;
SELECT * from myinput1_n8 a LEFT OUTER JOIN myinput1_n8 b ON (a.value=b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value) RIGHT OUTER JOIN myinput1_n8 c ON (b.key=c.key AND c.key > 40 AND c.value > 50 AND c.key = c.value AND b.key > 40 AND b.value > 50 AND b.key = b.value);
SELECT * from myinput1_n8 a RIGHT OUTER JOIN myinput1_n8 b ON (a.value=b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value) LEFT OUTER JOIN myinput1_n8 c ON (b.key=c.key AND c.key > 40 AND c.value > 50 AND c.key = c.value AND b.key > 40 AND b.value > 50 AND b.key = b.value);
SELECT * FROM myinput1_n8 a LEFT OUTER JOIN myinput1_n8 b RIGHT OUTER JOIN myinput1_n8 c ON a.value = b.value and b.key = c.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value AND c.key > 40 AND c.value > 50 AND c.key = c.value;

SELECT /*+ MAPJOIN(a) */ * FROM myinput1_n8 a JOIN myinput1_n8 b on a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT /*+ MAPJOIN(a) */ * FROM myinput1_n8 a JOIN myinput1_n8 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT /*+ MAPJOIN(a) */ * FROM myinput1_n8 a JOIN myinput1_n8 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT /*+ MAPJOIN(a) */ * FROM myinput1_n8 a JOIN myinput1_n8 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT /*+ MAPJOIN(a) */ * FROM myinput1_n8 a JOIN myinput1_n8 b ON a.value = b.value and a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;

SELECT /*+ MAPJOIN(b) */ * FROM myinput1_n8 a JOIN myinput1_n8 b on a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT /*+ MAPJOIN(b) */ * FROM myinput1_n8 a JOIN myinput1_n8 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT /*+ MAPJOIN(b) */ * FROM myinput1_n8 a JOIN myinput1_n8 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT /*+ MAPJOIN(b) */ * FROM myinput1_n8 a JOIN myinput1_n8 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT /*+ MAPJOIN(b) */ * FROM myinput1_n8 a JOIN myinput1_n8 b ON a.value = b.value and a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;

SELECT /*+ MAPJOIN(b) */ * FROM myinput1_n8 a LEFT OUTER JOIN myinput1_n8 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT /*+ MAPJOIN(b) */ * FROM myinput1_n8 a LEFT OUTER JOIN myinput1_n8 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT /*+ MAPJOIN(b) */ * FROM myinput1_n8 a LEFT OUTER JOIN myinput1_n8 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;

SELECT /*+ MAPJOIN(a) */ * FROM myinput1_n8 a RIGHT OUTER JOIN myinput1_n8 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT /*+ MAPJOIN(a) */ * FROM myinput1_n8 a RIGHT OUTER JOIN myinput1_n8 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT /*+ MAPJOIN(a) */ * FROM myinput1_n8 a RIGHT OUTER JOIN myinput1_n8 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;

SELECT /*+ MAPJOIN(a) */ * FROM smb_input1_n3 a JOIN smb_input1_n3 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT /*+ MAPJOIN(a) */ * FROM smb_input1_n3 a JOIN smb_input2_n3 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT /*+ MAPJOIN(a) */ * FROM smb_input2_n3 a JOIN smb_input2_n3 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT /*+ MAPJOIN(a) */ * FROM smb_input1_n3 a JOIN smb_input1_n3 b ON a.key = b.key AND a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;

SELECT /*+ MAPJOIN(b) */ * FROM smb_input1_n3 a JOIN smb_input1_n3 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT /*+ MAPJOIN(b) */ * FROM smb_input1_n3 a JOIN smb_input2_n3 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT /*+ MAPJOIN(b) */ * FROM smb_input2_n3 a JOIN smb_input2_n3 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT /*+ MAPJOIN(b) */ * FROM smb_input2_n3 a JOIN smb_input2_n3 b ON a.key = b.key AND a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;

SELECT /*+ MAPJOIN(b) */ * FROM smb_input1_n3 a LEFT OUTER JOIN smb_input1_n3 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT /*+ MAPJOIN(b) */ * FROM smb_input1_n3 a LEFT OUTER JOIN smb_input2_n3 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT /*+ MAPJOIN(b) */ * FROM smb_input2_n3 a LEFT OUTER JOIN smb_input2_n3 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;

SELECT /*+ MAPJOIN(a) */ * FROM smb_input1_n3 a RIGHT OUTER JOIN smb_input1_n3 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT /*+ MAPJOIN(a) */ * FROM smb_input1_n3 a RIGHT OUTER JOIN smb_input2_n3 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT /*+ MAPJOIN(a) */ * FROM smb_input2_n3 a RIGHT OUTER JOIN smb_input2_n3 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
