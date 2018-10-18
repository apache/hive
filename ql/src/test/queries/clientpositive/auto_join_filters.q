set hive.mapred.mode=nonstrict;
set hive.auto.convert.join = true;

CREATE TABLE myinput1_n5(key int, value int);
LOAD DATA LOCAL INPATH '../../data/files/in3.txt' INTO TABLE myinput1_n5;

SELECT sum(hash(a.key,a.value,b.key,b.value))  FROM myinput1_n5 a JOIN myinput1_n5 b on a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value))  FROM myinput1_n5 a LEFT OUTER JOIN myinput1_n5 b on a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value))  FROM myinput1_n5 a RIGHT OUTER JOIN myinput1_n5 b on a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SET hive.mapjoin.full.outer=false;
SELECT sum(hash(a.key,a.value,b.key,b.value))  FROM myinput1_n5 a FULL OUTER JOIN myinput1_n5 b on a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SET hive.mapjoin.full.outer=true;
SET hive.merge.nway.joins=false;
EXPLAIN SELECT sum(hash(a.key,a.value,b.key,b.value))  FROM myinput1_n5 a FULL OUTER JOIN myinput1_n5 b on a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value))  FROM myinput1_n5 a FULL OUTER JOIN myinput1_n5 b on a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SET hive.merge.nway.joins=true;

SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n5 a JOIN myinput1_n5 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n5 a JOIN myinput1_n5 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n5 a JOIN myinput1_n5 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n5 a JOIN myinput1_n5 b ON a.value = b.value and a.key=b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;

SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n5 a LEFT OUTER JOIN myinput1_n5 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n5 a LEFT OUTER JOIN myinput1_n5 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n5 a LEFT OUTER JOIN myinput1_n5 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n5 a LEFT OUTER JOIN myinput1_n5 b ON a.key = b.key and a.value=b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;

SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n5 a RIGHT OUTER JOIN myinput1_n5 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n5 a RIGHT OUTER JOIN myinput1_n5 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n5 a RIGHT OUTER JOIN myinput1_n5 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n5 a RIGHT OUTER JOIN myinput1_n5 b ON a.key=b.key and a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;

SET hive.mapjoin.full.outer=false;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n5 a FULL OUTER JOIN myinput1_n5 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n5 a FULL OUTER JOIN myinput1_n5 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n5 a FULL OUTER JOIN myinput1_n5 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n5 a FULL OUTER JOIN myinput1_n5 b ON a.value = b.value and a.key=b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SET hive.mapjoin.full.outer=true;
SET hive.merge.nway.joins=false;
EXPLAIN SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n5 a FULL OUTER JOIN myinput1_n5 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n5 a FULL OUTER JOIN myinput1_n5 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n5 a FULL OUTER JOIN myinput1_n5 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n5 a FULL OUTER JOIN myinput1_n5 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n5 a FULL OUTER JOIN myinput1_n5 b ON a.value = b.value and a.key=b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SET hive.merge.nway.joins=true;

SELECT sum(hash(a.key,a.value,b.key,b.value)) from myinput1_n5 a LEFT OUTER JOIN myinput1_n5 b ON (a.value=b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value) RIGHT OUTER JOIN myinput1_n5 c ON (b.value=c.value AND c.key > 40 AND c.value > 50 AND c.key = c.value AND b.key > 40 AND b.value > 50 AND b.key = b.value);
SELECT sum(hash(a.key,a.value,b.key,b.value)) from myinput1_n5 a RIGHT OUTER JOIN myinput1_n5 b ON (a.value=b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value) LEFT OUTER JOIN myinput1_n5 c ON (b.value=c.value AND c.key > 40 AND c.value > 50 AND c.key = c.value AND b.key > 40 AND b.value > 50 AND b.key = b.value);
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n5 a LEFT OUTER JOIN myinput1_n5 b RIGHT OUTER JOIN myinput1_n5 c ON a.value = b.value and b.value = c.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value AND c.key > 40 AND c.value > 50 AND c.key = c.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) from myinput1_n5 a LEFT OUTER JOIN myinput1_n5 b ON (a.value=b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value) RIGHT OUTER JOIN myinput1_n5 c ON (b.key=c.key AND c.key > 40 AND c.value > 50 AND c.key = c.value AND b.key > 40 AND b.value > 50 AND b.key = b.value);
SELECT sum(hash(a.key,a.value,b.key,b.value)) from myinput1_n5 a RIGHT OUTER JOIN myinput1_n5 b ON (a.value=b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value) LEFT OUTER JOIN myinput1_n5 c ON (b.key=c.key AND c.key > 40 AND c.value > 50 AND c.key = c.value AND b.key > 40 AND b.value > 50 AND b.key = b.value);
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n5 a LEFT OUTER JOIN myinput1_n5 b RIGHT OUTER JOIN myinput1_n5 c ON a.value = b.value and b.key = c.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value AND c.key > 40 AND c.value > 50 AND c.key = c.value;


CREATE TABLE smb_input1_n0(key int, value int) CLUSTERED BY (key) SORTED BY (key) INTO 2 BUCKETS; 
CREATE TABLE smb_input2_n0(key int, value int) CLUSTERED BY (value) SORTED BY (value) INTO 2 BUCKETS; 
LOAD DATA LOCAL INPATH '../../data/files/in/000000_0' into table smb_input1_n0;
LOAD DATA LOCAL INPATH '../../data/files/in/000001_0' into table smb_input1_n0;
LOAD DATA LOCAL INPATH '../../data/files/in/000000_0' into table smb_input2_n0;
LOAD DATA LOCAL INPATH '../../data/files/in/000001_0' into table smb_input2_n0;

SET hive.optimize.bucketmapjoin = true;
SET hive.optimize.bucketmapjoin.sortedmerge = true;
SET hive.input.format = org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;

SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n5 a JOIN myinput1_n5 b on a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n5 a LEFT OUTER JOIN myinput1_n5 b on a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n5 a RIGHT OUTER JOIN myinput1_n5 b on a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SET hive.mapjoin.full.outer=false;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n5 a FULL OUTER JOIN myinput1_n5 b on a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SET hive.mapjoin.full.outer=true;
SET hive.merge.nway.joins=false;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n5 a FULL OUTER JOIN myinput1_n5 b on a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SET hive.merge.nway.joins=true;

SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n5 a JOIN myinput1_n5 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n5 a JOIN myinput1_n5 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n5 a JOIN myinput1_n5 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n5 a JOIN myinput1_n5 b ON a.value = b.value and a.key=b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;

SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n5 a LEFT OUTER JOIN myinput1_n5 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n5 a LEFT OUTER JOIN myinput1_n5 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n5 a LEFT OUTER JOIN myinput1_n5 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n5 a LEFT OUTER JOIN myinput1_n5 b ON a.key = b.key and a.value=b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;

SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n5 a RIGHT OUTER JOIN myinput1_n5 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n5 a RIGHT OUTER JOIN myinput1_n5 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n5 a RIGHT OUTER JOIN myinput1_n5 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n5 a RIGHT OUTER JOIN myinput1_n5 b ON a.key=b.key and a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;

SET hive.mapjoin.full.outer=false;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n5 a FULL OUTER JOIN myinput1_n5 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n5 a FULL OUTER JOIN myinput1_n5 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n5 a FULL OUTER JOIN myinput1_n5 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n5 a FULL OUTER JOIN myinput1_n5 b ON a.value = b.value and a.key=b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SET hive.mapjoin.full.outer=true;
SET hive.merge.nway.joins=false;
EXPLAIN SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n5 a FULL OUTER JOIN myinput1_n5 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n5 a FULL OUTER JOIN myinput1_n5 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n5 a FULL OUTER JOIN myinput1_n5 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n5 a FULL OUTER JOIN myinput1_n5 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n5 a FULL OUTER JOIN myinput1_n5 b ON a.value = b.value and a.key=b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SET hive.merge.nway.joins=true;

SELECT sum(hash(a.key,a.value,b.key,b.value)) from myinput1_n5 a LEFT OUTER JOIN myinput1_n5 b ON (a.value=b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value) RIGHT OUTER JOIN myinput1_n5 c ON (b.value=c.value AND c.key > 40 AND c.value > 50 AND c.key = c.value AND b.key > 40 AND b.value > 50 AND b.key = b.value);
SELECT sum(hash(a.key,a.value,b.key,b.value)) from myinput1_n5 a RIGHT OUTER JOIN myinput1_n5 b ON (a.value=b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value) LEFT OUTER JOIN myinput1_n5 c ON (b.value=c.value AND c.key > 40 AND c.value > 50 AND c.key = c.value AND b.key > 40 AND b.value > 50 AND b.key = b.value);
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n5 a LEFT OUTER JOIN myinput1_n5 b RIGHT OUTER JOIN myinput1_n5 c ON a.value = b.value and b.value = c.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value AND c.key > 40 AND c.value > 50 AND c.key = c.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) from myinput1_n5 a LEFT OUTER JOIN myinput1_n5 b ON (a.value=b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value) RIGHT OUTER JOIN myinput1_n5 c ON (b.key=c.key AND c.key > 40 AND c.value > 50 AND c.key = c.value AND b.key > 40 AND b.value > 50 AND b.key = b.value);
SELECT sum(hash(a.key,a.value,b.key,b.value)) from myinput1_n5 a RIGHT OUTER JOIN myinput1_n5 b ON (a.value=b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value) LEFT OUTER JOIN myinput1_n5 c ON (b.key=c.key AND c.key > 40 AND c.value > 50 AND c.key = c.value AND b.key > 40 AND b.value > 50 AND b.key = b.value);
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n5 a LEFT OUTER JOIN myinput1_n5 b RIGHT OUTER JOIN myinput1_n5 c ON a.value = b.value and b.key = c.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value AND c.key > 40 AND c.value > 50 AND c.key = c.value;
