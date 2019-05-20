set hive.mapred.mode=nonstrict;
set hive.auto.convert.join = true;

CREATE TABLE myinput1_n0(key int, value int);
LOAD DATA LOCAL INPATH '../../data/files/in3.txt' INTO TABLE myinput1_n0;

SET hive.optimize.bucketmapjoin = true;
SET hive.optimize.bucketmapjoin.sortedmerge = true;
SET hive.input.format = org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;

EXPLAIN SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n0 a FULL OUTER JOIN myinput1_n0 b on a.key = 40 AND b.key = 40;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n0 a FULL OUTER JOIN myinput1_n0 b on a.key = 40 AND b.key = 40;

EXPLAIN SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n0 a FULL OUTER JOIN myinput1_n0 b on a.key = 40 AND a.value = 40 AND a.key = a.value AND b.key = 40;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n0 a FULL OUTER JOIN myinput1_n0 b on a.key = 40 AND a.key = a.value AND b.key = 40;

EXPLAIN SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n0 a FULL OUTER JOIN myinput1_n0 b on a.key = 40 AND a.key = b.key AND b.key = 40;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n0 a FULL OUTER JOIN myinput1_n0 b on a.key = 40 AND a.key = b.key AND b.key = 40;

EXPLAIN SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n0 a FULL OUTER JOIN myinput1_n0 b on a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n0 a FULL OUTER JOIN myinput1_n0 b on a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
