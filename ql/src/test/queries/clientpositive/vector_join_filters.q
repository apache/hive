SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.mapjoin.native.enabled=true;
set hive.fetch.task.conversion=none;
SET hive.auto.convert.join=true;
SET hive.auto.convert.join.noconditionaltask=true;
SET hive.auto.convert.join.noconditionaltask.size=1000000000;

-- SORT_QUERY_RESULTS

CREATE TABLE myinput1_txt(key int, value int);
LOAD DATA LOCAL INPATH '../../data/files/in3.txt' INTO TABLE myinput1_txt;
CREATE TABLE myinput1 STORED AS ORC AS SELECT * FROM myinput1_txt;

SELECT sum(hash(a.key,a.value,b.key,b.value))  FROM myinput1 a JOIN myinput1 b on a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value))  FROM myinput1 a LEFT OUTER JOIN myinput1 b on a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value))  FROM myinput1 a RIGHT OUTER JOIN myinput1 b on a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;

SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1 a JOIN myinput1 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1 a JOIN myinput1 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1 a JOIN myinput1 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1 a JOIN myinput1 b ON a.value = b.value and a.key=b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;

SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1 a LEFT OUTER JOIN myinput1 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1 a LEFT OUTER JOIN myinput1 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1 a LEFT OUTER JOIN myinput1 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1 a LEFT OUTER JOIN myinput1 b ON a.key = b.key and a.value=b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;

SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1 a RIGHT OUTER JOIN myinput1 b ON a.key = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1 a RIGHT OUTER JOIN myinput1 b ON a.key = b.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1 a RIGHT OUTER JOIN myinput1 b ON a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1 a RIGHT OUTER JOIN myinput1 b ON a.key=b.key and a.value = b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value;

SELECT sum(hash(a.key,a.value,b.key,b.value)) from myinput1 a LEFT OUTER JOIN myinput1 b ON (a.value=b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value) RIGHT OUTER JOIN myinput1 c ON (b.value=c.value AND c.key > 40 AND c.value > 50 AND c.key = c.value AND b.key > 40 AND b.value > 50 AND b.key = b.value);
SELECT sum(hash(a.key,a.value,b.key,b.value)) from myinput1 a RIGHT OUTER JOIN myinput1 b ON (a.value=b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value) LEFT OUTER JOIN myinput1 c ON (b.value=c.value AND c.key > 40 AND c.value > 50 AND c.key = c.value AND b.key > 40 AND b.value > 50 AND b.key = b.value);
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1 a LEFT OUTER JOIN myinput1 b RIGHT OUTER JOIN myinput1 c ON a.value = b.value and b.value = c.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value AND c.key > 40 AND c.value > 50 AND c.key = c.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) from myinput1 a LEFT OUTER JOIN myinput1 b ON (a.value=b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value) RIGHT OUTER JOIN myinput1 c ON (b.key=c.key AND c.key > 40 AND c.value > 50 AND c.key = c.value AND b.key > 40 AND b.value > 50 AND b.key = b.value);
SELECT sum(hash(a.key,a.value,b.key,b.value)) from myinput1 a RIGHT OUTER JOIN myinput1 b ON (a.value=b.value AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value) LEFT OUTER JOIN myinput1 c ON (b.key=c.key AND c.key > 40 AND c.value > 50 AND c.key = c.value AND b.key > 40 AND b.value > 50 AND b.key = b.value);
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1 a LEFT OUTER JOIN myinput1 b RIGHT OUTER JOIN myinput1 c ON a.value = b.value and b.key = c.key AND a.key > 40 AND a.value > 50 AND a.key = a.value AND b.key > 40 AND b.value > 50 AND b.key = b.value AND c.key > 40 AND c.value > 50 AND c.key = c.value;