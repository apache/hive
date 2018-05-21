set hive.mapred.mode=nonstrict;
set hive.auto.convert.join = true;

CREATE TABLE myinput1_n2(key int, value int);
LOAD DATA LOCAL INPATH '../../data/files/in1.txt' INTO TABLE myinput1_n2;

SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n2 a JOIN myinput1_n2 b;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n2 a LEFT OUTER JOIN myinput1_n2 b;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n2 a RIGHT OUTER JOIN myinput1_n2 b;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n2 a JOIN myinput1_n2 b ON a.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n2 a JOIN myinput1_n2 b ON a.key = b.key;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n2 a JOIN myinput1_n2 b ON a.value = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n2 a JOIN myinput1_n2 b ON a.value = b.value and a.key=b.key;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n2 a LEFT OUTER JOIN myinput1_n2 b ON a.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n2 a LEFT OUTER JOIN myinput1_n2 b ON a.value = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n2 a LEFT OUTER JOIN myinput1_n2 b ON a.key = b.key;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n2 a LEFT OUTER JOIN myinput1_n2 b ON a.key = b.key and a.value=b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n2 a RIGHT OUTER JOIN myinput1_n2 b ON a.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n2 a RIGHT OUTER JOIN myinput1_n2 b ON a.key = b.key;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n2 a RIGHT OUTER JOIN myinput1_n2 b ON a.value = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n2 a RIGHT OUTER JOIN myinput1_n2 b ON a.key=b.key and a.value = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n2 a FULL OUTER JOIN myinput1_n2 b ON a.key = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n2 a FULL OUTER JOIN myinput1_n2 b ON a.key = b.key;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n2 a FULL OUTER JOIN myinput1_n2 b ON a.value = b.value;
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n2 a FULL OUTER JOIN myinput1_n2 b ON a.value = b.value and a.key=b.key;

SELECT sum(hash(a.key,a.value,b.key,b.value)) from myinput1_n2 a LEFT OUTER JOIN myinput1_n2 b ON (a.value=b.value) RIGHT OUTER JOIN myinput1_n2 c ON (b.value=c.value);
SELECT sum(hash(a.key,a.value,b.key,b.value)) from myinput1_n2 a RIGHT OUTER JOIN myinput1_n2 b ON (a.value=b.value) LEFT OUTER JOIN myinput1_n2 c ON (b.value=c.value);
SELECT sum(hash(a.key,a.value,b.key,b.value)) FROM myinput1_n2 a LEFT OUTER JOIN myinput1_n2 b RIGHT OUTER JOIN myinput1_n2 c ON a.value = b.value and b.value = c.value;

