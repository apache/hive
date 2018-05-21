set hive.mapred.mode=nonstrict;
DROP TABLE IF EXISTS DECIMAL_3_txt;
DROP TABLE IF EXISTS DECIMAL_3_n0;

CREATE TABLE DECIMAL_3_txt(key decimal(38,18), value int)
ROW FORMAT DELIMITED
   FIELDS TERMINATED BY ' '
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/kv7.txt' INTO TABLE DECIMAL_3_txt;

CREATE TABLE DECIMAL_3_n0 STORED AS ORC AS SELECT * FROM DECIMAL_3_txt;

set hive.auto.convert.join=false;
EXPLAIN
SELECT * FROM DECIMAL_3_n0 a JOIN DECIMAL_3_n0 b ON (a.key = b.key) ORDER BY a.key, a.value, b.key, b.value;

SELECT * FROM DECIMAL_3_n0 a JOIN DECIMAL_3_n0 b ON (a.key = b.key) ORDER BY a.key, a.value, b.key, b.value;

set hive.auto.convert.join=true;
EXPLAIN
SELECT * FROM DECIMAL_3_n0 a JOIN DECIMAL_3_n0 b ON (a.key = b.key) ORDER BY a.key, a.value, b.key, b.value;

SELECT * FROM DECIMAL_3_n0 a JOIN DECIMAL_3_n0 b ON (a.key = b.key) ORDER BY a.key, a.value, b.key, b.value;

DROP TABLE DECIMAL_3_txt;
DROP TABLE DECIMAL_3_n0;
