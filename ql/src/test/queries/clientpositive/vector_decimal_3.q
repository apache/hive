set hive.mapred.mode=nonstrict;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

DROP TABLE IF EXISTS DECIMAL_3_txt_n0;
DROP TABLE IF EXISTS DECIMAL_3_n1;

CREATE TABLE DECIMAL_3_txt_n0(key decimal(38,18), value int)
ROW FORMAT DELIMITED
   FIELDS TERMINATED BY ' '
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/kv7.txt' INTO TABLE DECIMAL_3_txt_n0;

CREATE TABLE DECIMAL_3_n1 STORED AS ORC AS SELECT * FROM DECIMAL_3_txt_n0;

SELECT * FROM DECIMAL_3_n1 ORDER BY key, value;

SELECT * FROM DECIMAL_3_n1 ORDER BY key DESC, value DESC;

SELECT * FROM DECIMAL_3_n1 ORDER BY key, value;

SELECT DISTINCT key FROM DECIMAL_3_n1 ORDER BY key;

SELECT key, sum(value) FROM DECIMAL_3_n1 GROUP BY key ORDER BY key;

SELECT value, sum(key) FROM DECIMAL_3_n1 GROUP BY value ORDER BY value;

SELECT * FROM DECIMAL_3_n1 a JOIN DECIMAL_3_n1 b ON (a.key = b.key) ORDER BY a.key, a.value, b.value;

SELECT * FROM DECIMAL_3_n1 WHERE key=3.14 ORDER BY key, value;

SELECT * FROM DECIMAL_3_n1 WHERE key=3.140 ORDER BY key, value;

DROP TABLE DECIMAL_3_txt_n0;
DROP TABLE DECIMAL_3_n1;
