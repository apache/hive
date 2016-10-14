set hive.mapred.mode=nonstrict;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

DROP TABLE IF EXISTS DECIMAL_5_txt;
DROP TABLE IF EXISTS DECIMAL_5;

CREATE TABLE DECIMAL_5_txt(key decimal(10,5), value int)
ROW FORMAT DELIMITED
   FIELDS TERMINATED BY ' '
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/kv7.txt' INTO TABLE DECIMAL_5_txt;

CREATE TABLE DECIMAL_5(key decimal(10,5), value int)
STORED AS ORC;

INSERT OVERWRITE TABLE DECIMAL_5 SELECT * FROM DECIMAL_5_txt;

SELECT key FROM DECIMAL_5 ORDER BY key;

SELECT DISTINCT key FROM DECIMAL_5 ORDER BY key;

SELECT cast(key as decimal) FROM DECIMAL_5;

SELECT cast(key as decimal(6,3)) FROM DECIMAL_5;

DROP TABLE DECIMAL_5_txt;
DROP TABLE DECIMAL_5;