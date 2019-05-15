set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

DROP TABLE IF EXISTS decimal_txt;
DROP TABLE IF EXISTS `decimal`;

CREATE TABLE decimal_txt (`dec` decimal);

LOAD DATA LOCAL INPATH '../../data/files/decimal_10_0.txt' OVERWRITE INTO TABLE decimal_txt;

CREATE TABLE `DECIMAL` STORED AS ORC AS SELECT * FROM decimal_txt;

EXPLAIN VECTORIZATION DETAIL
SELECT `dec` FROM `DECIMAL` order by `dec`;

SELECT `dec` FROM `DECIMAL` order by `dec`;

-- DECIMAL_64

EXPLAIN VECTORIZATION DETAIL
SELECT `dec` FROM `decimal_txt` order by `dec`;

SELECT `dec` FROM `decimal_txt` order by `dec`;

DROP TABLE DECIMAL_txt;
DROP TABLE `DECIMAL`;
