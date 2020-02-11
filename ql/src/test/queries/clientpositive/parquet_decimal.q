set hive.vectorized.execution.enabled=false;
set hive.mapred.mode=nonstrict;

DROP TABLE IF EXISTS `dec_n1`;

CREATE TABLE `dec_n1`(name string, value decimal(8,4));

LOAD DATA LOCAL INPATH '../../data/files/dec.txt' INTO TABLE `dec_n1`;

DROP TABLE IF EXISTS parq_dec_n1;

CREATE TABLE parq_dec_n1(name string, value decimal(5,2)) STORED AS PARQUET;

DESC parq_dec_n1;

INSERT OVERWRITE TABLE parq_dec_n1 SELECT name, value FROM `dec_n1`;

SELECT * FROM parq_dec_n1;

SELECT value, count(*) FROM parq_dec_n1 GROUP BY value ORDER BY value;

TRUNCATE TABLE parq_dec_n1;

INSERT OVERWRITE TABLE parq_dec_n1 SELECT name, NULL FROM `dec_n1`;

SELECT * FROM parq_dec_n1;

DROP TABLE IF EXISTS parq_dec1;

CREATE TABLE parq_dec1(name string, value decimal(4,1)) STORED AS PARQUET;

DESC parq_dec1;

LOAD DATA LOCAL INPATH '../../data/files/dec.parq' INTO TABLE parq_dec1;

SELECT VALUE FROM parq_dec1;

DROP TABLE `dec_n1`;
DROP TABLE parq_dec_n1;
DROP TABLE parq_dec1;
