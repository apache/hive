set hive.mapred.mode=nonstrict;
DROP TABLE IF EXISTS dec;

CREATE TABLE dec(name string, value decimal(8,4));

LOAD DATA LOCAL INPATH '../../data/files/dec.txt' INTO TABLE dec;

DROP TABLE IF EXISTS parq_dec;

CREATE TABLE parq_dec(name string, value decimal(5,2)) STORED AS PARQUET;

DESC parq_dec;

INSERT OVERWRITE TABLE parq_dec SELECT name, value FROM dec;

SELECT * FROM parq_dec;

SELECT value, count(*) FROM parq_dec GROUP BY value ORDER BY value;

TRUNCATE TABLE parq_dec;

INSERT OVERWRITE TABLE parq_dec SELECT name, NULL FROM dec;

SELECT * FROM parq_dec;

DROP TABLE IF EXISTS parq_dec1;

CREATE TABLE parq_dec1(name string, value decimal(4,1)) STORED AS PARQUET;

DESC parq_dec1;

LOAD DATA LOCAL INPATH '../../data/files/dec.parq' INTO TABLE parq_dec1;

SELECT VALUE FROM parq_dec1;

DROP TABLE dec;
DROP TABLE parq_dec;
DROP TABLE parq_dec1;
