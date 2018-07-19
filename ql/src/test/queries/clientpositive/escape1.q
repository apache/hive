--! qt:dataset:part
set hive.compute.query.using.stats=false;
set hive.mapred.mode=nonstrict;
set hive.exec.dynamic.partition=true;
set hive.exec.max.dynamic.partitions.pernode=200;

DROP TABLE escape1;
DROP TABLE escape_raw_n0;

CREATE TABLE escape_raw_n0 (s STRING) STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/escapetest.txt' INTO TABLE  escape_raw_n0;

SELECT count(*) from escape_raw_n0;
SELECT * from escape_raw_n0;

CREATE TABLE escape1 (a STRING) PARTITIONED BY (ds STRING, part STRING);
INSERT OVERWRITE TABLE escape1 PARTITION (ds='1', part) SELECT '1', s from 
escape_raw_n0;

SELECT count(*) from escape1;
SELECT * from escape1;
SHOW PARTITIONS escape1;

ALTER TABLE escape1 DROP PARTITION (ds='1');
SHOW PARTITIONS escape1;

DROP TABLE escape1;
DROP TABLE escape_raw_n0;
