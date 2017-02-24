set hive.compute.query.using.stats=false;
set hive.mapred.mode=nonstrict;
set hive.exec.dynamic.partition=true;
set hive.exec.max.dynamic.partitions.pernode=200;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set hive.default.fileformat=RCFILE;

DROP TABLE IF EXISTS escape2;
DROP TABLE IF EXISTS escape_raw;

CREATE TABLE escape_raw (s STRING) STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '../../data/files/escapetest.txt' INTO TABLE  escape_raw;

SELECT count(*) from escape_raw;
SELECT * from escape_raw;

CREATE TABLE escape2(a STRING) PARTITIONED BY (ds STRING, part STRING);
INSERT OVERWRITE TABLE escape2 PARTITION (ds='1', part) SELECT '1', s from 
escape_raw;

SELECT count(*) from escape2;
SELECT * from escape2;
SHOW PARTITIONS escape2;

-- ASCII values 1-31, 59, 92, 127 were not included in the below commands

ALTER table escape2 PARTITION (ds='1', part=' ') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='!') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='\"') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='#') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='$') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='%') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='&') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part="\'") CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='(') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part=')') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='*') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='+') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part=',') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='-') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='.') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='/') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='0') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='1') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='2') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='3') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='4') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='5') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='6') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='7') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='8') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='9') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part=':') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='<') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='=') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='>') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='?') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='@') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='A') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='B') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='C') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='D') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='E') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='F') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='G') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='H') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='I') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='J') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='K') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='L') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='M') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='N') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='O') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='P') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='Q') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='R') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='S') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='T') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='U') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='V') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='W') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='X') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='Y') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='Z') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='[') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part=']') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='_') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='`') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='{') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='|') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='}') CONCATENATE;
ALTER TABLE escape2 PARTITION (ds='1', part='~') CONCATENATE;

DROP TABLE escape2;
DROP TABLE escape_raw;
