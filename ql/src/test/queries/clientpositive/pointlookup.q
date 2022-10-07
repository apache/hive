--! qt:dataset:src

set hive.optimize.point.lookup.min=31;

explain
SELECT key
FROM src
WHERE
   ((key = '0'  AND value = 'val_0') OR
    (key = '1'  AND value = 'val_1') OR
    (key = '2'  AND value = 'val_2') OR
    (key = '3'  AND value = 'val_3') OR
    (key = '4'  AND value = 'val_4') OR
    (key = '5'  AND value = 'val_5') OR
    (key = '6'  AND value = 'val_6') OR
    (key = '7'  AND value = 'val_7') OR
    (key = '8'  AND value = 'val_8') OR
    (key = '9'  AND value = 'val_9') OR
    (key = '10' AND value = 'val_10'))
;

create table orOutput as
SELECT key
FROM src
WHERE
   ((key = '0'  AND value = 'val_0') OR
    (key = '1'  AND value = 'val_1') OR
    (key = '2'  AND value = 'val_2') OR
    (key = '3'  AND value = 'val_3') OR
    (key = '4'  AND value = 'val_4') OR
    (key = '5'  AND value = 'val_5') OR
    (key = '6'  AND value = 'val_6') OR
    (key = '7'  AND value = 'val_7') OR
    (key = '8'  AND value = 'val_8') OR
    (key = '9'  AND value = 'val_9') OR
    (key = '10' AND value = 'val_10'))
;

set hive.optimize.point.lookup.min=3;
set hive.optimize.partition.columns.separate=false;
explain
SELECT key
FROM src
WHERE
   ((key = '0'  AND value = 'val_0') OR
    (key = '1'  AND value = 'val_1') OR
    (key = '2'  AND value = 'val_2') OR
    (key = '3'  AND value = 'val_3') OR
    (key = '4'  AND value = 'val_4') OR
    (key = '5'  AND value = 'val_5') OR
    (key = '6'  AND value = 'val_6') OR
    (key = '7'  AND value = 'val_7') OR
    (key = '8'  AND value = 'val_8') OR
    (key = '9'  AND value = 'val_9') OR
    (key = '10' AND value = 'val_10'))
;

create table inOutput as
SELECT key
FROM src
WHERE
   ((key = '0'  AND value = 'val_0') OR
    (key = '1'  AND value = 'val_1') OR
    (key = '2'  AND value = 'val_2') OR
    (key = '3'  AND value = 'val_3') OR
    (key = '4'  AND value = 'val_4') OR
    (key = '5'  AND value = 'val_5') OR
    (key = '6'  AND value = 'val_6') OR
    (key = '7'  AND value = 'val_7') OR
    (key = '8'  AND value = 'val_8') OR
    (key = '9'  AND value = 'val_9') OR
    (key = '10' AND value = 'val_10'))
;

set hive.optimize.partition.columns.separate=true;
explain
SELECT key
FROM src
WHERE
   ((key = '0'  AND value = 'val_0') OR
    (key = '1'  AND value = 'val_1') OR
    (key = '2'  AND value = 'val_2') OR
    (key = '3'  AND value = 'val_3') OR
    (key = '4'  AND value = 'val_4') OR
    (key = '5'  AND value = 'val_5') OR
    (key = '6'  AND value = 'val_6') OR
    (key = '7'  AND value = 'val_7') OR
    (key = '8'  AND value = 'val_8') OR
    (key = '9'  AND value = 'val_9') OR
    (key = '10' AND value = 'val_10'))
;

create table inOutputOpt as
SELECT key
FROM src
WHERE
   ((key = '0'  AND value = 'val_0') OR
    (key = '1'  AND value = 'val_1') OR
    (key = '2'  AND value = 'val_2') OR
    (key = '3'  AND value = 'val_3') OR
    (key = '4'  AND value = 'val_4') OR
    (key = '5'  AND value = 'val_5') OR
    (key = '6'  AND value = 'val_6') OR
    (key = '7'  AND value = 'val_7') OR
    (key = '8'  AND value = 'val_8') OR
    (key = '9'  AND value = 'val_9') OR
    (key = '10' AND value = 'val_10'))
;

-- Output from all these tables should be the same
select count(*) from orOutput;
select count(*) from inOutput;
select count(*) from inOutputOpt;

-- check that orOutput and inOutput matches using full outer join
select orOutput.key, inOutput.key
from orOutput full outer join inOutput on (orOutput.key = inOutput.key)
where orOutput.key is null
or inOutput.key is null;

-- check that ourOutput and inOutputOpt matches using full outer join
select orOutput.key, inOutputOpt.key
from orOutput full outer join inOutputOpt on (orOutput.key = inOutputOpt.key)
where orOutput.key is null
or inOutputOpt.key is null;

drop table orOutput;
drop table inOutput;
drop table inOutputOpt;

-- test case(s) for HIVE-26320 for ORC
SET hive.optimize.point.lookup=true;
SET hive.optimize.point.lookup.min=2;
CREATE EXTERNAL TABLE hive26230_orc(kob varchar(2), enhanced_type_code int) STORED AS ORC;
INSERT INTO hive26230_orc VALUES('BB',18),('BC',18),('AB',18);
SELECT CASE WHEN ((kob='BB' AND enhanced_type_code=18) OR (kob='BC' AND enhanced_type_code=18)) THEN 1 ELSE 0 END AS logic_check
FROM hive26230_orc;
DROP TABLE hive26230_orc;

CREATE EXTERNAL TABLE hive26230_char_orc(kob char(2), enhanced_type_code int) STORED AS ORC;
INSERT INTO hive26230_char_orc VALUES('B',18),('BC',18),('AB',18);
SELECT CASE WHEN ((kob='B' AND enhanced_type_code=18) OR (kob='BC' AND enhanced_type_code=18)) THEN 1 ELSE 0 END AS logic_check
FROM hive26230_char_orc;
DROP TABLE hive26230_char_orc;

-- test case(s) for HIVE-26320 for PARQUET
CREATE EXTERNAL TABLE hive26230_parq(kob varchar(2), enhanced_type_code int) STORED AS PARQUET;
INSERT INTO hive26230_parq VALUES('BB',18),('BC',18),('AB',18);
SELECT CASE WHEN ((kob='BB' AND enhanced_type_code=18) OR (kob='BC' AND enhanced_type_code=18)) THEN 1 ELSE 0 END AS logic_check
FROM hive26230_parq;
DROP TABLE hive26230_parq;

CREATE EXTERNAL TABLE hive26230_char_parq(kob char(2), enhanced_type_code int) STORED AS PARQUET;
INSERT INTO hive26230_char_parq VALUES('B',18),('BC',18),('AB',18);
SELECT CASE WHEN ((kob='B' AND enhanced_type_code=18) OR (kob='BC' AND enhanced_type_code=18)) THEN 1 ELSE 0 END AS logic_check
FROM hive26230_char_parq;
DROP TABLE hive26230_char_parq;

CREATE EXTERNAL TABLE hive26230_int_parq(kob int, enhanced_type_code int) STORED AS PARQUET;
INSERT INTO hive26230_int_parq VALUES(2,18),(23,18),(12,18);
SELECT CASE WHEN ((kob=2 AND enhanced_type_code=18) OR (kob=23  AND enhanced_type_code=18)) THEN 1 ELSE 0 END AS logic_check FROM hive26230_int_parq;
DROP TABLE hive26230_int_parq;
