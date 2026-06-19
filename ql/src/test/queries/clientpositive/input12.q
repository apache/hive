--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set mapreduce.framework.name=yarn;
set mapreduce.jobtracker.address=localhost:58;
set hive.exec.mode.local.auto=true;


CREATE TABLE dest1_n122(key INT, value STRING) STORED AS TEXTFILE;
CREATE TABLE dest2_n32(key INT, value STRING) STORED AS TEXTFILE;
CREATE TABLE dest3_n5(key INT) PARTITIONED BY(ds STRING, hr STRING) STORED AS TEXTFILE;

EXPLAIN
FROM src 
INSERT OVERWRITE TABLE dest1_n122 SELECT src.* WHERE src.key < 100
INSERT OVERWRITE TABLE dest2_n32 SELECT src.key, src.value WHERE src.key >= 100 and src.key < 200
INSERT OVERWRITE TABLE dest3_n5 PARTITION(ds='2008-04-08', hr='12') SELECT src.key WHERE src.key >= 200;

FROM src 
INSERT OVERWRITE TABLE dest1_n122 SELECT src.* WHERE src.key < 100
INSERT OVERWRITE TABLE dest2_n32 SELECT src.key, src.value WHERE src.key >= 100 and src.key < 200
INSERT OVERWRITE TABLE dest3_n5 PARTITION(ds='2008-04-08', hr='12') SELECT src.key WHERE src.key >= 200;

SELECT dest1_n122.* FROM dest1_n122;
SELECT dest2_n32.* FROM dest2_n32;
SELECT dest3_n5.* FROM dest3_n5;
