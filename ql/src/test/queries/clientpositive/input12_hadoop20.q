--! qt:dataset:src
set mapred.job.tracker=localhost:58;
set hive.exec.mode.local.auto=true;

-- INCLUDE_HADOOP_MAJOR_VERSIONS( 0.20S)

CREATE TABLE dest1_n88(key INT, value STRING) STORED AS TEXTFILE;
CREATE TABLE dest2_n23(key INT, value STRING) STORED AS TEXTFILE;
CREATE TABLE dest3_n2(key INT) PARTITIONED BY(ds STRING, hr STRING) STORED AS TEXTFILE;

EXPLAIN
FROM src 
INSERT OVERWRITE TABLE dest1_n88 SELECT src.* WHERE src.key < 100
INSERT OVERWRITE TABLE dest2_n23 SELECT src.key, src.value WHERE src.key >= 100 and src.key < 200
INSERT OVERWRITE TABLE dest3_n2 PARTITION(ds='2008-04-08', hr='12') SELECT src.key WHERE src.key >= 200;

FROM src 
INSERT OVERWRITE TABLE dest1_n88 SELECT src.* WHERE src.key < 100
INSERT OVERWRITE TABLE dest2_n23 SELECT src.key, src.value WHERE src.key >= 100 and src.key < 200
INSERT OVERWRITE TABLE dest3_n2 PARTITION(ds='2008-04-08', hr='12') SELECT src.key WHERE src.key >= 200;

SELECT dest1_n88.* FROM dest1_n88;
SELECT dest2_n23.* FROM dest2_n23;
SELECT dest3_n2.* FROM dest3_n2;
