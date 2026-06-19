--! qt:dataset:src
set hive.mapred.mode=nonstrict;
CREATE TABLE dest1_n152(key INT, value STRING) STORED AS TEXTFILE;

EXPLAIN
FROM src
INSERT OVERWRITE TABLE dest1_n152 SELECT src.key, src.value WHERE src.key < 100;

FROM src
INSERT OVERWRITE TABLE dest1_n152 SELECT src.key, src.value WHERE src.key < 100;

SELECT dest1_n152.* FROM dest1_n152;
