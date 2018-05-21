--! qt:dataset:src
set hive.mapred.mode=nonstrict;
CREATE TABLE dest1_n4(dummy STRING, key INT, value DOUBLE) STORED AS TEXTFILE;

EXPLAIN
FROM src
INSERT OVERWRITE TABLE dest1_n4 SELECT '1234', key, count(1) WHERE src.key < 100 group by key;

FROM src
INSERT OVERWRITE TABLE dest1_n4 SELECT '1234', key, count(1) WHERE src.key < 100 group by key;

SELECT dest1_n4.* FROM dest1_n4;
