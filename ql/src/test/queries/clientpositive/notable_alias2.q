--! qt:dataset:src
-- SORT_QUERY_RESULTS

set hive.mapred.mode=nonstrict;
CREATE TABLE dest1_n50(dummy STRING, key INT, value DOUBLE) STORED AS TEXTFILE;

EXPLAIN
FROM src
INSERT OVERWRITE TABLE dest1_n50 SELECT '1234', src.key, count(1) WHERE key < 100 group by src.key;

FROM src
INSERT OVERWRITE TABLE dest1_n50 SELECT '1234', src.key, count(1) WHERE key < 100 group by src.key;

SELECT dest1_n50.* FROM dest1_n50;
