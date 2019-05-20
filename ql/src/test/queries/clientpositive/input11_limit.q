--! qt:dataset:src
set hive.stats.column.autogather=false;
set hive.mapred.mode=nonstrict;
-- SORT_QUERY_RESULTS

CREATE TABLE dest1_n153(key INT, value STRING) STORED AS TEXTFILE;

EXPLAIN
FROM src
INSERT OVERWRITE TABLE dest1_n153 SELECT src.key, src.value WHERE src.key < 100 LIMIT 10;

FROM src
INSERT OVERWRITE TABLE dest1_n153 SELECT src.key, src.value WHERE src.key < 100 LIMIT 10;

SELECT dest1_n153.* FROM dest1_n153;
