--! qt:dataset:src1
--! qt:dataset:src
set hive.mapred.mode=nonstrict;
-- SORT_QUERY_RESULTS

CREATE TABLE dest1_n121(key1 INT, value1 STRING, key2 INT, value2 STRING) STORED AS TEXTFILE;

EXPLAIN EXTENDED
FROM src src1 JOIN src src2 ON (src1.key = src2.key)
INSERT OVERWRITE TABLE dest1_n121 SELECT src1.*, src2.*;

FROM src src1 JOIN src src2 ON (src1.key = src2.key)
INSERT OVERWRITE TABLE dest1_n121 SELECT src1.*, src2.*;

SELECT dest1_n121.* FROM dest1_n121;
