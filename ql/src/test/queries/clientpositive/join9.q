--! qt:dataset:srcpart
--! qt:dataset:src1
--! qt:dataset:src
set hive.mapred.mode=nonstrict;
-- SORT_QUERY_RESULTS

CREATE TABLE dest1_n39(key INT, value STRING) STORED AS TEXTFILE;

EXPLAIN EXTENDED
FROM srcpart src1 JOIN src src2 ON (src1.key = src2.key)
INSERT OVERWRITE TABLE dest1_n39 SELECT src1.key, src2.value where src1.ds = '2008-04-08' and src1.hr = '12';

FROM srcpart src1 JOIN src src2 ON (src1.key = src2.key)
INSERT OVERWRITE TABLE dest1_n39 SELECT src1.key, src2.value where src1.ds = '2008-04-08' and src1.hr = '12';

SELECT dest1_n39.* FROM dest1_n39;
