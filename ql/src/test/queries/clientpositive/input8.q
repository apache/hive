--! qt:dataset:src1
set hive.mapred.mode=nonstrict;
CREATE TABLE dest1_n28(c1 STRING, c2 INT, c3 DOUBLE) STORED AS TEXTFILE;

EXPLAIN
FROM src1 
INSERT OVERWRITE TABLE dest1_n28 SELECT 4 + NULL, src1.key - NULL, NULL + NULL;

FROM src1 
INSERT OVERWRITE TABLE dest1_n28 SELECT 4 + NULL, src1.key - NULL, NULL + NULL;

SELECT dest1_n28.* FROM dest1_n28;
