--! qt:dataset:src1
--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.cbo.returnpath.hiveop=true;
set hive.auto.convert.join = true;

CREATE TABLE dest1_n112(key1 INT, value1 STRING, key2 INT, value2 STRING) STORED AS TEXTFILE;

explain
FROM src src1 JOIN src src2 ON (src1.key = src2.key)
INSERT OVERWRITE TABLE dest1_n112 SELECT src1.*, src2.*;


FROM src src1 JOIN src src2 ON (src1.key = src2.key)
INSERT OVERWRITE TABLE dest1_n112 SELECT src1.*, src2.*;

SELECT sum(hash(dest1_n112.key1,dest1_n112.value1,dest1_n112.key2,dest1_n112.value2)) FROM dest1_n112;