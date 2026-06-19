--! qt:dataset:src1
--! qt:dataset:src
set hive.mapred.mode=nonstrict;

set hive.auto.convert.join = true;

CREATE TABLE dest1_n41(key1 INT, value1 STRING, key2 INT, value2 STRING) STORED AS TEXTFILE;

explain
FROM src src1 JOIN src src2 ON (src1.key = src2.key)
INSERT OVERWRITE TABLE dest1_n41 SELECT src1.*, src2.*;


FROM src src1 JOIN src src2 ON (src1.key = src2.key)
INSERT OVERWRITE TABLE dest1_n41 SELECT src1.*, src2.*;

SELECT sum(hash(dest1_n41.key1,dest1_n41.value1,dest1_n41.key2,dest1_n41.value2)) FROM dest1_n41;