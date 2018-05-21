--! qt:dataset:src1
--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.auto.convert.join = true;

CREATE TABLE dest1_n140(key INT, value STRING) STORED AS TEXTFILE;

explain
FROM src src1 JOIN src src2 ON (src1.key = src2.key) JOIN src src3 ON (src1.key = src3.key)
INSERT OVERWRITE TABLE dest1_n140 SELECT src1.key, src3.value;

FROM src src1 JOIN src src2 ON (src1.key = src2.key) JOIN src src3 ON (src1.key = src3.key)
INSERT OVERWRITE TABLE dest1_n140 SELECT src1.key, src3.value;

SELECT sum(hash(dest1_n140.key,dest1_n140.value)) FROM dest1_n140;