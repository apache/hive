set hive.mapred.mode=nonstrict;
set hive.auto.convert.join=true;
set hive.spark.use.file.size.for.mapjoin=true;
set hive.auto.convert.join.noconditionaltask.size=4000;

EXPLAIN
SELECT src1.key, src2.value
FROM src src1 JOIN src src2 ON (src1.key = src2.key)
WHERE src1.key = 97;

SELECT src1.key, src2.value
FROM src src1 JOIN src src2 ON (src1.key = src2.key)
WHERE src1.key = 97;

set hive.auto.convert.join.noconditionaltask.size=8000;

-- This is copied from auto_join2. Without the configuration both joins are mapjoins,
-- but with the configuration on, Hive should not turn the second join into mapjoin since it
-- has a upstream reduce sink.

CREATE TABLE dest(key INT, value STRING) STORED AS TEXTFILE;

EXPLAIN
FROM src src1 JOIN src src2 ON (src1.key = src2.key) JOIN src src3 ON (src1.key + src2.key = src3.key)
INSERT OVERWRITE TABLE dest SELECT src1.key, src3.value;

FROM src src1 JOIN src src2 ON (src1.key = src2.key) JOIN src src3 ON (src1.key + src2.key = src3.key)
INSERT OVERWRITE TABLE dest SELECT src1.key, src3.value;

SELECT sum(hash(dest.key,dest.value)) FROM dest;
