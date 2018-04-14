--! qt:dataset:srcpart
--! qt:dataset:src1
--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.PostExecutePrinter,org.apache.hadoop.hive.ql.hooks.PrintCompletedTasksHook;

set hive.auto.convert.join = true;
set hive.mapjoin.followby.gby.localtask.max.memory.usage = 0.0001; -- HIVE-18445
set hive.mapjoin.localtask.max.memory.usage = 0.0001;
set hive.mapjoin.check.memory.rows = 2;
set hive.auto.convert.join.noconditionaltask = false;

-- This test tests the scenario when the mapper dies. So, create a conditional task for the mapjoin
CREATE TABLE dest1_n62(key INT, value STRING) STORED AS TEXTFILE;

FROM srcpart src1 JOIN src src2 ON (src1.key = src2.key)
INSERT OVERWRITE TABLE dest1_n62 SELECT src1.key, src2.value
where (src1.ds = '2008-04-08' or src1.ds = '2008-04-09' )and (src1.hr = '12' or src1.hr = '11');

SELECT sum(hash(dest1_n62.key,dest1_n62.value)) FROM dest1_n62;



CREATE TABLE dest_j2_n0(key INT, value STRING) STORED AS TEXTFILE;
FROM src src1 JOIN src src2 ON (src1.key = src2.key) JOIN src src3 ON (src1.key + src2.key = src3.key)
INSERT OVERWRITE TABLE dest_j2_n0 SELECT src1.key, src3.value;

SELECT sum(hash(dest_j2_n0.key,dest_j2_n0.value)) FROM dest_j2_n0;

CREATE TABLE dest_j1_n5(key INT, value STRING) STORED AS TEXTFILE;

FROM src src1 JOIN src src2 ON (src1.key = src2.key)
INSERT OVERWRITE TABLE dest_j1_n5 SELECT src1.key, src2.value;

SELECT sum(hash(dest_j1_n5.key,dest_j1_n5.value)) FROM dest_j1_n5;
