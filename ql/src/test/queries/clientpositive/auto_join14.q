--! qt:dataset:srcpart
--! qt:dataset:src
set hive.mapred.mode=nonstrict;

set hive.auto.convert.join = true;


CREATE TABLE dest1_n83(c1 INT, c2 STRING) STORED AS TEXTFILE;

set mapreduce.framework.name=yarn;
set mapreduce.jobtracker.address=localhost:58;
set hive.exec.mode.local.auto=true;

explain
FROM src JOIN srcpart ON src.key = srcpart.key AND srcpart.ds = '2008-04-08' and src.key > 100
INSERT OVERWRITE TABLE dest1_n83 SELECT src.key, srcpart.value;

FROM src JOIN srcpart ON src.key = srcpart.key AND srcpart.ds = '2008-04-08' and src.key > 100
INSERT OVERWRITE TABLE dest1_n83 SELECT src.key, srcpart.value;

SELECT sum(hash(dest1_n83.c1,dest1_n83.c2)) FROM dest1_n83;
