--! qt:dataset:srcpart
--! qt:dataset:src1
--! qt:dataset:src
-- SORT_QUERY_RESULTS

CREATE TABLE dest_j1_n4(key STRING, value STRING, val2 STRING) STORED AS TEXTFILE;
set hive.cbo.enable=false;

EXPLAIN EXTENDED
INSERT OVERWRITE TABLE dest_j1_n4
SELECT /*+ MAPJOIN(x,y) */ x.key, z.value, y.value
FROM src1 x JOIN src y ON (x.key = y.key) 
JOIN srcpart z ON (x.key = z.key)
WHERE z.ds='2008-04-08' and z.hr=11;

INSERT OVERWRITE TABLE dest_j1_n4
SELECT /*+ MAPJOIN(x,y) */ x.key, z.value, y.value
FROM src1 x JOIN src y ON (x.key = y.key) 
JOIN srcpart z ON (x.key = z.key)
WHERE z.ds='2008-04-08' and z.hr=11;

select * from dest_j1_n4;

CREATE TABLE src_copy(key int, value string);
CREATE TABLE src1_copy(key string, value string);
INSERT OVERWRITE TABLE src_copy select key, value from src;
INSERT OVERWRITE TABLE src1_copy select key, value from src1;

EXPLAIN EXTENDED
INSERT OVERWRITE TABLE dest_j1_n4
SELECT /*+ MAPJOIN(x,y) */ x.key, z.value, y.value
FROM src1_copy x JOIN src_copy y ON (x.key = y.key) 
JOIN srcpart z ON (x.key = z.key)
WHERE z.ds='2008-04-08' and z.hr=11;

INSERT OVERWRITE TABLE dest_j1_n4
SELECT /*+ MAPJOIN(x,y) */ x.key, z.value, y.value
FROM src1_copy x JOIN src_copy y ON (x.key = y.key) 
JOIN srcpart z ON (x.key = z.key)
WHERE z.ds='2008-04-08' and z.hr=11;

select * from dest_j1_n4;





