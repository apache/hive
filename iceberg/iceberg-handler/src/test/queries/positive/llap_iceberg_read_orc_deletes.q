-- ORC files with position deletes are read through LLAP IO (see the LLAP IO counters): the deletes and ROW__POSITION
-- apply to the right rows in a later row group and across a batch boundary (the third row group starts at file row
-- 20000, its sixth batch at 25120)
set hive.llap.io.enabled=true;
set hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;
set hive.optimize.index.filter=true;

-- V2: position delete files
CREATE EXTERNAL TABLE ice_orc_v2 (id INT) STORED BY ICEBERG STORED AS ORC
TBLPROPERTIES ('format-version'='2');
INSERT INTO ice_orc_v2 SELECT pos FROM (SELECT posexplode(split(space(29999), ' ')) AS (pos, val)) t;
DELETE FROM ice_orc_v2 WHERE id IN (25119, 25121);

-- V3: deletion vectors
CREATE EXTERNAL TABLE ice_orc_v3 (id INT) STORED BY ICEBERG STORED AS ORC
TBLPROPERTIES ('format-version'='3');
INSERT INTO ice_orc_v3 SELECT pos FROM (SELECT posexplode(split(space(29999), ' ')) AS (pos, val)) t;
DELETE FROM ice_orc_v3 WHERE id IN (25119, 25121);

SET hive.exec.post.hooks=org.apache.hadoop.hive.ql.hooks.PostExecTezSummaryPrinter;
SELECT id, ROW__POSITION FROM ice_orc_v2 WHERE id BETWEEN 25117 AND 25123 ORDER BY id;
SELECT id, ROW__POSITION FROM ice_orc_v3 WHERE id BETWEEN 25117 AND 25123 ORDER BY id;

DROP TABLE ice_orc_v2;
DROP TABLE ice_orc_v3;
