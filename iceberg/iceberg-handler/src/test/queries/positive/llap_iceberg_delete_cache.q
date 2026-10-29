-- Deletes read through LLAP IO are served from the LLAP metadata cache: once cached, a read still applies them after
-- the delete files are gone from storage.
set hive.llap.io.enabled=true;
set hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

dfs -mkdir -p /tmp/iceberg_llap_delete_cache;
dfs -rm -r -f /tmp/iceberg_llap_delete_cache;

-- V3: deletion vectors
CREATE EXTERNAL TABLE ice_dv (id INT) STORED BY ICEBERG STORED AS ORC
LOCATION '/tmp/iceberg_llap_delete_cache/ice_dv'
TBLPROPERTIES ('format-version'='3');
INSERT INTO ice_dv VALUES (1), (2), (3), (4), (5);
DELETE FROM ice_dv WHERE id IN (2, 4);

SELECT * FROM ice_dv ORDER BY id;
dfs -rm /tmp/iceberg_llap_delete_cache/ice_dv/data/*-pos-deletes*;
SELECT * FROM ice_dv ORDER BY id;

-- V2: position delete files, cached as deletion vectors
CREATE EXTERNAL TABLE ice_pos (id INT) STORED BY ICEBERG STORED AS PARQUET
LOCATION '/tmp/iceberg_llap_delete_cache/ice_pos'
TBLPROPERTIES ('format-version'='2');
INSERT INTO ice_pos VALUES (1), (2), (3);
INSERT INTO ice_pos VALUES (4), (5), (6);
DELETE FROM ice_pos WHERE id IN (2, 5);

SELECT * FROM ice_pos ORDER BY id;
dfs -rm /tmp/iceberg_llap_delete_cache/ice_pos/data/*-pos-deletes*;
SELECT * FROM ice_pos ORDER BY id;

-- a query that reads position deletes through LLAP IO in one vertex and without it in another (a lateral view does
-- not vectorize): both readers share the query's cache on the daemon
CREATE EXTERNAL TABLE ice_mix (id INT) STORED BY ICEBERG STORED AS PARQUET
LOCATION '/tmp/iceberg_llap_delete_cache/ice_mix'
TBLPROPERTIES ('format-version'='2');
INSERT INTO ice_mix VALUES (1), (2), (3);
INSERT INTO ice_mix VALUES (4), (5), (6);
DELETE FROM ice_mix WHERE id IN (2, 5);
set hive.auto.convert.join=false;
set hive.optimize.shared.work=false;
SELECT count(*) FROM ice_mix a JOIN (SELECT id FROM ice_mix LATERAL VIEW explode(array(1)) e AS x) b ON a.id = b.id;
set hive.optimize.shared.work=true;
set hive.auto.convert.join=true;

DROP TABLE ice_dv;
DROP TABLE ice_pos;
DROP TABLE ice_mix;
dfs -rm -r -f /tmp/iceberg_llap_delete_cache;
