--! qt:dataset:src

DROP TABLE IF EXISTS kv_table;
CREATE EXTERNAL TABLE kv_table(key int, value string)
STORED BY 'org.apache.hadoop.hive.kudu.KuduStorageHandler'
TBLPROPERTIES ("kudu.table_name" = "default.kudu_kv");

INSERT INTO TABLE kv_table
SELECT key, value FROM src;

ANALYZE TABLE kv_table COMPUTE STATISTICS;

ANALYZE TABLE kv_table COMPUTE STATISTICS FOR COLUMNS key, value;

EXPLAIN
SELECT y.*
FROM
(SELECT kv_table.* FROM kv_table) x
JOIN
(SELECT src.* FROM src) y
ON (x.key = y.key)
ORDER BY key, value LIMIT 10;

SELECT y.*
FROM
(SELECT kv_table.* FROM kv_table) x
JOIN
(SELECT src.* FROM src) y
ON (x.key = y.key)
ORDER BY key, value LIMIT 10;

EXPLAIN
SELECT max(k.key) as max, min(k.value) as min
FROM kv_table k JOIN src s ON (k.key = s.key)
WHERE k.key > 100 and k.key % 2 = 0
GROUP BY k.key
ORDER BY min
LIMIT 10;

SELECT max(k.key) as max, min(k.value) as min
FROM kv_table k JOIN src s ON (k.key = s.key)
WHERE k.key > 100 and k.key % 2 = 0
GROUP BY k.key
ORDER BY min
LIMIT 10;