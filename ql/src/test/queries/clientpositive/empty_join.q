set hive.auto.convert.join=true;
set hive.mapjoin.hybridgrace.hashtable=false;
set hive.explain.user=true;

DROP TABLE IF EXISTS test_1;
CREATE TABLE test_1 AS SELECT 1 AS id;

DROP TABLE IF EXISTS test_2;
CREATE TABLE test_2 (id INT);

DROP TABLE IF EXISTS test_3;
CREATE TABLE test_3 AS SELECT 1 AS id;

explain
SELECT t1.id, t2.id, t3.id
FROM test_1 t1
LEFT JOIN test_2 t2 ON t1.id = t2.id
INNER JOIN test_3 t3 ON t1.id = t3.id;

SELECT t1.id, t2.id, t3.id
FROM test_1 t1
LEFT JOIN test_2 t2 ON t1.id = t2.id
INNER JOIN test_3 t3 ON t1.id = t3.id
;
