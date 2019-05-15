set hive.auto.convert.join=true;
set hive.mapjoin.hybridgrace.hashtable=false;
set hive.explain.user=true;

DROP TABLE IF EXISTS test_1_n2;
CREATE TABLE test_1_n2 AS SELECT 1 AS id;

DROP TABLE IF EXISTS test_2_n2;
CREATE TABLE test_2_n2 (id INT);

DROP TABLE IF EXISTS test_3_n0;
CREATE TABLE test_3_n0 AS SELECT 1 AS id;

explain
SELECT t1.id, t2.id, t3.id
FROM test_1_n2 t1
LEFT JOIN test_2_n2 t2 ON t1.id = t2.id
INNER JOIN test_3_n0 t3 ON t1.id = t3.id;

SELECT t1.id, t2.id, t3.id
FROM test_1_n2 t1
LEFT JOIN test_2_n2 t2 ON t1.id = t2.id
INNER JOIN test_3_n0 t3 ON t1.id = t3.id
;
