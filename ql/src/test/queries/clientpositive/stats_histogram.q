set hive.stats.kll.enable=true;
set metastore.stats.fetch.bitvector=true;
set hive.stats.autogather=true;
set hive.stats.column.autogather=true;

CREATE TABLE test_stats (a string, b int)
STORED AS ORC;

INSERT INTO test_stats (a, b) VALUES ("a", 2);
INSERT INTO test_stats (a, b) VALUES ("b", 2);
INSERT INTO test_stats (a, b) VALUES ("c", 2);
INSERT INTO test_stats (a, b) VALUES ("d", 2);
INSERT INTO test_stats (a, b) VALUES ("e", 2);
INSERT INTO test_stats (a, b) VALUES ("f", 2);
INSERT INTO test_stats (a, b) VALUES ("g", 2);
INSERT INTO test_stats (a, b) VALUES ("h", 2);
INSERT INTO test_stats (a, b) VALUES ("i", 3);
INSERT INTO test_stats (a, b) VALUES ("j", 4);
INSERT INTO test_stats (a, b) VALUES ("k", 5);
INSERT INTO test_stats (a, b) VALUES ("l", 6);
INSERT INTO test_stats (a, b) VALUES ("m", 7);
INSERT INTO test_stats (a, b) VALUES ("n", NULL);
INSERT INTO test_stats (a, b) VALUES ("o", NULL);

-- Plan optimized by CBO.
EXPLAIN CBO COST SELECT * FROM test_stats t1 JOIN test_stats t2 ON (t1.a = t2.a) WHERE t1.b < 3;

EXPLAIN CBO COST SELECT COUNT(*) FROM test_stats WHERE b < 3;
EXPLAIN SELECT COUNT(*) FROM test_stats WHERE b < 3;
EXPLAIN EXTENDED SELECT COUNT(*) FROM test_stats WHERE b < 3;

SELECT COUNT(*) FROM test_stats WHERE b < 3;

EXPLAIN CBO COST SELECT COUNT(*) FROM test_stats WHERE b >= 7;
EXPLAIN SELECT COUNT(*) FROM test_stats WHERE b >= 7;
EXPLAIN EXTENDED SELECT COUNT(*) FROM test_stats WHERE b >= 7;

SELECT COUNT(*) FROM test_stats WHERE b >= 7;

DESCRIBE FORMATTED test_stats;
DESCRIBE FORMATTED test_stats b;

DROP TABLE test_stats;
