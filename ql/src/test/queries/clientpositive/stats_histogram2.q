set hive.stats.kll.enable=true;
set metastore.stats.fetch.bitvector=true;
set metastore.stats.fetch.kll=true;
set hive.stats.autogather=true;
set hive.stats.column.autogather=true;

CREATE TABLE test_stats (a string, b timestamp, c date)
STORED AS ORC;

INSERT INTO test_stats (a, b, c) VALUES ("a", "2020-11-2 00:00:00", "2020-11-2");
INSERT INTO test_stats (a, b, c) VALUES ("b", "2020-11-2 00:00:00", "2020-11-2");
INSERT INTO test_stats (a, b, c) VALUES ("c", "2020-11-2 00:00:00", "2020-11-2");
INSERT INTO test_stats (a, b, c) VALUES ("d", "2020-11-2 00:00:00", "2020-11-2");
INSERT INTO test_stats (a, b, c) VALUES ("e", "2020-11-2 00:00:00", "2020-11-2");
INSERT INTO test_stats (a, b, c) VALUES ("f", "2020-11-2 00:00:00", "2020-11-2");
INSERT INTO test_stats (a, b, c) VALUES ("g", "2020-11-2 02:10:10", "2020-11-2");
INSERT INTO test_stats (a, b, c) VALUES ("h", "2020-11-2 02:30:00", "2020-11-2");
INSERT INTO test_stats (a, b, c) VALUES ("i", "2020-11-03 00:00:00", "2020-11-3");
INSERT INTO test_stats (a, b, c) VALUES ("j", "2020-11-4 00:00:00", "2020-11-4");
INSERT INTO test_stats (a, b, c) VALUES ("k", "2020-11-5 00:00:00", "2020-11-5");
INSERT INTO test_stats (a, b, c) VALUES ("l", "2020-11-6 00:00:00", "2020-11-6");
INSERT INTO test_stats (a, b, c) VALUES ("m", "2020-11-7 00:00:00", "2020-11-7");
INSERT INTO test_stats (a, b, c) VALUES ("n", NULL, NULL);
INSERT INTO test_stats (a, b, c) VALUES ("o", NULL, NULL);

-- Plan optimized by CBO.
EXPLAIN CBO SELECT * FROM test_stats t1 JOIN test_stats t2 ON (t1.a = t2.a) WHERE t1.b < "2020-11-3" AND t1.c < "2020-11-3";

EXPLAIN CBO SELECT COUNT(*) FROM test_stats WHERE b < "2020-11-3" AND c < "2020-11-3";
EXPLAIN SELECT COUNT(*) FROM test_stats WHERE b < "2020-11-3" AND c < "2020-11-3";
EXPLAIN EXTENDED SELECT COUNT(*) FROM test_stats WHERE b < "2020-11-3" AND c < "2020-11-3";

SELECT COUNT(*) FROM test_stats WHERE b < "2020-11-3" AND c < "2020-11-3";

EXPLAIN CBO SELECT COUNT(*) FROM test_stats WHERE b >= "2020-11-7" AND c >= "2020-11-7";
EXPLAIN SELECT COUNT(*) FROM test_stats WHERE b >= "2020-11-7" AND c >= "2020-11-7";
EXPLAIN EXTENDED SELECT COUNT(*) FROM test_stats WHERE b >= "2020-11-7" AND c >= "2020-11-7";

SELECT COUNT(*) FROM test_stats WHERE b >= "2020-11-7" AND c >= "2020-11-7";

DESCRIBE FORMATTED test_stats;
DESCRIBE FORMATTED test_stats b;
DESCRIBE FORMATTED test_stats c;

DROP TABLE test_stats;
