--! qt:replace:/(\s+totalSize\s+)\S+(\s+)/$1#Masked#/
set hive.stats.kll.enable=true;
set metastore.stats.fetch.bitvector=true;
set metastore.stats.fetch.kll=true;
set hive.stats.autogather=true;
set hive.stats.column.autogather=true;

CREATE TABLE test_stats (a string, b int, c double, d float, e decimal(5,2), f timestamp, g date)
STORED AS ORC;

INSERT INTO test_stats (a, b, c, d, e, f, g) VALUES ("a", 2, 1.1, 12.2, 1.3, "2020-11-2 00:00:00", "2020-11-2");
INSERT INTO test_stats (a, b, c, d, e, f, g) VALUES ("b", 2, 2.1, NULL, 6.3, "2020-11-2 00:00:00", "2020-11-2");
INSERT INTO test_stats (a, b, c, d, e, f, g) VALUES ("c", 2, 2.1, NULL, -8.3, "2020-11-2 00:00:00", "2020-11-02");
INSERT INTO test_stats (a, b, c, d, e, f, g) VALUES ("d", 2, 3.1, 13.2, 10.2, "2020-11-2 00:00:00", "2020-11-2");
INSERT INTO test_stats (a, b, c, d, e, f, g) VALUES ("e", 2, 3.1, 14.2, 10.2, "2020-11-02 00:00:00", "2020-11-2");
INSERT INTO test_stats (a, b, c, d, e, f, g) VALUES ("f", 2, 4.1, NULL, 12.2, "2020-11-2 00:00:00", "2020-11-2");
INSERT INTO test_stats (a, b, c, d, e, f, g) VALUES ("g", 2, 5.1, 15.2, -10.2, "2020-11-2 00:00:00", "2020-11-2");
INSERT INTO test_stats (a, b, c, d, e, f, g) VALUES ("h", 2, 6.1, 16.2, 12.2, "2020-11-2 00:00:00", "2020-11-2");
INSERT INTO test_stats (a, b, c, d, e, f, g) VALUES ("i", 3, 6.1, 17.2, 7.2, "2020-11-03 00:00:00", "2020-11-3");
INSERT INTO test_stats (a, b, c, d, e, f, g) VALUES ("j", 4, NULL, 20.2, 1.2, "2020-11-4 00:00:00", "2020-11-4");
INSERT INTO test_stats (a, b, c, d, e, f, g) VALUES ("k", 5, NULL, 50.2, -123.2, "2020-11-5 00:00:00", "2020-11-05");
INSERT INTO test_stats (a, b, c, d, e, f, g) VALUES ("l", 6, NULL, 55.2, 1.2, "2020-11-6 00:00:00", "2020-11-6");
INSERT INTO test_stats (a, b, c, d, e, f, g) VALUES ("m", 7, 9.1, 57.2, 1001.2, "2020-11-7 00:00:00", "2020-11-7");
INSERT INTO test_stats (a, b, c, d, e, f, g) VALUES ("n", NULL, 10.1, 11.2, 0.2, NULL, NULL);
INSERT INTO test_stats (a, b, c, d, e, f, g) VALUES ("o", NULL, 56.1, 53.3, -12.3, NULL, NULL);
INSERT INTO test_stats (a, b, c, d, e, f, g) VALUES ("p", NULL, 23.3, 67.2, 1.2, NULL, NULL);
INSERT INTO test_stats (a, b, c, d, e, f, g) VALUES ("q", NULL, 31.1, 34.6, 2.4, NULL, NULL);
INSERT INTO test_stats (a, b, c, d, e, f, g) VALUES ("r", NULL, 11.5, 29.4, 3.4, NULL, NULL);
INSERT INTO test_stats (a, b, c, d, e, f, g) VALUES ("s", NULL, 101.2, 27.2, -6.1, NULL, NULL);
INSERT INTO test_stats (a, b, c, d, e, f, g) VALUES ("t", NULL, 33.9, 58.5, 4.3, NULL, NULL);

DESCRIBE FORMATTED test_stats;
DESCRIBE FORMATTED test_stats a;
DESCRIBE FORMATTED test_stats b;
DESCRIBE FORMATTED test_stats c;
DESCRIBE FORMATTED test_stats d;
DESCRIBE FORMATTED test_stats e;
DESCRIBE FORMATTED test_stats f;
DESCRIBE FORMATTED test_stats g;

EXPLAIN SELECT COUNT(*)
FROM test_stats t1 JOIN test_stats t2 ON (t1.a = t2.a)
WHERE t1.b BETWEEN 2 AND 6;
SELECT COUNT(*)
FROM test_stats t1 JOIN test_stats t2 ON (t1.a = t2.a)
WHERE t1.b BETWEEN 2 AND 6;

EXPLAIN SELECT COUNT(*)
FROM test_stats t1 JOIN test_stats t2 ON (t1.a = t2.a)
WHERE t1.b NOT BETWEEN 2 AND 6;
SELECT COUNT(*)
FROM test_stats t1 JOIN test_stats t2 ON (t1.a = t2.a)
WHERE t1.b NOT BETWEEN 2 AND 6;

EXPLAIN SELECT COUNT(*)
FROM test_stats t1 JOIN test_stats t2 ON (t1.a = t2.a)
WHERE t1.b >= 3;
SELECT COUNT(*)
FROM test_stats t1 JOIN test_stats t2 ON (t1.a = t2.a)
WHERE t1.b >= 3;

EXPLAIN SELECT COUNT(*)
FROM test_stats t1 JOIN test_stats t2 ON (t1.a = t2.a)
WHERE t1.b > 3;
SELECT COUNT(*)
FROM test_stats t1 JOIN test_stats t2 ON (t1.a = t2.a)
WHERE t1.b > 3;

EXPLAIN SELECT COUNT(*)
FROM test_stats t1 JOIN test_stats t2 ON (t1.a = t2.a)
WHERE t1.b <= 2;
SELECT COUNT(*)
FROM test_stats t1 JOIN test_stats t2 ON (t1.a = t2.a)
WHERE t1.b <= 2;

EXPLAIN SELECT COUNT(*)
FROM test_stats t1 JOIN test_stats t2 ON (t1.a = t2.a)
WHERE t1.b < 5;
SELECT COUNT(*)
FROM test_stats t1 JOIN test_stats t2 ON (t1.a = t2.a)
WHERE t1.b < 5;