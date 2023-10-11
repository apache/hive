set hive.cbo.fallback.strategy=NEVER;
set hive.stats.column.autogather=false;

CREATE TABLE tbl1 (key int, f1 int);
CREATE TABLE tbl2 (f1 int) PARTITIONED BY (key int);

INSERT INTO tbl1 values (5, 8);
INSERT INTO tbl1 values (6, 9);
INSERT INTO tbl1 values (7, 10);
INSERT INTO tbl1 values (-1, 11);

EXPLAIN FROM (SELECT key, f1 FROM tbl1 WHERE key=5) a
INSERT OVERWRITE TABLE tbl2 PARTITION(key=5)
SELECT f1 WHERE key > 0 GROUP BY f1
INSERT OVERWRITE TABLE tbl2 partition(key=6)
SELECT f1 WHERE key > 0 GROUP BY f1;

FROM (SELECT key, f1 FROM tbl1 WHERE key=5) a
INSERT OVERWRITE TABLE tbl2 PARTITION (key=5)
SELECT f1 WHERE key > 0 GROUP BY f1
INSERT OVERWRITE TABLE tbl2 partition(key=6)
SELECT f1 WHERE key > 0 GROUP BY f1;

SELECT * from tbl2;
