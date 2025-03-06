SET hive.cbo.returnpath.hiveop=true;
SET hive.default.nulls.last=false;

CREATE TABLE t1(key int, value string);

EXPLAIN CBO SELECT * FROM t1 a INNER JOIN t1 b on a.key = b.key;
EXPLAIN SELECT * FROM t1 a INNER JOIN t1 b on a.key = b.key;

SET hive.default.nulls.last=true;

EXPLAIN CBO SELECT * FROM t1 a INNER JOIN t1 b on a.key = b.key;
EXPLAIN SELECT * FROM t1 a INNER JOIN t1 b on a.key = b.key;
