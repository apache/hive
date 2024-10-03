CREATE TABLE t_asj_18 (k STRING, v INT);
INSERT INTO t_asj_18 values ('a', 10), ('a', 10);

set hive.auto.convert.join=false;
set hive.tez.auto.reducer.parallelism=true;

EXPLAIN SELECT * FROM (
    SELECT k, COUNT(DISTINCT v), SUM(v)
    FROM t_asj_18 GROUP BY k
) a LEFT JOIN (
    SELECT k, COUNT(v)
    FROM t_asj_18 GROUP BY k
) b ON a.k = b.k;

SELECT * FROM (
    SELECT k, COUNT(DISTINCT v), SUM(v)
    FROM t_asj_18 GROUP BY k
) a LEFT JOIN (
    SELECT k, COUNT(v)
    FROM t_asj_18 GROUP BY k
) b ON a.k = b.k;

set hive.optimize.distinct.rewrite=false;

EXPLAIN SELECT * FROM (
    SELECT k, COUNT(DISTINCT v)
    FROM t_asj_18 GROUP BY k
) a LEFT JOIN (
    SELECT k, COUNT(v)
    FROM t_asj_18 GROUP BY k
) b ON a.k = b.k;

SELECT * FROM (
    SELECT k, COUNT(DISTINCT v)
    FROM t_asj_18 GROUP BY k
) a LEFT JOIN (
    SELECT k, COUNT(v)
    FROM t_asj_18 GROUP BY k
) b ON a.k = b.k;