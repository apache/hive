set hive.auto.convert.join=true;
set hive.optimize.tez=true;
EXPLAIN EXTENDED SELECT key, count(value) as cnt FROM src GROUP BY key ORDER BY cnt;
EXPLAIN EXTENDED SELECT s2.key, count(distinct s2.value) as cnt FROM src s1 join src s2 on (s1.key = s2.key) GROUP BY s2.key ORDER BY cnt;

SELECT key, count(value) as cnt FROM src GROUP BY key ORDER BY cnt;