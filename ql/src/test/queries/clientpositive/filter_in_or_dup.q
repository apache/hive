set hive.optimize.point.lookup.min=2;

EXPLAIN
SELECT f.key
FROM cbo_t1 f
WHERE (f.key = '1' OR f.key='2')
AND f.key IN ('1', '2');

EXPLAIN
SELECT f.key
FROM cbo_t1 f
WHERE (f.key = '1' OR f.key = '2')
AND f.key IN ('1', '2', '3');

EXPLAIN
SELECT f.key
FROM cbo_t1 f
WHERE (f.key = '1' OR f.key='2' OR f.key='3')
AND f.key IN ('1', '2');
