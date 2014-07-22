-- sampling with join and alias
-- SORT_QUERY_RESULTS

EXPLAIN EXTENDED
SELECT s.*
FROM srcpart TABLESAMPLE (BUCKET 1 OUT OF 1 ON key) s
JOIN srcpart TABLESAMPLE (BUCKET 1 OUT OF 10 ON key) t
WHERE t.key = s.key and t.value = s.value and s.ds='2008-04-08' and s.hr='11';

SELECT s.key, s.value
FROM srcpart TABLESAMPLE (BUCKET 1 OUT OF 1 ON key) s
JOIN srcpart TABLESAMPLE (BUCKET 1 OUT OF 10 ON key) t
WHERE t.key = s.key and t.value = s.value and s.ds='2008-04-08' and s.hr='11';

EXPLAIN
SELECT * FROM src TABLESAMPLE(100 ROWS) a JOIN src1 TABLESAMPLE(10 ROWS) b ON a.key=b.key;

SELECT * FROM src TABLESAMPLE(100 ROWS) a JOIN src1 TABLESAMPLE(10 ROWS) b ON a.key=b.key;

EXPLAIN
SELECT * FROM src TABLESAMPLE(100 ROWS) a, src1 TABLESAMPLE(10 ROWS) b WHERE a.key=b.key;

SELECT * FROM src TABLESAMPLE(100 ROWS) a, src1 TABLESAMPLE(10 ROWS) b WHERE a.key=b.key;