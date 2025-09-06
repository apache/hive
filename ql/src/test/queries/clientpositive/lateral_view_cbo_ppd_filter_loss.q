-- SORT_QUERY_RESULTS
-- HIVE-29084: LATERAL VIEW cartesian product schema construction

-- Verifies PPD doesn't eliminate OR filter comparing base table vs lateral view columns
SELECT t.key, t.value, lv.col
FROM (SELECT '238' AS key, 'val_238' AS value) t
LATERAL VIEW explode(array('238', '86', '311')) lv AS col
WHERE t.key = '333' OR lv.col = '86'
ORDER BY t.key, lv.col;

-- Verifies PPD doesn't eliminate inequality filter between base table and lateral view columns  
SELECT t.key, t.value, lv.col
FROM (SELECT '238' AS key, 'val_238' AS value) t
LATERAL VIEW explode(array('238', '86', '311')) lv AS col
WHERE t.key != lv.col
ORDER BY t.key, lv.col;

-- Verifies PPD doesn't eliminate OR filter between different LATERAL VIEW columns
SELECT t.key, lv1.col1, lv2.col2
FROM (SELECT '238' AS key, 'val_238' AS value) t
LATERAL VIEW explode(array('a', 'b')) lv1 AS col1
LATERAL VIEW explode(array('b', 'c')) lv2 AS col2
WHERE lv1.col1 = 'a' OR lv2.col2 = 'c'
ORDER BY t.key, lv1.col1, lv2.col2;

-- Verifies PPD doesn't eliminate complex filter with three LATERAL VIEW columns
SELECT t.key, lv1.col1, lv2.col2, lv3.col3
FROM (SELECT '238' AS key, 'val_238' AS value) t
LATERAL VIEW explode(array('x', 'y')) lv1 AS col1
LATERAL VIEW explode(array('x', 'y')) lv2 AS col2
LATERAL VIEW explode(array('x', 'y')) lv3 AS col3
WHERE lv1.col1 != lv2.col2 AND lv2.col2 != lv3.col3
ORDER BY t.key, lv1.col1, lv2.col2, lv3.col3;
