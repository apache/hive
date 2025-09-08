-- SORT_QUERY_RESULTS

-- Verifies PPD doesn't eliminate OR filter comparing base table vs lateral view columns
-- deliberately uses a non-existent key value to demonstrate that the filter is not eliminated as before the fix
SELECT t.key, lv.col
FROM (SELECT '1' AS key) t
LATERAL VIEW explode(array('2', '3')) lv AS col
WHERE t.key = '4' OR lv.col = '3';

-- Verifies PPD doesn't eliminate inequality filter between base table and lateral view columns  
SELECT t.key, lv.col
FROM (SELECT '1' AS key) t
LATERAL VIEW explode(array('1', '2')) lv AS col
WHERE t.key != lv.col;

-- Verifies PPD doesn't eliminate OR filter between different LATERAL VIEW columns
SELECT t.*, lv1.col1, lv2.col2
FROM (SELECT 1) t
LATERAL VIEW explode(array('a', 'b')) lv1 AS col1
LATERAL VIEW explode(array('b', 'c')) lv2 AS col2
WHERE lv1.col1 = 'a' OR lv2.col2 = 'c';

-- Verifies PPD doesn't eliminate complex filter with three LATERAL VIEW columns
SELECT t.*, lv1.col1, lv2.col2, lv3.col3
FROM (SELECT 1) t
LATERAL VIEW explode(array('x', 'y')) lv1 AS col1
LATERAL VIEW explode(array('x', 'y')) lv2 AS col2
LATERAL VIEW explode(array('x', 'y')) lv3 AS col3
WHERE lv1.col1 != lv2.col2 AND lv2.col2 != lv3.col3;
