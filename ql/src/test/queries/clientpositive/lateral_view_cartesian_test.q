-- Test case for LATERAL VIEW cartesian product query
-- This tests the query from HIVE-29084 for proper selectivity estimation

SELECT first_val, second_val
FROM (SELECT array('a', 'b') as val_array) inline_data
LATERAL VIEW explode(val_array) lv1 AS first_val
LATERAL VIEW explode(val_array) lv2 AS second_val
WHERE first_val != second_val
ORDER BY first_val, second_val;