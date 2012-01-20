use default;
-- Test sort_array() UDF

DESCRIBE FUNCTION sort_array;
DESCRIBE FUNCTION EXTENDED sort_array;

-- Evaluate function against STRING valued keys
EXPLAIN
SELECT sort_array(array("b", "d", "c", "a")) FROM src LIMIT 1;

SELECT sort_array(array("f", "a", "g", "c", "b", "d", "e")) FROM src LIMIT 1;
SELECT sort_array(sort_array(array("hadoop distributed file system", "enterprise databases", "hadoop map-reduce"))) FROM src LIMIT 1;

-- Evaluate function against INT valued keys
SELECT sort_array(array(2, 9, 7, 3, 5, 4, 1, 6, 8)) FROM src LIMIT 1;

-- Evaluate function against FLOAT valued keys
SELECT sort_array(sort_array(array(2.333, 9, 1.325, 2.003, 0.777, -3.445, 1))) FROM src LIMIT 1;

