set hive.strict.checks.cartesian.product=false;

CREATE TABLE table_7 (int_col INT);

explain
SELECT
(t1.int_col) * (t1.int_col) AS int_col
FROM (
SELECT
MIN(NULL) OVER () AS int_col
FROM table_7
) t1
WHERE
(False) NOT IN (SELECT
False AS boolean_col
FROM (
SELECT
MIN(NULL) OVER () AS int_col
FROM table_7
) tt1
WHERE
(t1.int_col) = (tt1.int_col));
