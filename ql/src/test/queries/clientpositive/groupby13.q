CREATE TABLE grpby_test (int_col_5 INT,
  int_col_7 INT);

SET hive.mapred.mode=strict;

EXPLAIN
SELECT
int_col_7,
MAX(LEAST(COALESCE(int_col_5, -279),
  COALESCE(int_col_7, 476))) AS int_col
FROM grpby_test
GROUP BY
int_col_7,
int_col_7,
LEAST(COALESCE(int_col_5, -279),
  COALESCE(int_col_7, 476));
