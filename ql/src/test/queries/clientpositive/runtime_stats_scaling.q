-- Test runtime statistics scaling: column stats adjustment when row count changes.
-- When runtime row count is smaller than compile-time estimate, count-based stats
-- (numNulls, numTrues, numFalses) must be adjusted to prevent invalid values.

set hive.fetch.task.conversion=none;

create table t_runtime_scaling (
  id int,
  str_col string,
  bool_col boolean
);

-- 10 rows: skewed id values create selectivity mismatch
-- str_col: 9 rows NULL, 1 non-null (tests numNulls for string)
-- bool_col: 2 true, 1 false, 7 NULL (tests numNulls for boolean)
insert into t_runtime_scaling values
  (1, NULL, NULL), (2, NULL, NULL), (3, NULL, NULL), (4, NULL, NULL),
  (5, NULL, NULL), (6, NULL, true), (7, NULL, true), (8, NULL, NULL),
  (9, NULL, NULL), (100, 'only_non_null', false);

analyze table t_runtime_scaling compute statistics;
analyze table t_runtime_scaling compute statistics for columns;

-- Compile-time: estimates ~50% selectivity (5 rows). Runtime: 1 row passes.

-- Test 1: numNulls scaling for string (str_col has 9 nulls, scaled to 1 row)
explain
select str_col from t_runtime_scaling where id > 50;

explain reoptimization
select str_col from t_runtime_scaling where id > 50;

-- Test 2: numNulls scaling for boolean (bool_col has 7 nulls, scaled to 1 row)
explain
select bool_col from t_runtime_scaling where id > 50;

explain reoptimization
select bool_col from t_runtime_scaling where id > 50;

-- Test 3: combined (both columns)
explain
select str_col, bool_col from t_runtime_scaling where id > 50;

explain reoptimization
select str_col, bool_col from t_runtime_scaling where id > 50;
