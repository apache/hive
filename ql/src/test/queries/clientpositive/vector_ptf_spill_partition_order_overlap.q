SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;
SET hive.vectorized.execution.ptf.enabled=true;
SET hive.vectorized.testing.reducer.batch.size=2;
SET hive.vectorized.ptf.max.memory.buffering.batch.count=1;

SET hive.fetch.task.conversion=none;

CREATE TABLE t1 (
  dept STRING,
  region BIGINT,
  emp_id BIGINT,
  salary DOUBLE
) STORED AS ORC;

INSERT INTO t1 values
  ('engineering', 10, 1, 50000.0),
  ('engineering', 10, 2, 55000.0),
  ('engineering', 10, 3, 60000.0),
  ('engineering', 10, 4, 45000.0),
  ('engineering', 10, 5, 70000.0);

EXPLAIN VECTORIZATION DETAIL
SELECT dept, region, emp_id,
  SUM(salary) OVER (
    PARTITION BY dept, region
    ORDER BY dept
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ) AS total
FROM t1;

SELECT dept, region, emp_id,
  SUM(salary) OVER (
    PARTITION BY dept, region
    ORDER BY dept
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ) AS total
FROM t1
order by emp_id;
