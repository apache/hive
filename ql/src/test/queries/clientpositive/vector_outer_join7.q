SET hive.auto.convert.join=true;
SET hive.auto.convert.join.noconditionaltask=true;

-- SORT_QUERY_RESULTS

-- HIVE-29598: regression test for stale scratch-slot values in vectorized
-- outer-join MapJoin. MAX() acts as an aggregation barrier so Calcite cannot
-- inline the inner expression and simplify the bug surface away.

CREATE TABLE t (k STRING, v STRING) STORED AS ORC;

INSERT INTO t VALUES
  ('A','1'),('A','2'),('A','3'),
  ('B','2'),('B','3'),
  ('C','3'),
  ('D','1'),('D','3');

WITH
  probe AS (
    SELECT k, v, (CAST(v AS INT) > 0) AS p_bool
    FROM t WHERE CAST(v AS INT) >= 3
  ),
  small_side AS (
    SELECT k, v, (CAST(v AS INT) > 9999) AS s_bool
    FROM t
  ),
  classified AS (
    SELECT p.k, p.v, CAST((s.s_bool OR p.p_bool) AS INT) AS observed_value
    FROM probe p
    LEFT JOIN small_side s
      ON  p.k = s.k
      AND CAST(p.v AS INT) - 1 = CAST(s.v AS INT)
  ),
  diagnosed AS (
    SELECT k, v, MAX(observed_value) AS observed_value
    FROM classified
    GROUP BY k, v
  )
SELECT k, v,
       observed_value AS observed_value_returned_by_select,
       1              AS required_value_per_sql_semantics
FROM diagnosed
WHERE observed_value = 0;
