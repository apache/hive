set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.mapjoin.native.enabled=true;
SET hive.auto.convert.join=true;
SET hive.vectorized.reuse.scratch.columns=true;
set hive.fetch.task.conversion=none;

-- SORT_QUERY_RESULTS

-- Regression test for HIVE-29598: vectorized outer join wrong results due to
-- stale scratch column values when hive.vectorized.reuse.scratch.columns=true.
-- A CAST expression and the outer join null-marking column share the same scratch
-- slot; downstream ColOrCol reads vector[i] without checking isNull, causing
-- rows that should be NULL to be misclassified.

CREATE TABLE src_hive29598 (k STRING, v STRING) STORED AS ORC;

INSERT INTO src_hive29598 VALUES
  ('p','1'),('p','2'),('p','3'),
  ('q','2'),('q','3'),
  ('r','3'),
  ('s','3');

WITH base AS (
  SELECT k, v FROM src_hive29598 GROUP BY k, v
),
classified AS (
  SELECT t1.k, t1.v,
         CASE WHEN t2.k IS NULL THEN 'new'
              WHEN t3.k IS NULL THEN 'two_step'
              ELSE 'three_step' END AS status
  FROM base t1
  LEFT JOIN base t2
    ON  t1.k = t2.k
    AND CAST(t1.v AS INT) - 1 = CAST(t2.v AS INT)
  LEFT JOIN base t3
    ON  t1.k = t3.k
    AND CAST(t1.v AS INT) - 2 = CAST(t3.v AS INT)
  WHERE CAST(t1.v AS INT) >= 3
  GROUP BY t1.k, t1.v,
           CASE WHEN t2.k IS NULL THEN 'new'
                WHEN t3.k IS NULL THEN 'two_step'
                ELSE 'three_step' END
)
SELECT * FROM classified WHERE status = 'new';

DROP TABLE src_hive29598;
