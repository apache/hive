set hive.explain.user=false;
SET hive.auto.convert.join=true;
SET hive.auto.convert.join.noconditionaltask=true;
set hive.fetch.task.conversion=none;

-- SORT_QUERY_RESULTS

-- Regression test for HIVE-29598: vectorized outer-join MapJoin can leave a
-- stale typed value in a scratch slot that the vectorizer has aliased to a
-- smallTableValueMapping target. For unmatched rows, generateOuterNulls()
-- flips isNull[i] = true but does not clear vector[i], so a downstream
-- ColOrCol -> IfExprLongScalarLongScalar chain that reads vector[i] without
-- consulting isNull[i] propagates the stale value into the result.
--
-- Repro shape: a LEFT OUTER MapJoin whose ON predicate uses a CAST scratch
-- column that is then reused as the small-table boolean-value column; the
-- projection computes CAST((s.s_bool OR p.p_bool) AS INT) over the null-padded
-- rows. The MAX() aggregate barrier prevents Calcite from inlining and
-- simplifying the bug surface away.
--
-- Expected: zero rows. Every probe row's (s_bool OR p_bool) is TRUE per SQL
-- three-valued logic (matched: FALSE OR TRUE; unmatched: NULL OR TRUE), so
-- CAST(... AS INT) is always 1 and WHERE observed_value = 0 matches nothing.
-- Without the fix: 'C 3 0 1' and 'D 3 0 1' leak through, since the stale int
-- from CastStringToLong ORed with 1 fails the strict == 1 check in
-- IfExprLongScalarLongScalar and stores 0.

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
