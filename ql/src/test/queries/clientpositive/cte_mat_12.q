set hive.optimize.cte.materialize.full.aggregate.only=false;
set hive.optimize.cte.materialize.threshold=2;

EXPLAIN
WITH x AS (SELECT null AS null_value, 1 AS non_null_value)
SELECT *, TYPEOF(null_value) FROM x UNION ALL SELECT *, TYPEOF(null_value) FROM x;

WITH x AS (SELECT null AS null_value, 1 AS non_null_value)
SELECT *, TYPEOF(null_value) FROM x UNION ALL SELECT *, TYPEOF(null_value) FROM x;
