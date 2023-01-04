--! qt:dataset:src

EXPLAIN CBO
SELECT *
FROM src src1
  JOIN src src2 ON (src1.key = src2.key)
  JOIN src src3 ON (src1.key = src3.key)
WHERE src1.key > 10 and src1.key < 20;

set hive.cbo.rule.exclusion.regex=HiveJoinPushTransitivePredicatesRule;

EXPLAIN CBO
SELECT *
FROM src src1
  JOIN src src2 ON (src1.key = src2.key)
  JOIN src src3 ON (src1.key = src3.key)
WHERE src1.key > 10 and src1.key < 20;

set hive.cbo.rule.exclusion.regex=HiveJoinPushTransitivePredicatesRule|HiveJoinAddNotNullRule;

EXPLAIN CBO
SELECT *
FROM src src1
  JOIN src src2 ON (src1.key = src2.key)
  JOIN src src3 ON (src1.key = src3.key)
WHERE src1.key > 10 and src1.key < 20;

set hive.cbo.rule.exclusion.regex=HiveJoin.*Rule;

EXPLAIN CBO
SELECT *
FROM src src1
  JOIN src src2 ON (src1.key = src2.key)
  JOIN src src3 ON (src1.key = src3.key)
WHERE src1.key > 10 and src1.key < 20;

set hive.cbo.rule.exclusion.regex=.*;

EXPLAIN CBO
SELECT *
FROM src src1
  JOIN src src2 ON (src1.key = src2.key)
  JOIN src src3 ON (src1.key = src3.key)
WHERE src1.key > 10 and src1.key < 20;
