set hive.optimize.join.disjunctive.transitive.predicates.pushdown=false;

CREATE TABLE tableA (
  bd_id      bigint,
  quota_type string
);

EXPLAIN CBO
SELECT a.bd_id
FROM (
    SELECT t.bd_id
    FROM tableA t
    WHERE (t.bd_id = 8 AND t.quota_type IN ('A','C')) OR (t.bd_id = 9 AND t.quota_type IN  ('A','B'))
 ) a JOIN (
     SELECT t.bd_id
     FROM tableA t
     WHERE t.bd_id = 9 AND t.quota_type IN ('A','B')
     UNION ALL
     SELECT t.bd_id
     FROM tableA t
     WHERE (t.bd_id = 8 AND t.quota_type IN ('A','C')) OR (t.bd_id = 9 AND t.quota_type IN ('A','B'))
) b ON a.bd_id = b.bd_id
WHERE a.bd_id = 8 OR a.bd_id <> 8;
