set hive.optimize.join.disjunctive.transitive.predicates.pushdown=false;

CREATE EXTERNAL TABLE table2 (
  tenant_id int
) PARTITIONED BY (date_key int)
STORED AS PARQUET;

CREATE EXTERNAL TABLE tenant_1 (
  tenant_id int,
  tenant_key bigint
) STORED AS PARQUET;

EXPLAIN CBO
SELECT * FROM (
   SELECT date_key, tenant_id
   FROM  table2
   WHERE tenant_id = 0
   UNION ALL
   SELECT date_key, tenant_id
   FROM  table2
   WHERE tenant_id <> 0
) a
JOIN tenant_1 dt
  ON a.tenant_id = dt.tenant_id;
