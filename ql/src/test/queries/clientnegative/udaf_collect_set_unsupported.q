--! qt:dataset:src
set hive.cbo.fallback.strategy=NEVER;

SELECT key, collect_set(create_union(value))
FROM src
GROUP BY key ORDER BY key limit 20;
