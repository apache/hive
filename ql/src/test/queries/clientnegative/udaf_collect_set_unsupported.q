--! qt:dataset:src
SELECT key, collect_set(create_union(value))
FROM src
GROUP BY key ORDER BY key limit 20;
