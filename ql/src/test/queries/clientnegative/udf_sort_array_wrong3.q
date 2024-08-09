--! qt:dataset:src
-- invalid argument type
set hive.cbo.fallback.strategy=NEVER;
SELECT sort_array(array(create_union(0,"a"))) FROM src LIMIT 1;
