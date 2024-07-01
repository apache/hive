-- union is not supported
set hive.cbo.fallback.strategy=NEVER;
SELECT NULLIF(create_union(0,1,2),create_union(0,1,3))

