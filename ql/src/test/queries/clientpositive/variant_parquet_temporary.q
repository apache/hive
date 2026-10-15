-- variant_get passes its path down as a nested column path; a Parquet VARIANT column is still read whole
set hive.explain.user=false;
set hive.fetch.task.conversion=more;

CREATE TEMPORARY TABLE vpt (id INT, v VARIANT, s STRUCT<v:VARIANT,n:INT>) STORED AS PARQUET;
INSERT INTO vpt VALUES
(1, parse_json('{"a":15,"b":"x"}'), named_struct('v', parse_json('{"a":25}'), 'n', 1)),
(2, parse_json('{"a":16}'), named_struct('v', parse_json('{"c":true}'), 'n', 2));

-- a fetch task over the temporary table
EXPLAIN SELECT id, variant_get(v, '$.a', 'int'), variant_get(s.v, '$.a', 'int') FROM vpt;
SELECT id, variant_get(v, '$.a', 'int'), variant_get(s.v, '$.a', 'int') FROM vpt;
SELECT id FROM vpt WHERE variant_get(v, '$.a', 'int') = 16;

-- a fetch task over a CTE materialized as Parquet
set hive.default.fileformat=parquet;
set hive.optimize.cte.materialize.threshold=1;
set hive.optimize.cte.materialize.full.aggregate.only=false;
EXPLAIN WITH c AS (SELECT id, v, s FROM vpt)
SELECT id, variant_get(v, '$.a', 'int'), variant_get(s.v, '$.a', 'int') FROM c;
WITH c AS (SELECT id, v, s FROM vpt) SELECT id, variant_get(v, '$.a', 'int'), variant_get(s.v, '$.a', 'int') FROM c;

-- row-mode tasks
set hive.fetch.task.conversion=none;
set hive.vectorized.execution.enabled=false;
SELECT id, variant_get(v, '$.a', 'int'), variant_get(s.v, '$.a', 'int') FROM vpt ORDER BY id;
WITH c AS (SELECT id, v, s FROM vpt)
SELECT id, variant_get(v, '$.a', 'int'), variant_get(s.v, '$.a', 'int') FROM c ORDER BY id;

-- a key that contains a comma
set hive.fetch.task.conversion=more;
INSERT INTO vpt VALUES (3, parse_json('{"a,b":{"c":7}}'), named_struct('v', parse_json('{}'), 'n', 3));
SELECT id, variant_get(v, '$.a,b.c', 'int') FROM vpt WHERE id = 3;
