-- SORT_QUERY_RESULTS
set hive.explain.user=false;

drop table if exists variant_proj;

CREATE EXTERNAL TABLE variant_proj (
  id INT,
  data VARIANT
) STORED BY ICEBERG
TBLPROPERTIES (
  'format-version'='3'
);

INSERT INTO variant_proj VALUES
(1, parse_json('{"name": "John", "age": 30}')),
(2, parse_json('[1, 2, 3]')),
(3, parse_json('"plain string"')),
(4, parse_json('42'));

INSERT INTO variant_proj (id) SELECT 5;

-- raw projection: variants are returned as their logical JSON
EXPLAIN SELECT id, data FROM variant_proj;

SELECT id, data FROM variant_proj;

SELECT * FROM variant_proj;

-- raw projection across a shuffle
SELECT id, data FROM variant_proj ORDER BY id DESC;

-- variant nested inside a struct is decoded within the folded output
-- (array<variant>/map<..,variant> reads fail in Iceberg column projection; tracked separately)
drop table if exists variant_nested;

CREATE EXTERNAL TABLE variant_nested (
  id INT,
  s STRUCT<label:STRING, v:VARIANT>
) STORED BY ICEBERG
TBLPROPERTIES (
  'format-version'='3'
);

INSERT INTO variant_nested SELECT
  1,
  named_struct('label', 'x', 'v', parse_json('{"k": 1}'));

SELECT * FROM variant_nested;

SELECT id, to_json(s.v) FROM variant_nested;

-- a plain struct with the variant shape keeps the generic struct rendering
drop table if exists bin_struct;

CREATE EXTERNAL TABLE bin_struct (
  id INT,
  s STRUCT<metadata:BINARY, value:BINARY>
) STORED BY ICEBERG;

INSERT INTO bin_struct SELECT 1, named_struct('metadata', cast('x' as binary), 'value', cast('y' as binary));

SELECT * FROM bin_struct;

drop table variant_proj;
drop table variant_nested;
drop table bin_struct;
