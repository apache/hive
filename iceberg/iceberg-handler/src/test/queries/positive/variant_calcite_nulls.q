-- VARIANT-typed values that Calcite creates while planning (HIVE-30170)
set hive.cbo.enable=true;
set hive.fetch.task.conversion=none;

drop table if exists vcn;
CREATE EXTERNAL TABLE vcn (id INT, v VARIANT) STORED BY ICEBERG TBLPROPERTIES ('format-version'='3');
INSERT INTO vcn VALUES (1, parse_json('{"a":1}')), (2, parse_json('[1,2]'));

-- empty plans: CAST(NULL AS variant) through ASTConverter.convertType
SELECT v FROM vcn WHERE false;
SELECT v FROM vcn LIMIT 0;
-- a variant expression over a literal: Calcite cannot fold it into a literal, so it is evaluated per row
EXPLAIN CBO SELECT parse_json('{"k":1}') FROM vcn;
SELECT id, parse_json('{"k":1}') FROM vcn ORDER BY id;

-- a VARIANT-typed NULL that Calcite folds out of an expression stays VARIANT for the UDF over it
SELECT id, to_json(IF(false, v, NULL)) FROM vcn ORDER BY id;
SELECT id, variant_get(CASE WHEN false THEN v END, '$.a', 'int') FROM vcn ORDER BY id;

-- a top-level VARIANT-typed NULL under vectorization: a repeating null of the variant's struct column
set hive.vectorized.execution.enabled=true;
EXPLAIN VECTORIZATION ONLY SELECT id, IF(false, v, NULL) FROM vcn;
SELECT id, IF(false, v, NULL) FROM vcn ORDER BY id;
