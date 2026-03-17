-- Mask random uuid
--! qt:replace:/(\s+'uuid'=')\S+('\s*)/$1#Masked#$2/
-- Mask random snapshot id
--! qt:replace:/('current-snapshot-id'=')\d+/$1#SnapshotId#/
-- Mask current-snapshot-timestamp-ms
--! qt:replace:/('current-snapshot-timestamp-ms'=')\d+/$1#Masked#/

-- SORT_QUERY_RESULTS
set hive.explain.user=false;
set hive.fetch.task.conversion=none;

drop table if exists variant_filter_basic;

CREATE EXTERNAL TABLE variant_filter_basic (
  id INT,
  data VARIANT
) STORED BY ICEBERG
TBLPROPERTIES (
  'format-version'='3'
);

INSERT INTO variant_filter_basic VALUES
(1, parse_json('{"name": "John", "address": {"city": "Toronto"}}')),
(2, parse_json('{"name": "Bill", "age": 28}')),
(3, parse_json('{"name": "Henry", "age": 30, "address": {"city": "NYC"}}'));

-- Retrieve and verify
SELECT id,
  variant_get(data, '$.name') AS name,
  variant_get(data, '$.age', 'int') AS age,
  variant_get(data, '$.address.city') AS city
FROM variant_filter_basic
WHERE variant_get(data, '$.age', 'int') >= 30;

-- Test PPD on shredded variant
drop table if exists shredded_variant_ppd;

CREATE EXTERNAL TABLE shredded_variant_ppd (
  id INT,
  data VARIANT
) STORED BY ICEBERG
TBLPROPERTIES (
  'format-version'='3',
  'variant.shredding.enabled'='true'
);

INSERT INTO shredded_variant_ppd VALUES
(1, parse_json('{"name": "John", "address": {"city": "Toronto"}}')),
(2, parse_json('{"name": "Bill", "age": 28}')),
(3, parse_json('{"name": "Henry", "age": 30, "address": {"city": "NYC"}}'));

-- Insert NULL variant
INSERT INTO shredded_variant_ppd (id) SELECT 4;

-- Retrieve and verify
SELECT id,
  variant_get(data, '$.name') AS name,
  variant_get(data, '$.age', 'int') AS age,
  variant_get(data, '$.address.city') AS city
FROM shredded_variant_ppd
WHERE variant_get(data, '$.age', 'int') >= 30;

EXPLAIN
SELECT id,
  variant_get(data, '$.name') AS name,
  variant_get(data, '$.age', 'int') AS age,
  variant_get(data, '$.address.city') AS city
FROM shredded_variant_ppd
WHERE variant_get(data, '$.age', 'int') >= 30;

-- Test IS NOT NULL on entire variant column (should return rows 1-3)
SELECT id, variant_get(data, '$.name') AS name FROM shredded_variant_ppd
WHERE data IS NOT NULL;

EXPLAIN
SELECT id, variant_get(data, '$.name') FROM shredded_variant_ppd
WHERE data IS NOT NULL;

-- Test IS NOT NULL on field that exists in some rows (should return rows 1, 3)
SELECT id, variant_get(data, '$.name') AS name FROM shredded_variant_ppd
WHERE variant_get(data, '$.address.city') IS NOT NULL;

EXPLAIN
SELECT id, variant_get(data, '$.name') FROM shredded_variant_ppd
WHERE variant_get(data, '$.address.city') IS NOT NULL;

-- Test IS NULL on field that exists in some rows (should return rows 2, 4)
SELECT id, variant_get(data, '$.name') AS name FROM shredded_variant_ppd
WHERE variant_get(data, '$.address.city') IS NULL
