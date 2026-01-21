-- Mask random uuid
--! qt:replace:/(\s+'uuid'=')\S+('\s*)/$1#Masked#$2/
-- Mask random snapshot id
--! qt:replace:/('current-snapshot-id'=')\d+/$1#SnapshotId#/
-- Mask current-snapshot-timestamp-ms
--! qt:replace:/('current-snapshot-timestamp-ms'=')\d+/$1#Masked#/

-- SORT_QUERY_RESULTS
set hive.explain.user=false;
set hive.fetch.task.conversion=none;

CREATE EXTERNAL TABLE variant_filter_basic (
    id BIGINT,
    data VARIANT
) STORED BY ICEBERG tblproperties('format-version'='3');

INSERT INTO variant_filter_basic VALUES
(1, parse_json('{ "name": "Alice", "age": 30, "address": {"city": "Wonderland"} }')),
(2, parse_json('{ "name": "Bob", "age": 40, "address": {"city": "Builderland"} }')),
(3, parse_json('{ "name": "Charlie", "age": 28, "address": {"city": "Dreamtown"} }'));

SELECT
  try_variant_get(data, '$.name') AS name,
  try_variant_get(data, '$.age', 'int') AS age,
  try_variant_get(data, '$.address.city') AS city
FROM variant_filter_basic;

SELECT
  try_variant_get(data, '$.name') AS name,
  try_variant_get(data, '$.age', 'int') AS age,
  try_variant_get(data, '$.address.city') AS city
FROM variant_filter_basic
WHERE try_variant_get(data, '$.age', 'int') >= 30;

EXPLAIN SELECT
  try_variant_get(data, '$.name') AS name,
  try_variant_get(data, '$.age', 'int') AS age,
  try_variant_get(data, '$.address.city') AS city
FROM variant_filter_basic
WHERE try_variant_get(data, '$.age', 'int') >= 30;