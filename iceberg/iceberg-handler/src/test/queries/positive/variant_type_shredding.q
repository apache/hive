-- Mask random uuid
--! qt:replace:/(\s+'uuid'=')\S+('\s*)/$1#Masked#$2/
-- Mask random snapshot id
--! qt:replace:/('current-snapshot-id'=')\d+/$1#SnapshotId#/
-- Mask current-snapshot-timestamp-ms
--! qt:replace:/('current-snapshot-timestamp-ms'=')\d+/$1#Masked#/

-- SORT_QUERY_RESULTS
set hive.explain.user=false;
set hive.fetch.task.conversion=none;

drop table if exists shredded_variant;

-- Create test table
CREATE EXTERNAL TABLE shredded_variant (
  id INT,
  data VARIANT
) STORED BY ICEBERG
TBLPROPERTIES (
  'format-version'='3',
  'variant.shredding.enabled'='true'
);

INSERT INTO shredded_variant VALUES
(1, parse_json('{"name": "John", "age": 30}')),
(2, parse_json('{"name": "Bill"}')),
(3, parse_json('{"name": "Henry", "address": {"city": "NYC"}}'));

-- Insert NULL variant
INSERT INTO shredded_variant (id) SELECT 4;

-- Retrieve and verify
SELECT id,
  variant_get(data, '$.name') AS name,
  variant_get(data, '$.age', 'int') AS age,
  variant_get(data, '$.address.city') AS city
FROM shredded_variant;
