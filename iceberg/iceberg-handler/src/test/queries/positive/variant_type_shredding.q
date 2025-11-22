-- Mask random uuid
--! qt:replace:/(\s+'uuid'=')\S+('\s*)/$1#Masked#$2/
-- Mask random snapshot id
--! qt:replace:/('current-snapshot-id'=')\d+/$1#SnapshotId#/
-- Mask current-snapshot-timestamp-ms
--! qt:replace:/('current-snapshot-timestamp-ms'=')\d+/$1#Masked#/

-- SORT_QUERY_RESULTS
set hive.explain.user=false;
set hive.fetch.task.conversion=none;

drop table if exists tbl_shredded_variant;

-- Create test table
CREATE EXTERNAL TABLE tbl_shredded_variant (
    id INT,
    data VARIANT
) STORED BY ICEBERG
tblproperties(
    'format-version'='3',
    'variant.shredding.enabled'='true'
);

-- Insert JSON structures
INSERT INTO tbl_shredded_variant VALUES
(1, parse_json('{"name": "John", "age": 30, "active": true}')),
(2, parse_json('{"name": "Bill", "active": false}')),
(3, parse_json('{"name": "Henry", "age": 20}'));

-- Disable vectorized execution until Variant type is supported
set hive.vectorized.execution.enabled=false;

-- Retrieve and verify
SELECT id, try_variant_get(data, '$.name') FROM tbl_shredded_variant
WHERE variant_get(data, '$.age') > 25;

EXPLAIN
SELECT id, try_variant_get(data, '$.name') FROM tbl_shredded_variant
WHERE variant_get(data, '$.age') > 25;
