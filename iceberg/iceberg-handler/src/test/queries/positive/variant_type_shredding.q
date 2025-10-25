-- Mask random uuid
--! qt:replace:/(\s+'uuid'=')\S+('\s*)/$1#Masked#$2/
-- Mask random snapshot id
--! qt:replace:/('current-snapshot-id'=')\d+/$1#SnapshotId#/
-- Mask current-snapshot-timestamp-ms
--! qt:replace:/('current-snapshot-timestamp-ms'=')\d+/$1#Masked#/

-- Create test table
CREATE EXTERNAL TABLE variant_test_shredding (
    id INT,
    data VARIANT
) STORED BY ICEBERG
tblproperties(
    'format-version'='3',
    'variant.shredding.enabled'='true'
);

-- Insert JSON structures
INSERT INTO variant_test_shredding VALUES
(1, parse_json('{"name": "John", "age": 30, "active": true}'));

-- Retrieve and verify
SELECT id, to_json(data) as json_data FROM variant_test_shredding ORDER BY id;

