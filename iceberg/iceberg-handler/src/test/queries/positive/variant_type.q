-- Mask random uuid
--! qt:replace:/(\s+'uuid'=')\S+('\s*)/$1#Masked#$2/
--! qt:replace:/(\s+uuid\s+)\S+/$1#Masked#/
-- Mask random snapshot id
--! qt:replace:/('current-snapshot-id'=')\d+/$1#SnapshotId#/
-- Mask current-snapshot-timestamp-ms
--! qt:replace:/('current-snapshot-timestamp-ms'=')\d+/$1#Masked#/

-- Create test table
CREATE EXTERNAL TABLE variant_test_basic (
    id INT,
    data VARIANT
) STORED BY ICEBERG tblproperties('format-version'='3');

show create table variant_test_basic;

describe variant_test_basic;

describe formatted variant_test_basic;

-- Insert primitive types
INSERT INTO variant_test_basic VALUES
(1, parse_json('null')),
(2, parse_json('true')),
(3, parse_json('false')),
(4, parse_json('42')),
(5, parse_json('3.14')),
(6, parse_json('"hello world"'));

-- Retrieve and verify
SELECT id, to_json(data) as json_data FROM variant_test_basic ORDER BY id;

-- Create table for complex structures
CREATE EXTERNAL TABLE variant_test_complex (
    id INT,
    data VARIANT
) STORED BY ICEBERG tblproperties('format-version'='3');;

-- Insert complex JSON structures
INSERT INTO variant_test_complex VALUES
(1, parse_json('{"name": "John", "age": 30, "active": true}')),
(2, parse_json('{"nested": {"level1": {"level2": "deep"}}, "array": [1, 2, 3]}')),
(3, parse_json('["apple", "banana", "cherry"]')),
(4, parse_json('{"mixed": [1, "text", true, null, {"key": "value"}]}')),
(5, parse_json('{"empty_obj": {}, "empty_array": [], "null_val": null}'));

-- Retrieve and verify
SELECT id, to_json(data) as json_data FROM variant_test_complex ORDER BY id;

-- Create table for edge cases
CREATE EXTERNAL TABLE variant_test_edge_cases (
    id INT,
    data VARIANT
) STORED BY ICEBERG tblproperties('format-version'='3');;

-- Insert edge cases
INSERT INTO variant_test_edge_cases VALUES
(1, parse_json('{"very_long_string": "This is a very long string that should test the string encoding limits and ensure proper handling of large text content in variant types"}')),
(2, parse_json('{"large_number": 123456789012345}')),
(3, parse_json('{"decimal_value": 123.456789}')),
(4, parse_json('{"special_chars": "Hello\\tWorld\\nNew Line! \\"Quoted\\""}')),
(5, parse_json('{"unicode": "Hello 世界 🌍"}')),
(6, parse_json('{"deep_nesting": {"level1": {"level2": {"level3": {"level4": "deep"}}}}}'));

-- Retrieve and verify
SELECT id, to_json(data) as json_data FROM variant_test_edge_cases ORDER BY id;

-- Create table for multiple operations
CREATE TABLE variant_test_operations (
    id INT,
    metadata VARIANT,
    payload VARIANT
) STORED BY ICEBERG tblproperties('format-version'='3');

-- Insert data with multiple variant columns
INSERT INTO variant_test_operations VALUES
(1,
 parse_json('{"timestamp": "2023-01-01", "version": "1.0"}'),
 parse_json('{"user": "john_doe", "actions": ["login", "view", "logout"]}')
),
(2,
 parse_json('{"timestamp": "2023-01-02", "version": "1.1"}'),
 parse_json('{"user": "jane_smith", "actions": ["login", "edit", "save", "logout"]}')
);

-- Complex queries with variant data
SELECT
    id,
    to_json(metadata) as metadata_json,
    to_json(payload) as payload_json
FROM variant_test_operations
ORDER BY id;

-- Test null values
SELECT variant_get(data, '$') as result FROM variant_test_basic WHERE id = 1;

-- Test boolean true
SELECT variant_get(data, '$') as result FROM variant_test_basic WHERE id = 2;

-- Test boolean false
SELECT variant_get(data, '$') as result FROM variant_test_basic WHERE id = 3;

-- Test object field access
SELECT variant_get(data, '$.name') as name FROM variant_test_complex WHERE id = 1;

SELECT variant_get(data, '$.age') as age FROM variant_test_complex WHERE id = 1;

SELECT variant_get(data, '$.active') as active FROM variant_test_complex WHERE id = 1;

-- Test nested object access
SELECT variant_get(data, '$.nested.level1.level2') as deep_value FROM variant_test_complex WHERE id = 2;

-- Test array access
SELECT variant_get(data, '$[0]') as first_element FROM variant_test_complex WHERE id = 3;

SELECT variant_get(data, '$[1]') as second_element FROM variant_test_complex WHERE id = 3;

SELECT variant_get(data, '$[2]') as third_element FROM variant_test_complex WHERE id = 3;

-- Test mixed array access
SELECT variant_get(data, '$.mixed[0]') as first_mixed FROM variant_test_complex WHERE id = 4;

SELECT variant_get(data, '$.mixed[1]') as second_mixed FROM variant_test_complex WHERE id = 4;

SELECT variant_get(data, '$.mixed[2]') as third_mixed FROM variant_test_complex WHERE id = 4;

SELECT variant_get(data, '$.mixed[3]') as fourth_mixed FROM variant_test_complex WHERE id = 4;

SELECT variant_get(data, '$.mixed[4].key') as nested_key FROM variant_test_complex WHERE id = 4;

-- Test empty structures
SELECT variant_get(data, '$.empty_obj') as empty_obj FROM variant_test_complex WHERE id = 5;

SELECT variant_get(data, '$.empty_array') as empty_array FROM variant_test_complex WHERE id = 5;

SELECT variant_get(data, '$.null_val') as null_val FROM variant_test_complex WHERE id = 5;

-- Test long string
SELECT variant_get(data, '$.very_long_string') as long_string FROM variant_test_edge_cases WHERE id = 1;

-- Test large number
SELECT variant_get(data, '$.large_number') as large_num FROM variant_test_edge_cases WHERE id = 2;

-- Test decimal value
SELECT variant_get(data, '$.decimal_value') as decimal_val FROM variant_test_edge_cases WHERE id = 3;

-- Test special characters
SELECT variant_get(data, '$.special_chars') as special_chars FROM variant_test_edge_cases WHERE id = 4;

-- Test unicode
SELECT variant_get(data, '$.unicode') as unicode_str FROM variant_test_edge_cases WHERE id = 5;

-- Test deep nesting
SELECT variant_get(data, '$.deep_nesting.level1.level2.level3.level4') as deep_value FROM variant_test_edge_cases WHERE id = 6;

-- Test type casting with primitive values
SELECT
    variant_get(data, '$', 'string') as as_string,
    variant_get(data, '$', 'int') as as_int,
    variant_get(data, '$', 'double') as as_double,
    variant_get(data, '$', 'boolean') as as_boolean
FROM variant_test_basic WHERE id = 4;

-- Test type casting with string values
SELECT
    variant_get(data, '$', 'string') as as_string,
    variant_get(data, '$', 'int') as as_int, -- Should be null
    variant_get(data, '$', 'double') as as_double -- Should be null
FROM variant_test_basic WHERE id = 6;

-- Test type casting with object fields
SELECT
    variant_get(data, '$.age', 'string') as age_string,
    variant_get(data, '$.age', 'int') as age_int,
    variant_get(data, '$.age', 'double') as age_double
FROM variant_test_complex WHERE id = 1;

-- Validate complex structures
SELECT
    id,
    variant_get(data, '$.name') as name,
    variant_get(data, '$.age') as age,
    variant_get(data, '$.active') as active
FROM variant_test_complex
WHERE id = 1;

-- Validate array access
SELECT
    id,
    variant_get(data, '$[0]') as elem0,
    variant_get(data, '$[1]') as elem1,
    variant_get(data, '$[2]') as elem2
FROM variant_test_complex
WHERE id = 3;

-- try with AVRO table
CREATE EXTERNAL TABLE variant_test_basic_avro (
    id INT,
    data VARIANT
) STORED BY ICEBERG stored as avro tblproperties('format-version'='3');

-- Insert timestamp types
INSERT INTO variant_test_basic_avro VALUES
(7, parse_json('"2023-01-01T12:00:00.123456Z"')),
(8, parse_json('"2023-01-01T12:00:00.123456"')),
(9, parse_json('"2023-01-01T12:00:00.123456789Z"')),
(10, parse_json('"2023-01-01T12:00:00.123456789"')),
(11, parse_json('"12:30:45.123456"')),
(12, parse_json('"2023-12-25"'));

-- Retrieve and verify timestamps
SELECT id, to_json(data) as json_data FROM variant_test_basic_avro;

-- Add a variant type column to an existing table
ALTER TABLE variant_test_basic ADD COLUMNS (extra_info VARIANT);
INSERT INTO variant_test_basic VALUES
(7, parse_json('{"key": "value"}'), parse_json('{"additional": "info"}'));

select id, to_json(data), to_json(extra_info) from variant_test_basic where id = 7;