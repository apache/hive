CREATE EXTERNAL TABLE ice_t (i int, v VARIANT) STORED BY ICEBERG tblproperties
('format-version'='3');
SHOW CREATE TABLE ice_t;

-- Hack as of now to manually pass the Variant metadata and value as binary. 03 in the value indicates integer type and 05 is the actual integer value.
INSERT INTO ice_t VALUES
(1, named_struct('metadata', unhex('0100000000'), 'value', unhex('0305')));
SELECT * FROM ice_t;

-- this direct read gives those binaries, telling what we wrote came back.
SELECT i, hex(v.metadata) as metadata_hex, hex(v.value) as value_hex FROM ice_t;

-- Just check the type for now untill we have the UDF's
SELECT
    i,
    case when substring(hex(v.value), 1, 2) = '03' then 'integer'
         when substring(hex(v.value), 1, 2) = '01' then 'string'
         else 'unknown' end as value_type,
    hex(v.value) as raw_value
FROM ice_t;

-- For integers, we can extract the value to see if it is 5, that is what we inserted.
SELECT
    i,
    case when substring(hex(v.value), 1, 2) = '03' then conv(substring(hex(v.value), 3, 2), 16, 10)
         else 'not_integer' end as int_value
FROM ice_t;