-- Test Iceberg tables with CLUSTERED BY clause
-- Verify that Iceberg tables follow Hive bucketing behavior for query optimization

-- SORT_QUERY_RESULTS
-- Mask random uuid
--! qt:replace:/(\s+uuid\s+)\S+(\s*)/$1#Masked#$2/
-- Mask a random snapshot id
--! qt:replace:/(\s+current-snapshot-id\s+)\S+(\s*)/$1#Masked#/
-- Mask added file size
--! qt:replace:/(\S\"added-files-size\\\":\\\")(\d+)(\\\")/$1#Masked#$3/
-- Mask total file size
--! qt:replace:/(\S\"total-files-size\\\":\\\")(\d+)(\\\")/$1#Masked#$3/
-- Mask current-snapshot-timestamp-ms
--! qt:replace:/(\s+current-snapshot-timestamp-ms\s+)\S+(\s*)/$1#Masked#$2/
-- Mask removed file size
--! qt:replace:/(\S\"removed-files-size\\\":\\\")(\d+)(\\\")/$1#Masked#$3/
-- Mask number of files
--! qt:replace:/(\s+numFiles\s+)\S+(\s+)/$1#Masked#$2/
-- Mask total data files
--! qt:replace:/(\S\"total-data-files\\\":\\\")(\d+)(\\\")/$1#Masked#$3/
-- Mask iceberg version
--! qt:replace:/(\S\"iceberg-version\\\":\\\")(\w+\s\w+\s\d+\.\d+\.\d+\s\(\w+\s\w+\))(\\\")/$1#Masked#$3/

-- Simple CREATE with CLUSTERED BY
CREATE TABLE ice_bucketed_simple (
    id int,
    name string,
    age int
)
CLUSTERED BY (id) INTO 4 BUCKETS
STORED BY ICEBERG;

DESCRIBE FORMATTED ice_bucketed_simple;

EXPLAIN INSERT INTO ice_bucketed_simple VALUES (1, 'Alice', 25), (2, 'Bob', 30);

INSERT INTO ice_bucketed_simple VALUES
    (1,  'Alice',   25),
    (2,  'Bob',     30),
    (3,  'Carrie',  22),
    (4,  'Danny',   28),
    (5,  'Eve',     35),
    (6,  'Frank',   40),
    (7,  'Grace',   27),
    (8,  'Henry',   33),
    (9,  'Ivy',     29),
    (10, 'Jack',    31);

SELECT * FROM ice_bucketed_simple ORDER BY id;

SELECT COUNT(*) from default.ice_bucketed_simple.files;

-- CLUSTERED BY on multiple columns
CREATE TABLE ice_bucketed_multi (
    customer_id int,
    order_id bigint,
    product string
)
CLUSTERED BY (customer_id, order_id) INTO 8 BUCKETS
STORED BY ICEBERG;

DESCRIBE FORMATTED ice_bucketed_multi;

EXPLAIN INSERT INTO ice_bucketed_multi VALUES (100, 1001, 'Widget'), (101, 1002, 'Gadget');

-- Convert EXTERNAL Hive table with CLUSTERED BY to Iceberg
CREATE EXTERNAL TABLE hive_bucketed (
    product_id int,
    category string,
    price decimal(10,2)
)
CLUSTERED BY (product_id) INTO 3 BUCKETS
STORED AS ORC;

INSERT INTO hive_bucketed VALUES (1, 'Electronics', 299.99), (2, 'Books', 15.50);

ALTER TABLE hive_bucketed CONVERT TO ICEBERG;

DESCRIBE FORMATTED hive_bucketed;

EXPLAIN INSERT INTO hive_bucketed VALUES (3, 'Furniture', 450.00);

DROP TABLE IF EXISTS ice_bucketed_simple;
DROP TABLE IF EXISTS ice_bucketed_multi;
DROP TABLE IF EXISTS hive_bucketed;
