-- Test Iceberg tables with CLUSTERED BY clause
-- Verify that Iceberg tables follow Hive bucketing behavior for query optimization

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

INSERT INTO ice_bucketed_simple VALUES (1, 'Alice', 25), (2, 'Bob', 30);
INSERT INTO ice_bucketed_simple VALUES (3, 'Carrie', 25), (4, 'Danny', 30);

SELECT * FROM ice_bucketed_simple ORDER BY id;

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
