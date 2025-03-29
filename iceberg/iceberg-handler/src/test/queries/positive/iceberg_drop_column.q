CREATE TABLE iceberg_test_table (
    id INT,
    name STRING,
    age INT,
    salary DOUBLE
) STORED BY ICEBERG;

INSERT INTO iceberg_test_table VALUES (1, 'Alice', 25, 50000.0);

ALTER TABLE iceberg_test_table DROP COLUMN IF EXISTS age;

SELECT * FROM iceberg_test_table;
