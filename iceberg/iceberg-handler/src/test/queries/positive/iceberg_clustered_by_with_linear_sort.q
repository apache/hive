-- Test combining Hive CLUSTERED BY with Iceberg linear WRITE LOCALLY ORDERED BY
-- Mask neededVirtualColumns due to non-strict order
--! qt:replace:/(\s+neededVirtualColumns:\s)(.*)/$1#Masked#/
-- Mask the totalSize value as it can have slight variability, causing test flakiness
--! qt:replace:/(\s+totalSize\s+)\S+(\s+)/$1#Masked#$2/
-- Mask random uuid
--! qt:replace:/(\s+uuid\s+)\S+(\s*)/$1#Masked#$2/
-- Mask a random snapshot id
--! qt:replace:/(\s+current-snapshot-id\s+)\S+(\s*)/$1#Masked#/
-- Mask added file size
--! qt:replace:/(\S\"added-files-size\\\":\\\")(\d+)(\\\")/$1#Masked#$3/
-- Mask total file size
--! qt:replace:/(\S\"total-files-size\\\":\\\")(\d+)(\\\")/$1#Masked#$3/
-- Mask removed file size
--! qt:replace:/(\S\"removed-files-size\\\":\\\")(\d+)(\\\")/$1#Masked#$3/
-- Mask current-snapshot-timestamp-ms
--! qt:replace:/(\s+current-snapshot-timestamp-ms\s+)\S+(\s*)/$1#Masked#$2/
--! qt:replace:/(MAJOR\s+succeeded\s+)[a-zA-Z0-9\-\.\s+]+(\s+manual)/$1#Masked#$2/
-- Mask iceberg version
--! qt:replace:/(\S\"iceberg-version\\\":\\\")(\w+\s\w+\s\d+\.\d+\.\d+\s\(\w+\s\w+\))(\\\")/$1#Masked#$3/
-- Mask data file paths in virtual-column reads
--! qt:replace:/file:[^\t]*/file:#Masked#/

set hive.llap.io.enabled=true;
set hive.vectorized.execution.enabled=true;
set hive.optimize.shared.work.merge.ts.schema=true;

CREATE TABLE clustered_linear_sales
        (customer_id BIGINT, order_date DATE, amount DOUBLE, region STRING)
        CLUSTERED BY (customer_id) INTO 3 BUCKETS
        WRITE LOCALLY ORDERED BY order_date ASC, amount DESC
        STORED BY ICEBERG STORED AS PARQUET
        TBLPROPERTIES ('format-version'='2');

DESCRIBE FORMATTED clustered_linear_sales;

EXPLAIN INSERT INTO clustered_linear_sales VALUES
                (1, DATE '2024-01-15', 125.50, 'North'),
                (2, DATE '2024-02-20', 89.75, 'South'),
                (3, DATE '2024-01-10', 245.30, 'East'),
                (1, DATE '2024-03-05', 67.90, 'North'),
                (4, DATE '2024-02-28', 178.45, 'West'),
                (2, DATE '2024-01-22', 312.80, 'South'),
                (5, DATE '2024-03-12', 156.20, 'East'),
                (3, DATE '2024-02-14', 234.65, 'East'),
                (6, DATE '2024-01-30', 98.40, 'West');

INSERT INTO clustered_linear_sales VALUES
                (1, DATE '2024-01-15', 125.50, 'North'),
                (2, DATE '2024-02-20', 89.75, 'South'),
                (3, DATE '2024-01-10', 245.30, 'East'),
                (1, DATE '2024-03-05', 67.90, 'North'),
                (4, DATE '2024-02-28', 178.45, 'West'),
                (2, DATE '2024-01-22', 312.80, 'South'),
                (5, DATE '2024-03-12', 156.20, 'East'),
                (3, DATE '2024-02-14', 234.65, 'East'),
                (6, DATE '2024-01-30', 98.40, 'West');

SELECT COUNT(*) as total_rows FROM clustered_linear_sales;

SELECT COUNT(*) from default.clustered_linear_sales.files;

-- Physical scan order within each Hive bucket should follow order_date ASC, amount DESC.

SELECT s.order_date, s.amount, s.customer_id, s.ROW__POSITION
FROM clustered_linear_sales TABLESAMPLE (BUCKET 1 OUT OF 3 ON customer_id) s
ORDER BY s.ROW__POSITION;

SELECT s.order_date, s.amount, s.customer_id
FROM clustered_linear_sales TABLESAMPLE (BUCKET 1 OUT OF 3 ON customer_id) s
ORDER BY s.order_date ASC, s.amount DESC;

SELECT s.order_date, s.amount, s.customer_id, s.ROW__POSITION
FROM clustered_linear_sales TABLESAMPLE (BUCKET 2 OUT OF 3 ON customer_id) s
ORDER BY s.ROW__POSITION;

SELECT s.order_date, s.amount, s.customer_id
FROM clustered_linear_sales TABLESAMPLE (BUCKET 2 OUT OF 3 ON customer_id) s
ORDER BY s.order_date ASC, s.amount DESC;

SELECT s.order_date, s.amount, s.customer_id, s.ROW__POSITION
FROM clustered_linear_sales TABLESAMPLE (BUCKET 3 OUT OF 3 ON customer_id) s
ORDER BY s.ROW__POSITION;

SELECT s.order_date, s.amount, s.customer_id
FROM clustered_linear_sales TABLESAMPLE (BUCKET 3 OUT OF 3 ON customer_id) s
ORDER BY s.order_date ASC, s.amount DESC;

DROP TABLE clustered_linear_sales;
