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
set hive.llap.io.enabled=true;
set hive.vectorized.execution.enabled=true;
set hive.optimize.shared.work.merge.ts.schema=true;

-- Validates z-order on CREATE via clause.
CREATE TABLE default.zorder_it_nulls (
    id int,
    text string)
WRITE LOCALLY ZORDER by (id, text)
STORED BY iceberg
STORED As orc;

DESCRIBE FORMATTED default.zorder_it_nulls;
EXPLAIN INSERT INTO default.zorder_it_nulls VALUES (3, "3"),(2, "2"),(4, "4"),(5, "5"),(1, "1"),(2, "3"),(3,null),(2,null),(null,"a");
INSERT INTO default.zorder_it_nulls VALUES
    (3, "3"),
    (2, "2"),
    (4, "4"),
    (5, "5"),
    (1, "1"),
    (2, "3"),
    (3,null),
    (2,null),
    (null,"a");

SELECT * FROM default.zorder_it_nulls;
DROP TABLE default.zorder_it_nulls;


CREATE TABLE default.zorder_dit (
    id int,
    text string,
    bool_val boolean,
    date_val date)
WRITE LOCALLY ZORDER by (date_val, id, text)
STORED BY iceberg
STORED As orc;

DESCRIBE FORMATTED default.zorder_dit;
INSERT INTO default.zorder_dit VALUES
  (0,    'a',  false, DATE '2023-01-01'),
  (255,  'z',  true,  DATE '2025-12-31'),
  (0,    'z',  false, DATE '2025-12-31'),
  (255,  'a',  true,  DATE '2023-01-01'),
  (128,  'm',  true,  DATE '2024-06-01'),
  (64,   'c',  false, DATE '2023-06-01'),
  (192,  'x',  true,  DATE '2025-01-01'),
  (32,   'b',  true,  DATE '2023-03-01'),
  (96,   'd',  false, DATE '2023-09-01'),
  (160,  'v',  true,  DATE '2024-09-01');

SELECT * FROM default.zorder_dit;
DROP TABLE default.zorder_dit;

CREATE TABLE default.zorder_tsdl (
    ts timestamp,
    dd double,
    ll bigint)
WRITE LOCALLY ZORDER by (ts, dd, ll)
STORED BY iceberg
STORED As orc;

DESCRIBE FORMATTED default.zorder_tsdl;
INSERT INTO default.zorder_tsdl VALUES
  (TIMESTAMP '2022-01-01 00:00:00',  0.0,       0),
  (TIMESTAMP '2030-12-31 23:59:59', 9999.99,   9999999),
  (TIMESTAMP '2022-01-01 00:00:00', 9999.99,   9999999),
  (TIMESTAMP '2030-12-31 23:59:59', 0.0,       0),
  (TIMESTAMP '2026-06-15 12:00:00', 5000.5,    5000000),
  (TIMESTAMP '2023-03-03 03:03:03', 9999.99,   0),
  (TIMESTAMP '2023-03-03 03:03:03', 0.0,       9999999),
  (TIMESTAMP '2025-05-05 05:05:05', 250.25,    54321),
  (TIMESTAMP '2027-07-07 07:07:07', 8000.8,    8888888),
  (TIMESTAMP '2024-04-04 04:04:04', 1000.1,    123456);

SELECT * FROM default.zorder_tsdl;
DROP TABLE default.zorder_tsdl;

-- Validates z-order on CREATE via clause and via TBLPROPERTIES.
CREATE TABLE default.zorder_props(id int, text string)
STORED BY iceberg
STORED As orc
TBLPROPERTIES ("sort.order" = "zorder", "sort.columns" = "id,text");

INSERT INTO default.zorder_props VALUES (3, 'B'),(1, 'A'),(7, 'C'),(2, 'A'),(9, 'B'),(6, 'C'),(4, 'A'),
    (10, 'C'),(5, NULL),(8, 'B'),(NULL, 'A'),(12, 'C'),(11, 'A'),(13, NULL),
    (14, 'B'),(15, 'C'),(16, 'A'),(19, 'B'),(17, 'C'),(18, 'A');

DESCRIBE FORMATTED default.zorder_props;
SELECT * FROM default.zorder_props;
DROP TABLE default.zorder_props;
