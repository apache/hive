CREATE TABLE ice_t (
  id INT,
  name STRING DEFAULT 'unknown',
  age INT DEFAULT 25
)
STORED BY ICEBERG
TBLPROPERTIES ('format-version'='3');

ALTER TABLE ice_t REPLACE COLUMNS (
  id INT,
  name STRING DEFAULT 'unknown1',
  age INT DEFAULT 25);