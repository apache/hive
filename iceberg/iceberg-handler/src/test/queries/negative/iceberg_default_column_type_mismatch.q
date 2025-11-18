CREATE TABLE t3 (
  id INT,
  age INT DEFAULT 'twenty five'
)
STORED BY ICEBERG
TBLPROPERTIES ('format-version'='3');