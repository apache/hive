set hive.vectorized.execution.enabled=false;
DROP TABLE IF EXISTS ice_orc;
CREATE EXTERNAL TABLE ice_orc (i int, s string, ts timestamp, d date) STORED BY ICEBERG STORED AS ORC TBLPROPERTIES('write.format.default'='orc');