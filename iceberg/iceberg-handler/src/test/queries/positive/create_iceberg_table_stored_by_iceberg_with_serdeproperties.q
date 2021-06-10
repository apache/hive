set hive.vectorized.execution.enabled=false;
CREATE EXTERNAL TABLE ice_t (i int, s string, ts timestamp, d date) STORED BY ICEBERG WITH SERDEPROPERTIES('write.format.default'='orc');
DESCRIBE FORMATTED ice_t;
DROP TABLE ice_t;