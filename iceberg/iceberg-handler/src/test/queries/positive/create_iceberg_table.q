set hive.vectorized.execution.enabled=false;
CREATE EXTERNAL TABLE ice_t (i int, s string, ts timestamp, d date) STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler';
DESCRIBE FORMATTED ice_t;
DROP TABLE ice_t;
