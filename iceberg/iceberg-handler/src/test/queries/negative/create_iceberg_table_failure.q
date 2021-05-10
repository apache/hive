set hive.vectorized.execution.enabled=true;
CREATE EXTERNAL TABLE ice_t (i int, s string, ts timestamp, d date) STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler';