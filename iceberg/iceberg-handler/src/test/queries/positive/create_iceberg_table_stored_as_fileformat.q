-- Mask random uuid
--! qt:replace:/(\s+uuid\s+)\S+(\s*)/$1#Masked#$2/

set hive.vectorized.execution.enabled=false;
DROP TABLE IF EXISTS ice_orc;
CREATE EXTERNAL TABLE ice_orc (i int, s string, ts timestamp, d date) STORED BY ICEBERG STORED AS ORC;
DESCRIBE FORMATTED ice_orc;
DROP TABLE ice_orc;

DROP TABLE IF EXISTS ice_parquet;
CREATE EXTERNAL TABLE ice_parquet (i int, s string, ts timestamp, d date) STORED BY ICEBERG STORED AS PARQUET;
DESCRIBE FORMATTED ice_parquet;
DROP TABLE ice_parquet;

DROP TABLE IF EXISTS ice_avro;
CREATE EXTERNAL TABLE ice_avro (i int, s string, ts timestamp, d date) STORED BY ICEBERG STORED AS AVRO;
DESCRIBE FORMATTED ice_avro;
DROP TABLE ice_avro;

DROP TABLE IF EXISTS ice_t;
CREATE EXTERNAL TABLE ice_t (i int, s string, ts timestamp, d date) STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler' STORED AS AVRO;
DESCRIBE FORMATTED ice_t;
DROP TABLE ice_t;

CREATE EXTERNAL TABLE ice_t (i int, s string, ts timestamp, d date) STORED BY ICEBERG WITH SERDEPROPERTIES('dummy'='dummy_value') STORED AS ORC;
DESCRIBE FORMATTED ice_t;
DROP TABLE ice_t;