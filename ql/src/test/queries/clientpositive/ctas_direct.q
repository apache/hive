-- SORT_QUERY_RESULTS
--! qt:dataset:alltypesorc

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.acid.direct.insert.enabled=true;
set hive.exec.max.dynamic.partitions=200;
set hive.exec.max.dynamic.partitions.pernode=200;

DROP TABLE IF EXISTS source;

DROP TABLE IF EXISTS test_orc_ctas;

DROP TABLE IF EXISTS test_orc_mmctas;

DROP TABLE IF EXISTS test_parquet_mmctas;

DROP TABLE IF EXISTS test_avro_mmctas;

DROP TABLE IF EXISTS test_textfile_mmctas;

DROP TABLE IF EXISTS test_partition_orc_ctas;

DROP TABLE IF EXISTS test_partition_orc_mmctas;

DROP TABLE IF EXISTS test_partition_parquet_mmctas;

DROP TABLE IF EXISTS test_partition_avro_mmctas;

DROP TABLE IF EXISTS test_partition_textfile_mmctas;

CREATE TABLE IF NOT EXISTS source STORED AS ORC TBLPROPERTIES('transactional'='true') AS (SELECT cint, cfloat, cdouble, cstring1, ctimestamp1 FROM alltypesorc);

CREATE TABLE IF NOT EXISTS test_orc_ctas STORED AS ORC TBLPROPERTIES('transactional'='true') AS ((SELECT * FROM alltypesorc WHERE cint > 200 LIMIT 10) UNION (SELECT * FROM alltypesorc WHERE cint < -100 LIMIT 10));

CREATE TABLE IF NOT EXISTS test_orc_mmctas STORED AS ORC TBLPROPERTIES('transactional'='true', 'transactional_properties'='insert_only') AS ((SELECT * FROM alltypesorc WHERE cint > 200 LIMIT 10) UNION (SELECT * FROM alltypesorc WHERE cint < -100 LIMIT 10));

CREATE TABLE IF NOT EXISTS test_parquet_mmctas STORED AS PARQUET TBLPROPERTIES('transactional'='true', 'transactional_properties'='insert_only') AS ((SELECT * FROM alltypesorc WHERE cint > 200 LIMIT 10) UNION (SELECT * FROM alltypesorc WHERE cint < -100 LIMIT 10));

CREATE TABLE IF NOT EXISTS test_avro_mmctas STORED AS AVRO TBLPROPERTIES('transactional'='true', 'transactional_properties'='insert_only') AS ((SELECT * FROM source WHERE cint > 200 LIMIT 10) UNION (SELECT * FROM source WHERE cint < -100 LIMIT 10));

CREATE TABLE IF NOT EXISTS test_textfile_mmctas STORED AS TEXTFILE TBLPROPERTIES('transactional'='true', 'transactional_properties'='insert_only') AS ((SELECT * FROM alltypesorc WHERE cint > 200 LIMIT 10) UNION (SELECT * FROM alltypesorc WHERE cint < -100 LIMIT 10));

CREATE TABLE IF NOT EXISTS test_partition_orc_ctas PARTITIONED BY (cint) STORED AS ORC TBLPROPERTIES('transactional'='true') AS ((SELECT * FROM alltypesorc WHERE cint > 200 LIMIT 10) UNION (SELECT * FROM alltypesorc WHERE cint < -100 LIMIT 10));

CREATE TABLE IF NOT EXISTS test_partition_orc_mmctas PARTITIONED BY (cint) STORED AS ORC TBLPROPERTIES('transactional'='true', 'transactional_properties'='insert_only') AS ((SELECT * FROM alltypesorc WHERE cint > 200 LIMIT 10) UNION (SELECT * FROM alltypesorc WHERE cint < -100 LIMIT 10));

CREATE TABLE IF NOT EXISTS test_partition_parquet_mmctas PARTITIONED BY (cint) STORED AS PARQUET TBLPROPERTIES('transactional'='true', 'transactional_properties'='insert_only') AS ((SELECT * FROM alltypesorc WHERE cint > 200 LIMIT 10) UNION (SELECT * FROM alltypesorc WHERE cint < -100 LIMIT 10));

CREATE TABLE IF NOT EXISTS test_partition_avro_mmctas PARTITIONED BY (cint) STORED AS AVRO TBLPROPERTIES('transactional'='true', 'transactional_properties'='insert_only') AS ((SELECT * FROM source WHERE cint > 200 LIMIT 10) UNION (SELECT * FROM source WHERE cint < -100 LIMIT 10));

CREATE TABLE IF NOT EXISTS test_partition_textfile_mmctas PARTITIONED BY (cint) STORED AS TEXTFILE TBLPROPERTIES('transactional'='true', 'transactional_properties'='insert_only') AS ((SELECT * FROM alltypesorc WHERE cint > 200 LIMIT 10) UNION (SELECT * FROM alltypesorc WHERE cint < -100 LIMIT 10));

SELECT * FROM test_orc_ctas ORDER BY cint;

SELECT * FROM test_orc_mmctas ORDER BY cint;

SELECT * FROM test_parquet_mmctas ORDER BY cint;

SELECT * FROM test_avro_mmctas ORDER BY cint;

SELECT * FROM test_textfile_mmctas ORDER BY cint;

SELECT * FROM test_partition_orc_ctas ORDER BY cint;

SELECT * FROM test_partition_orc_mmctas ORDER BY cint;

SELECT * FROM test_partition_parquet_mmctas ORDER BY cint;

SELECT * FROM test_partition_avro_mmctas ORDER BY cint;

SELECT * FROM test_partition_textfile_mmctas ORDER BY cint;

DROP TABLE IF EXISTS source;

DROP TABLE IF EXISTS test_orc_ctas;

DROP TABLE IF EXISTS test_orc_mmctas;

DROP TABLE IF EXISTS test_parquet_mmctas;

DROP TABLE IF EXISTS test_avro_mmctas;

DROP TABLE IF EXISTS test_textfile_mmctas;

DROP TABLE IF EXISTS test_partition_orc_ctas;

DROP TABLE IF EXISTS test_partition_orc_mmctas;

DROP TABLE IF EXISTS test_partition_parquet_mmctas;

DROP TABLE IF EXISTS test_partition_avro_mmctas;

DROP TABLE IF EXISTS test_partition_textfile_mmctas;