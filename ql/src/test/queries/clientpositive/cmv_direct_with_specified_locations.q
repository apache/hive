-- SORT_QUERY_RESULTS
--! qt:dataset:alltypesorc

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.acid.direct.insert.enabled=true;

DROP TABLE IF EXISTS source;

DROP MATERIALIZED VIEW IF EXISTS test_orc_cmv;

DROP MATERIALIZED VIEW IF EXISTS test_orc_mmcmv;

DROP MATERIALIZED VIEW IF EXISTS test_parquet_mmcmv;

DROP MATERIALIZED VIEW IF EXISTS test_avro_mmcmv;

DROP MATERIALIZED VIEW IF EXISTS test_textfile_mmcmv;

CREATE TABLE source STORED AS ORC TBLPROPERTIES('transactional'='true') AS (SELECT cint, cfloat, cdouble, cstring1, ctimestamp1 FROM alltypesorc);

CREATE MATERIALIZED VIEW test_orc_cmv STORED AS ORC LOCATION '/build/ql/test/data/warehouse/test_cmv_orc' TBLPROPERTIES('transactional'='true') AS ((SELECT * FROM source WHERE cint > 200 LIMIT 10) UNION (SELECT * FROM source WHERE cint < -100 LIMIT 10));

CREATE MATERIALIZED VIEW test_orc_mmcmv STORED AS ORC LOCATION '/build/ql/test/data/warehouse/test_mmcmv_orc' TBLPROPERTIES('transactional'='true', 'transactional_properties'='insert_only') AS ((SELECT * FROM source WHERE cint > 200 LIMIT 10) UNION (SELECT * FROM source WHERE cint < -100 LIMIT 10));

CREATE MATERIALIZED VIEW test_parquet_mmcmv STORED AS PARQUET LOCATION '/build/ql/test/data/warehouse/test_mmcmv_parquet' TBLPROPERTIES('transactional'='true', 'transactional_properties'='insert_only') AS ((SELECT * FROM source WHERE cint > 200 LIMIT 10) UNION (SELECT * FROM source WHERE cint < -100 LIMIT 10));

CREATE MATERIALIZED VIEW test_avro_mmcmv STORED AS AVRO LOCATION '/build/ql/test/data/warehouse/test_mmcmv_avro' TBLPROPERTIES('transactional'='true', 'transactional_properties'='insert_only') AS ((SELECT * FROM source WHERE cint > 200 LIMIT 10) UNION (SELECT * FROM source WHERE cint < -100 LIMIT 10));

CREATE MATERIALIZED VIEW test_textfile_mmcmv STORED AS TEXTFILE LOCATION '/build/ql/test/data/warehouse/test_mmcmv_textfile' TBLPROPERTIES('transactional'='true', 'transactional_properties'='insert_only') AS ((SELECT * FROM source WHERE cint > 200 LIMIT 10) UNION (SELECT * FROM source WHERE cint < -100 LIMIT 10));

SELECT * FROM test_orc_cmv ORDER BY cint;

SELECT * FROM test_orc_mmcmv ORDER BY cint;

SELECT * FROM test_parquet_mmcmv ORDER BY cint;

SELECT * FROM test_avro_mmcmv ORDER BY cint;

SELECT * FROM test_textfile_mmcmv ORDER BY cint;

DROP MATERIALIZED VIEW IF EXISTS test_orc_cmv;

DROP MATERIALIZED VIEW IF EXISTS test_orc_mmcmv;

DROP MATERIALIZED VIEW IF EXISTS test_parquet_mmcmv;

DROP MATERIALIZED VIEW IF EXISTS test_avro_mmcmv;

DROP MATERIALIZED VIEW IF EXISTS test_textfile_mmcmv;

DROP TABLE IF EXISTS source;