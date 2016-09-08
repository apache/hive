-- SORT_QUERY_RESULTS;

-- we're setting this so that TestNegaiveCliDriver.vm doesn't stop processing after DROP TABLE fails;

set hive.cli.errors.ignore=true;

DROP TABLE IF EXISTS encrypted_table PURGE;
DROP TABLE IF EXISTS encrypted_ext_table PURGE;

CREATE TABLE encrypted_table (key INT, value STRING) LOCATION '${hiveconf:hive.metastore.warehouse.dir}/default/encrypted_table';
CRYPTO CREATE_KEY --keyName key_128 --bitLength 128;
CRYPTO CREATE_ZONE --keyName key_128 --path ${hiveconf:hive.metastore.warehouse.dir}/default/encrypted_table;

INSERT OVERWRITE TABLE encrypted_table SELECT * FROM src;

CREATE EXTERNAL TABLE encrypted_ext_table (key INT, value STRING) LOCATION '${hiveconf:hive.metastore.warehouse.dir}/default/encrypted_table';
SHOW TABLES;

DROP TABLE default.encrypted_ext_table;
SHOW TABLES;

DROP TABLE default.encrypted_table;
SHOW TABLES;

DROP TABLE default.encrypted_table PURGE;
SHOW TABLES;

DROP TABLE IF EXISTS encrypted_table1 PURGE;
CREATE TABLE encrypted_table1 (key INT, value STRING) LOCATION '${hiveconf:hive.metastore.warehouse.dir}/default/encrypted_table1';
CRYPTO CREATE_ZONE --keyName key_128 --path ${hiveconf:hive.metastore.warehouse.dir}/default/encrypted_table1;
INSERT OVERWRITE TABLE encrypted_table1 SELECT * FROM src;

SELECT COUNT(*) FROM encrypted_table1;
TRUNCATE TABLE encrypted_table1;
SELECT COUNT(*) FROM encrypted_table1;

INSERT OVERWRITE TABLE encrypted_table1 SELECT * FROM src;
DROP TABLE default.encrypted_table1;
SHOW TABLES;

TRUNCATE TABLE encrypted_table1;
DROP TABLE default.encrypted_table1;
SHOW TABLES;

DROP TABLE default.encrypted_table1 PURGE;
SHOW TABLES;

CRYPTO DELETE_KEY --keyName key_128;
