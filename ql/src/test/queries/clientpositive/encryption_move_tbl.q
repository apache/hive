-- SORT_QUERY_RESULTS;

-- we're setting this so that TestNegaiveCliDriver.vm doesn't stop processing after ALTER TABLE fails;

set hive.cli.errors.ignore=true;

DROP TABLE IF EXISTS encrypted_table PURGE;
DROP DATABASE IF EXISTS encrypted_db;
CREATE TABLE encrypted_table (key INT, value STRING) LOCATION '${hiveconf:hive.metastore.warehouse.dir}/default/encrypted_table';
CRYPTO CREATE_KEY --keyName key_128 --bitLength 128;
CRYPTO CREATE_ZONE --keyName key_128 --path ${hiveconf:hive.metastore.warehouse.dir}/default/encrypted_table;

CREATE DATABASE encrypted_db LOCATION '${hiveconf:hive.metastore.warehouse.dir}/encrypted_db';
CRYPTO CREATE_KEY --keyName key_128_2 --bitLength 128;
CRYPTO CREATE_ZONE --keyName key_128_2 --path ${hiveconf:hive.metastore.warehouse.dir}/encrypted_db;

INSERT OVERWRITE TABLE encrypted_table SELECT * FROM src;
SHOW TABLES;
-- should fail
ALTER TABLE default.encrypted_table RENAME TO encrypted_db.encrypted_table_2;
SHOW TABLES;
-- should succeed in Hadoop 2.7 but fail in 2.6  (HDFS-7530)
ALTER TABLE default.encrypted_table RENAME TO default.plain_table;
SHOW TABLES;


DROP TABLE encrypted_table PURGE;
DROP TABLE default.plain_table PURGE;
DROP DATABASE encrypted_db;
CRYPTO DELETE_KEY --keyName key_128;
CRYPTO DELETE_KEY --keyName key_128_2;
