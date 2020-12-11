--! qt:dataset:src
-- SORT_QUERY_RESULTS;
set hive.stats.column.autogather=false;

-- we're setting this so that TestNegaiveCliDriver.vm doesn't stop processing after ALTER TABLE fails;

set hive.cli.errors.ignore=true;

DROP TABLE IF EXISTS encrypted_table_n1 PURGE;
DROP DATABASE IF EXISTS encrypted_db;

-- create table default.encrypted_table_n1 in its default warehouse location ${hiveconf:hive.metastore.warehouse.dir}/encrypted_table
CREATE TABLE encrypted_table_n1 (key INT, value STRING) LOCATION '${hiveconf:hive.metastore.warehouse.dir}/encrypted_table';
CRYPTO CREATE_KEY --keyName key_128 --bitLength 128;
CRYPTO CREATE_ZONE --keyName key_128 --path ${hiveconf:hive.metastore.warehouse.dir}/encrypted_table;

-- create database encrypted_db in its default warehouse location {hiveconf:hive.metastore.warehouse.dir}/encrypted_db.db
CREATE DATABASE encrypted_db LOCATION '${hiveconf:hive.metastore.warehouse.dir}/encrypted_db.db';
CRYPTO CREATE_KEY --keyName key_128_2 --bitLength 128;
CRYPTO CREATE_ZONE --keyName key_128_2 --path ${hiveconf:hive.metastore.warehouse.dir}/encrypted_db.db;

INSERT OVERWRITE TABLE encrypted_table_n1 SELECT * FROM src;
SHOW TABLES LIKE "encrypted_%";
ANALYZE TABLE encrypted_table_n1 COMPUTE STATISTICS FOR COLUMNS;
DESCRIBE FORMATTED encrypted_table_n1 key;
DESCRIBE FORMATTED encrypted_table_n1 value;

-- should fail, since they are in different encryption zones, but table columns statistics should not change
ALTER TABLE default.encrypted_table_n1 RENAME TO encrypted_db.encrypted_table_2;
SHOW TABLES;
DESCRIBE FORMATTED encrypted_table_n1 key;
DESCRIBE FORMATTED encrypted_table_n1 value;

-- should succeed in Hadoop 2.7 but fail in 2.6  (HDFS-7530)
ALTER TABLE default.encrypted_table_n1 RENAME TO default.plain_table;
SHOW TABLES;

-- create table encrypted_table_outloc under default database but in a specified location other than the default db location in the warehouse
-- rename should succeed since it does not need to move data (HIVE-14909), otherwise, it would fail.
CREATE TABLE encrypted_table_outloc (key INT, value STRING) LOCATION '${hiveconf:hive.metastore.warehouse.dir}/../specified_table_location';
CRYPTO CREATE_KEY --keyName key_128_3 --bitLength 128;
CRYPTO CREATE_ZONE --keyName key_128_3 --path ${hiveconf:hive.metastore.warehouse.dir}/../specified_table_location;
ALTER TABLE encrypted_table_outloc RENAME TO renamed_encrypted_table_outloc;
SHOW TABLES;

-- create database encrypted_db_outloc in a specified location other than its default in warehouse
CREATE DATABASE encrypted_db_outloc MANAGEDLOCATION '${hiveconf:hive.metastore.warehouse.dir}/../specified_db_location';
CRYPTO CREATE_KEY --keyName key_128_4 --bitLength 128;
CRYPTO CREATE_ZONE --keyName key_128_4 --path ${hiveconf:hive.metastore.warehouse.dir}/../specified_db_location;

USE encrypted_db_outloc;
CREATE TABLE encrypted_table_n1 (key INT, value STRING);
INSERT OVERWRITE TABLE encrypted_table_n1 SELECT * FROM default.src;
ALTER TABLE encrypted_table_n1 RENAME TO renamed_encrypted_table_n1;
-- should succeed since data moves within specified_db_location
SHOW TABLES;
-- should fail, since they are in different encryption zones
ALTER TABLE encrypted_db_outloc.renamed_encrypted_table_n1 RENAME TO default.plain_table_2;
SHOW TABLES;

DROP TABLE default.encrypted_table_n1 PURGE;
DROP TABLE default.plain_table PURGE;
DROP TABLE default.renamed_encrypted_table_outloc PURGE;
DROP DATABASE encrypted_db;
DROP TABLE encrypted_db_outloc.renamed_encrypted_table_n1 PURGE;
DROP DATABASE encrypted_db_outloc;
CRYPTO DELETE_KEY --keyName key_128;
CRYPTO DELETE_KEY --keyName key_128_2;
CRYPTO DELETE_KEY --keyName key_128_3;
CRYPTO DELETE_KEY --keyName key_128_4;
