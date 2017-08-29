-- SORT_QUERY_RESULTS;

set hive.cli.errors.ignore=true;

DROP TABLE IF EXISTS encrypted_table;
DROP DATABASE IF EXISTS encrypted_db;

-- create database encrypted_db in its default warehouse location {hiveconf:hive.metastore.warehouse.dir}/encrypted_db.db
CREATE DATABASE encrypted_db LOCATION '${hiveconf:hive.metastore.warehouse.dir}/encrypted_db.db';
CRYPTO CREATE_KEY --keyName key_128 --bitLength 128;
CRYPTO CREATE_ZONE --keyName key_128 --path ${hiveconf:hive.metastore.warehouse.dir}/encrypted_db.db;

CREATE TABLE encrypted_db.encrypted_table (key INT, value STRING) LOCATION '${hiveconf:hive.metastore.warehouse.dir}/encrypted_db.db/encrypted_table';;

INSERT OVERWRITE TABLE encrypted_db.encrypted_table SELECT * FROM src;

DROP TABLE encrypted_db.encrypted_table;

DROP DATABASE encrypted_db;
CRYPTO DELETE_KEY --keyName key_128;
