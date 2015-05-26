DROP TABLE IF EXISTS dve_encrypted_table PURGE;
CREATE TABLE dve_encrypted_table (key INT, value STRING) LOCATION '${hiveconf:hive.metastore.warehouse.dir}/default/dve_encrypted_table';
CRYPTO CREATE_KEY --keyName key_128 --bitLength 128;
CRYPTO CREATE_ZONE --keyName key_128 --path ${hiveconf:hive.metastore.warehouse.dir}/default/dve_encrypted_table;
CREATE VIEW dve_view AS SELECT * FROM dve_encrypted_table;
DROP VIEW dve_view;
