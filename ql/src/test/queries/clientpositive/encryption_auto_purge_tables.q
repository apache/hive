-- SORT_QUERY_RESULTS;

-- we're setting this so that TestNegaiveCliDriver.vm doesn't stop processing after DROP TABLE fails;

DROP TABLE IF EXISTS encrypted_table PURGE;
DROP TABLE IF EXISTS encrypted_ext_table PURGE;

CREATE TABLE encrypted_table (key INT, value STRING) LOCATION '${hiveconf:hive.metastore.warehouse.dir}/default/encrypted_table';
CRYPTO CREATE_KEY --keyName key_128 --bitLength 128;
CRYPTO CREATE_ZONE --keyName key_128 --path ${hiveconf:hive.metastore.warehouse.dir}/default/encrypted_table;

SHOW TABLES;

ALTER TABLE encrypted_table SET TBLPROPERTIES("auto.purge"="true");

INSERT OVERWRITE TABLE encrypted_table SELECT * FROM src;
SELECT COUNT(*) from encrypted_table;

TRUNCATE TABLE encrypted_table;
SELECT COUNT(*) FROM encrypted_table;

INSERT OVERWRITE TABLE encrypted_table SELECT * FROM src;
SELECT COUNT(*) FROM encrypted_table;

CREATE EXTERNAL TABLE encrypted_ext_table (key INT, value STRING) LOCATION '${hiveconf:hive.metastore.warehouse.dir}/default/encrypted_table';
ALTER TABLE encrypted_ext_table SET TBLPROPERTIES("auto.purge"="true");

INSERT OVERWRITE TABLE encrypted_ext_table SELECT * FROM src;
SELECT COUNT(*) from encrypted_ext_table;

DROP TABLE encrypted_table;
DROP TABLE encrypted_ext_table;
SHOW TABLES;

-- cleanup
DROP TABLE IF EXISTS encrypted_table PURGE;
DROP TABLE IF EXISTS encrypted_ext_table PURGE;
CRYPTO DELETE_KEY --keyName key_128;
