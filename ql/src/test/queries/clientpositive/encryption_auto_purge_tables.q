--! qt:dataset:src
-- SORT_QUERY_RESULTS;

-- we're setting this so that TestNegaiveCliDriver.vm doesn't stop processing after DROP TABLE fails;

DROP TABLE IF EXISTS encrypted_table_n5 PURGE;
DROP TABLE IF EXISTS encrypted_ext_table_n0 PURGE;

CREATE TABLE encrypted_table_n5 (key INT, value STRING) LOCATION '${hiveconf:hive.metastore.warehouse.dir}/default/encrypted_table';
CRYPTO CREATE_KEY --keyName key_128 --bitLength 128;
CRYPTO CREATE_ZONE --keyName key_128 --path ${hiveconf:hive.metastore.warehouse.dir}/default/encrypted_table;

SHOW TABLES LIKE "encrypted_%";

ALTER TABLE encrypted_table_n5 SET TBLPROPERTIES("auto.purge"="true");

INSERT OVERWRITE TABLE encrypted_table_n5 SELECT * FROM src;
SELECT COUNT(*) from encrypted_table_n5;

TRUNCATE TABLE encrypted_table_n5;
SELECT COUNT(*) FROM encrypted_table_n5;

INSERT OVERWRITE TABLE encrypted_table_n5 SELECT * FROM src;
SELECT COUNT(*) FROM encrypted_table_n5;

CREATE EXTERNAL TABLE encrypted_ext_table_n0 (key INT, value STRING) LOCATION '${hiveconf:hive.metastore.warehouse.dir}/default/encrypted_table';
ALTER TABLE encrypted_ext_table_n0 SET TBLPROPERTIES("auto.purge"="true");

INSERT OVERWRITE TABLE encrypted_ext_table_n0 SELECT * FROM src;
SELECT COUNT(*) from encrypted_ext_table_n0;

DROP TABLE encrypted_table_n5;
DROP TABLE encrypted_ext_table_n0;
SHOW TABLES LIKE "encrypted_%";

-- cleanup
DROP TABLE IF EXISTS encrypted_table_n5 PURGE;
DROP TABLE IF EXISTS encrypted_ext_table_n0 PURGE;
CRYPTO DELETE_KEY --keyName key_128;
