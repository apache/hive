-- SORT_QUERY_RESULTS;

-- we're setting this so that TestNegaiveCliDriver.vm doesn't stop processing after ALTER TABLE fails;

set hive.cli.errors.ignore=true;

DROP TABLE IF EXISTS encrypted_table PURGE;
CREATE TABLE encrypted_table (key INT, value STRING) LOCATION '${hiveconf:hive.metastore.warehouse.dir}/default/encrypted_table';
CRYPTO CREATE_KEY --keyName key_128 --bitLength 128;
CRYPTO CREATE_ZONE --keyName key_128 --path ${hiveconf:hive.metastore.warehouse.dir}/default/encrypted_table;

INSERT OVERWRITE TABLE encrypted_table SELECT * FROM src;
SHOW TABLES;
ALTER TABLE default.encrypted_table RENAME TO default.plain_table;
SHOW TABLES;

DROP TABLE encrypted_table PURGE;

CRYPTO DELETE_KEY --keyName key_128;

