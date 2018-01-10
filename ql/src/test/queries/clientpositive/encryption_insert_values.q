-- SORT_QUERY_RESULTS;
set hive.stats.column.autogather=false;

DROP TABLE IF EXISTS encrypted_table PURGE;
CREATE TABLE encrypted_table (key INT, value STRING) LOCATION '${hiveconf:hive.metastore.warehouse.dir}/default/encrypted_table';
CRYPTO CREATE_KEY --keyName key_128 --bitLength 128;
CRYPTO CREATE_ZONE --keyName key_128 --path ${hiveconf:hive.metastore.warehouse.dir}/default/encrypted_table;

INSERT INTO encrypted_table values(1,'foo'),(2,'bar');

select * from encrypted_table;

CRYPTO DELETE_KEY --keyName key_128;
