-- SORT_QUERY_RESULTS

DROP TABLE IF EXISTS encrypted_table_n4 PURGE;
CREATE TABLE encrypted_table_n4 (key INT, value STRING) LOCATION '${hiveconf:hive.metastore.warehouse.dir}/default/encrypted_table';

CRYPTO CREATE_KEY --keyName key_128 --bitLength 128;
CRYPTO CREATE_ZONE --keyName key_128 --path ${hiveconf:hive.metastore.warehouse.dir}/default/encrypted_table;

LOAD DATA LOCAL INPATH '../../data/files/kv1.txt' INTO TABLE encrypted_table_n4;

dfs -chmod -R 555 ${hiveconf:hive.metastore.warehouse.dir}/default/encrypted_table;

SELECT count(*) FROM encrypted_table_n4;

drop table encrypted_table_n4 PURGE;
CRYPTO DELETE_KEY --keyName key_128;
