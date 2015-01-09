-- SORT_QUERY_RESULTS

DROP TABLE IF EXISTS encrypted_table;
CREATE TABLE encrypted_table (key INT, value STRING) LOCATION '${hiveconf:hive.metastore.warehouse.dir}/default/encrypted_table';

CRYPTO CREATE_KEY --keyName key_128 --bitLength 128;
CRYPTO CREATE_ZONE --keyName key_128 --path ${hiveconf:hive.metastore.warehouse.dir}/default/encrypted_table;

LOAD DATA LOCAL INPATH '../../data/files/kv1.txt' INTO TABLE encrypted_table;

dfs -chmod -R 555 ${hiveconf:hive.metastore.warehouse.dir}/default/encrypted_table;

SELECT count(*) FROM encrypted_table;

drop table encrypted_table;
CRYPTO DELETE_KEY --keyName key_128;
