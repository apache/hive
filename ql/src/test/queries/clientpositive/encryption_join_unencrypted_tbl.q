--! qt:dataset:src
--SORT_QUERY_RESULTS

DROP TABLE IF EXISTS encrypted_table PURGE;
CREATE TABLE encrypted_table (key INT, value STRING) LOCATION '${hiveconf:hive.metastore.warehouse.dir}/default/encrypted_table';
CRYPTO CREATE_KEY --keyName key_128 --bitLength 128;
CRYPTO CREATE_ZONE --keyName key_128 --path ${hiveconf:hive.metastore.warehouse.dir}/default/encrypted_table;
set hive.mapred.mode=nonstrict;
INSERT OVERWRITE TABLE encrypted_table SELECT * FROM src;

SELECT * FROM encrypted_table;

EXPLAIN EXTENDED SELECT * FROM src t1 JOIN encrypted_table t2 WHERE t1.key = t2.key;

drop table encrypted_table PURGE;
CRYPTO DELETE_KEY --keyName key_128;
