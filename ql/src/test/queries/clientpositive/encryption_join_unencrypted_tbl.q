--SORT_QUERY_RESULTS

DROP TABLE IF EXISTS encrypted_table;
CREATE TABLE encrypted_table (key INT, value STRING) LOCATION '/user/hive/warehouse/default/encrypted_table';
CRYPTO CREATE_KEY --keyName key_128 --bitLength 128;
CRYPTO CREATE_ZONE --keyName key_128 --path /user/hive/warehouse/default/encrypted_table;

INSERT OVERWRITE TABLE encrypted_table SELECT * FROM src;

SELECT * FROM encrypted_table;

EXPLAIN EXTENDED SELECT * FROM src t1 JOIN encrypted_table t2 WHERE t1.key = t2.key;