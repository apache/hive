--! qt:dataset:src
DROP DATABASE IF EXISTS testCT CASCADE;
CREATE DATABASE testCT;
dfs ${system:test.dfs.mkdir} ${hiveconf:hive.metastore.warehouse.dir}/default/encrypted_tablectas;

CRYPTO CREATE_KEY --keyName key_128 --bitLength 128;
CRYPTO CREATE_ZONE --keyName key_128 --path ${hiveconf:hive.metastore.warehouse.dir}/default/encrypted_tablectas;

CREATE TABLE testCT.encrypted_tablectas LOCATION '${hiveconf:hive.metastore.warehouse.dir}/default/encrypted_tablectas'
AS SELECT * from src where key = 100 limit 1;

select * from testCT.encrypted_tablectas;

DROP TABLE testCT.encrypted_tablectas PURGE;
CRYPTO DELETE_KEY --keyName key_128;
DROP DATABASE testCT;
