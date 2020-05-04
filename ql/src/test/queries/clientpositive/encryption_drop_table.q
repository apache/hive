--! qt:dataset:src
-- SORT_QUERY_RESULTS;

-- we're setting this so that TestNegaiveCliDriver.vm doesn't stop processing after DROP TABLE fails;

set hive.cli.errors.ignore=true;

DROP TABLE IF EXISTS encrypted_table_n2;
DROP TABLE IF EXISTS encrypted_ext_table;

CREATE TABLE encrypted_table_n2 (key INT, value STRING) LOCATION '${hiveconf:hive.metastore.warehouse.dir}/default/encrypted_table';
CRYPTO CREATE_KEY --keyName key_128 --bitLength 128;
CRYPTO CREATE_ZONE --keyName key_128 --path ${hiveconf:hive.metastore.warehouse.dir}/default/encrypted_table;

INSERT OVERWRITE TABLE encrypted_table_n2 SELECT * FROM src;

CREATE EXTERNAL TABLE encrypted_ext_table (key INT, value STRING) LOCATION '${hiveconf:hive.metastore.warehouse.dir}/default/encrypted_table';
SHOW TABLES LIKE "encrypted_%";

DROP TABLE default.encrypted_ext_table;
SHOW TABLES LIKE "encrypted_%";

DROP TABLE default.encrypted_table_n2;
SHOW TABLES LIKE "encrypted_%";

DROP TABLE IF EXISTS encrypted_table1;
CREATE TABLE encrypted_table1 (key INT, value STRING) LOCATION '${hiveconf:hive.metastore.warehouse.dir}/default/encrypted_table1';
CRYPTO CREATE_ZONE --keyName key_128 --path ${hiveconf:hive.metastore.warehouse.dir}/default/encrypted_table1;
INSERT OVERWRITE TABLE encrypted_table1 SELECT * FROM src;

SELECT COUNT(*) FROM encrypted_table1;
TRUNCATE TABLE encrypted_table1;
SELECT COUNT(*) FROM encrypted_table1;

INSERT OVERWRITE TABLE encrypted_table1 SELECT * FROM src;
DROP TABLE default.encrypted_table1;
SHOW TABLES LIKE "encrypted_%";

TRUNCATE TABLE encrypted_table1;
DROP TABLE default.encrypted_table1;
SHOW TABLES LIKE "encrypted_%";

CRYPTO DELETE_KEY --keyName key_128;
