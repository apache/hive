-- SORT_QUERY_RESULTS;

-- we're setting this so that TestNegativeCliDriver.vm doesn't stop processing after DROP TABLE fails;

set hive.cli.errors.ignore=true;
set hive.mapred.mode=nonstrict;
DROP TABLE IF EXISTS encrypted_table_dp;
CREATE TABLE encrypted_table_dp (key INT, value STRING) partitioned by (p STRING) LOCATION '${hiveconf:hive.metastore.warehouse.dir}/default/encrypted_table_dp';
CRYPTO CREATE_KEY --keyName key_128 --bitLength 128;
CRYPTO CREATE_ZONE --keyName key_128 --path ${hiveconf:hive.metastore.warehouse.dir}/default/encrypted_table_dp;

INSERT INTO encrypted_table_dp PARTITION(p)(p,key,value) values('2014-09-23', 1, 'foo'),('2014-09-24', 2, 'bar');
SELECT * FROM encrypted_table_dp;

CREATE EXTERNAL TABLE encrypted_ext_table_dp (key INT, value STRING) partitioned by (p STRING) LOCATION '${hiveconf:hive.metastore.warehouse.dir}/default/encrypted_table_dp';
ALTER TABLE encrypted_ext_table_dp ADD PARTITION (p='2014-09-23') LOCATION '${hiveconf:hive.metastore.warehouse.dir}/default/encrypted_table_dp/p=2014-09-23';
SELECT * FROM encrypted_ext_table_dp;
ALTER TABLE encrypted_ext_table_dp DROP PARTITION (p='2014-09-23');
SELECT * FROM encrypted_ext_table_dp;
DROP TABLE encrypted_ext_table_dp;

SELECT * FROM encrypted_table_dp;
ALTER TABLE encrypted_table_dp DROP PARTITION (p='2014-09-23');
SELECT * FROM encrypted_table_dp;

TRUNCATE TABLE encrypted_table_dp PARTITION (p='2014-09-24');
SHOW PARTITIONS encrypted_table_dp;
SELECT * FROM encrypted_table_dp;

ALTER TABLE encrypted_table_dp DROP PARTITION (p='2014-09-24');
DROP TABLE encrypted_table_dp;
