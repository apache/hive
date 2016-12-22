set fs.trash.interval=5;

drop table IF EXISTS encrypted_table PURGE;

CREATE TABLE encrypted_table (key INT, value STRING) LOCATION '${hiveconf:hive.metastore.warehouse.dir}/default/encrypted_table';
CRYPTO CREATE_KEY --keyName key_128 --bitLength 128;
CRYPTO CREATE_ZONE --keyName key_128 --path ${hiveconf:hive.metastore.warehouse.dir}/default/encrypted_table;

INSERT OVERWRITE TABLE encrypted_table SELECT * FROM src;
select count(*) from encrypted_table;

drop table encrypted_table;
drop table encrypted_table PURGE;
CRYPTO DELETE_KEY --keyName key_128;
set fs.trash.interval=0;
