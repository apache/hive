set hive.exec.dynamic.partition.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.enforce.bucketing=true;

-- SORT_QUERY_RESULTS

-- init
drop table IF EXISTS encryptedTable PURGE;
drop table IF EXISTS unencryptedTable PURGE;

create table encryptedTable(value string)
    partitioned by (key string) clustered by (value) into 2 buckets stored as orc
    LOCATION '${hiveconf:hive.metastore.warehouse.dir}/encryptedTable' TBLPROPERTIES ('transactional'='true');
CRYPTO CREATE_KEY --keyName key_1 --bitLength 128;
CRYPTO CREATE_ZONE --keyName key_1 --path ${hiveconf:hive.metastore.warehouse.dir}/encryptedTable;

create table unencryptedTable(value string)
    partitioned by (key string) clustered by (value) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');

-- insert encrypted table from values
explain extended insert into table encryptedTable partition (key) values
    ('val_501', '501'),
    ('val_502', '502');

insert into table encryptedTable partition (key) values
    ('val_501', '501'),
    ('val_502', '502');

select * from encryptedTable order by key;

-- insert encrypted table from unencrypted source
explain extended from src
insert into table encryptedTable partition (key)
    select value, key limit 2;

from src
insert into table encryptedTable partition (key)
    select value, key limit 2;

select * from encryptedTable order by key;

-- insert unencrypted table from encrypted source
explain extended from encryptedTable
insert into table unencryptedTable partition (key)
    select value, key;

from encryptedTable
insert into table unencryptedTable partition (key)
    select value, key;

select * from unencryptedTable order by key;

-- clean up
drop table encryptedTable PURGE;
CRYPTO DELETE_KEY --keyName key_1;
drop table unencryptedTable PURGE;