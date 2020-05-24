--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;


-- SORT_QUERY_RESULTS

-- init
drop table IF EXISTS encryptedTable_n0 PURGE;
drop table IF EXISTS unencryptedTable_n0 PURGE;

create table encryptedTable_n0(value string)
    partitioned by (key string) clustered by (value) into 2 buckets stored as orc
    LOCATION '${hiveconf:hive.metastore.warehouse.dir}/encryptedTable' TBLPROPERTIES ('transactional'='true');
CRYPTO CREATE_KEY --keyName key_1 --bitLength 128;
CRYPTO CREATE_ZONE --keyName key_1 --path ${hiveconf:hive.metastore.warehouse.dir}/encryptedTable;

create table unencryptedTable_n0(value string)
    partitioned by (key string) clustered by (value) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');

-- insert encrypted table from values
insert into table encryptedTable_n0 partition (key) values
    ('val_501', '501'),
    ('val_502', '502');

select * from encryptedTable_n0 order by key;

-- insert encrypted table from unencrypted source
from src
insert into table encryptedTable_n0 partition (key)
    select value, key limit 2;

select * from encryptedTable_n0 order by key;

-- insert unencrypted table from encrypted source
from encryptedTable_n0
insert into table unencryptedTable_n0 partition (key)
    select value, key;

select * from unencryptedTable_n0 order by key;

-- clean up
drop table encryptedTable_n0 PURGE;
CRYPTO DELETE_KEY --keyName key_1;
drop table unencryptedTable_n0 PURGE;
