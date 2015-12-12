set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.mapred.mode=nonstrict;

-- SORT_QUERY_RESULTS

-- init
drop table IF EXISTS encryptedTable PURGE;
drop table IF EXISTS unencryptedTable PURGE;

create table encryptedTable(key string,
    value string) partitioned by (ds string) clustered by (key) into 2 buckets stored as orc
    LOCATION '${hiveconf:hive.metastore.warehouse.dir}/encryptedTable' TBLPROPERTIES ('transactional'='true');
CRYPTO CREATE_KEY --keyName key_1 --bitLength 128;
CRYPTO CREATE_ZONE --keyName key_1 --path ${hiveconf:hive.metastore.warehouse.dir}/encryptedTable;

create table unencryptedTable(key string,
    value string) partitioned by (ds string) clustered by (key) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');

-- insert encrypted table from values
insert into table encryptedTable partition
    (ds='today') values
    ('501', 'val_501'),
    ('502', 'val_502');

select * from encryptedTable order by key;

-- insert encrypted table from unencrypted source
insert into table encryptedTable partition (ds='yesterday')
select * from src where key in ('238', '86');

select * from encryptedTable order by key;

-- insert unencrypted table from encrypted source
insert into table unencryptedTable partition (ds='today')
select key, value from encryptedTable where ds='today';

insert into table unencryptedTable partition (ds='yesterday')
select key, value from encryptedTable where ds='yesterday';

select * from unencryptedTable order by key;

-- clean up
drop table encryptedTable PURGE;
CRYPTO DELETE_KEY --keyName key_1;
drop table unencryptedTable PURGE;
