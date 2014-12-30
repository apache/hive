set hive.exec.dynamic.partition.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.enforce.bucketing=true;

-- SORT_QUERY_RESULTS

-- init
drop table IF EXISTS encryptedWith128BitsKeyDB.encryptedTable;
drop table IF EXISTS unencryptedTable;

create table encryptedWith128BitsKeyDB.encryptedTable(value string)
    partitioned by (key string) clustered by (value) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');

create table unencryptedTable(value string)
    partitioned by (key string) clustered by (value) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');

-- insert encrypted table from values
explain extended insert into table encryptedWith128BitsKeyDB.encryptedTable partition (key) values
    ('val_501', '501'),
    ('val_502', '502');

insert into table encryptedWith128BitsKeyDB.encryptedTable partition (key) values
    ('val_501', '501'),
    ('val_502', '502');

select * from encryptedWith128BitsKeyDB.encryptedTable order by key;

-- insert encrypted table from unencrypted source
explain extended from src
insert into table encryptedWith128BitsKeyDB.encryptedTable partition (key)
    select value, key limit 2;

from src
insert into table encryptedWith128BitsKeyDB.encryptedTable partition (key)
    select value, key limit 2;

select * from encryptedWith128BitsKeyDB.encryptedTable order by key;

-- insert unencrypted table from encrypted source
explain extended from encryptedWith128BitsKeyDB.encryptedTable
insert into table unencryptedTable partition (key)
    select value, key;

from encryptedWith128BitsKeyDB.encryptedTable
insert into table unencryptedTable partition (key)
    select value, key;

select * from unencryptedTable order by key;

-- clean up
drop table encryptedWith128BitsKeyDB.encryptedTable;
drop table unencryptedTable;