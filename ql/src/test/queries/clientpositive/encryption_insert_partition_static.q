set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.enforce.bucketing=true;

-- SORT_QUERY_RESULTS

-- init
drop table IF EXISTS encryptedWith128BitsKeyDB.encryptedTableIn128BitsKey;
drop table IF EXISTS unencryptedTable;

create table encryptedWith128BitsKeyDB.encryptedTableIn128BitsKey(key string,
    value string) partitioned by (ds string) clustered by (key) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');

create table unencryptedTable(key string,
    value string) partitioned by (ds string) clustered by (key) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');

-- insert encrypted table from values
explain extended insert into table encryptedWith128BitsKeyDB.encryptedTableIn128BitsKey partition
    (ds='today') values
    ('501', 'val_501'),
    ('502', 'val_502');

insert into table encryptedWith128BitsKeyDB.encryptedTableIn128BitsKey partition
    (ds='today') values
    ('501', 'val_501'),
    ('502', 'val_502');

select * from encryptedWith128BitsKeyDB.encryptedTableIn128BitsKey order by key;

-- insert encrypted table from unencrypted source
explain extended from src
insert into table encryptedWith128BitsKeyDB.encryptedTableIn128BitsKey partition
    (ds='yesterday')
    select * limit 2;

from src
insert into table encryptedWith128BitsKeyDB.encryptedTableIn128BitsKey partition
    (ds='yesterday')
    select * limit 2;

select * from encryptedWith128BitsKeyDB.encryptedTableIn128BitsKey order by key;

-- insert unencrypted table from encrypted source
explain extended from encryptedWith128BitsKeyDB.encryptedTableIn128BitsKey
insert into table unencryptedTable partition
    (ds='today')
    select key, value;

from encryptedWith128BitsKeyDB.encryptedTableIn128BitsKey
insert into table unencryptedTable partition
    (ds='today')
    select key, value;

select * from unencryptedTable order by key;

-- clean up
drop table encryptedWith128BitsKeyDB.encryptedTableIn128BitsKey;
drop table unencryptedTable;