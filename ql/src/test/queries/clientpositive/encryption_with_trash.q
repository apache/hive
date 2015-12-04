set fs.trash.interval=5

-- SORT_QUERY_RESULTS

-- init
drop table IF EXISTS encryptedTableSrc PURGE;
drop table IF EXISTS unencryptedTable PURGE;

create table encryptedTableSrc(key string, value string)
LOCATION '${hiveconf:hive.metastore.warehouse.dir}/encryptedTableSrc';

create table encryptedTable(key string, value string) partitioned by (ds string)
    LOCATION '${hiveconf:hive.metastore.warehouse.dir}/encryptedTable';
CRYPTO CREATE_KEY --keyName key_1 --bitLength 128;
CRYPTO CREATE_ZONE --keyName key_1 --path ${hiveconf:hive.metastore.warehouse.dir}/encryptedTableSrc;
CRYPTO CREATE_ZONE --keyName key_1 --path ${hiveconf:hive.metastore.warehouse.dir}/encryptedTable;

-- insert src table from values
insert into table encryptedTableSrc values ('501', 'val_501'), ('502', 'val_502');

insert into table encryptedTable partition (ds='today') select key, value from encryptedTableSrc;
select count(*) from encryptedTable where ds='today';
insert into table encryptedTable partition (ds='today') select key, value from encryptedTableSrc;
select count(*) from encryptedTable where ds='today';

insert overwrite table encryptedTable partition (ds='today') select key, value from encryptedTableSrc;
select count(*) from encryptedTable where ds='today';

-- clean up
drop table encryptedTable PURGE;
drop table unencryptedTable PURGE;
CRYPTO DELETE_KEY --keyName key_1;
set fs.trash.interval=0
