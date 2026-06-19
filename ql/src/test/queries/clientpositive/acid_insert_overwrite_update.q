set hive.mapred.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

-- Check with update
drop table if exists sequential_update;
create transactional table sequential_update(ctime timestamp, seq bigint, mtime timestamp, prev_writeid bigint, prev_dirname string) stored as orc;
insert overwrite table sequential_update values(current_timestamp, 0, current_timestamp, 0, '');
select distinct IF(seq==0, 'LOOKS OKAY', 'BROKEN'), regexp_extract(INPUT__FILE__NAME, '.*/(.*)/[^/]*', 1) from sequential_update;

update sequential_update set mtime = current_timestamp, seq=1, prev_writeid = ROW__ID.writeId, prev_dirname = regexp_extract(INPUT__FILE__NAME, '.*/(.*)/[^/]*', 1);
select distinct IF(seq==1, 'LOOKS OKAY', 'BROKEN'), regexp_extract(INPUT__FILE__NAME, '.*/(.*)/[^/]*', 1) from sequential_update;


-- Check with compaction
insert overwrite table sequential_update values(current_timestamp, 0, current_timestamp, 0, '');
update sequential_update set mtime = current_timestamp, seq=1, prev_writeid = ROW__ID.writeId, prev_dirname = regexp_extract(INPUT__FILE__NAME, '.*/(.*)/[^/]*', 1);
select distinct IF(seq==1, 'LOOKS OKAY', 'BROKEN'), regexp_extract(INPUT__FILE__NAME, '.*/(.*)/[^/]*', 1) from sequential_update;

alter table sequential_update compact 'major';
select distinct IF(seq==1, 'LOOKS OKAY', 'BROKEN'), regexp_extract(INPUT__FILE__NAME, '.*/(.*)/[^/]*', 1) from sequential_update;


-- Check with deletes
insert overwrite table sequential_update values(current_timestamp, 0, current_timestamp, 0, ''), (current_timestamp, 2, current_timestamp, 2, '');
delete from sequential_update where seq=2;
select distinct IF(seq==0, 'LOOKS OKAY', 'BROKEN'), regexp_extract(INPUT__FILE__NAME, '.*/(.*)/[^/]*', 1) from sequential_update;

select distinct IF(seq==0, 'LOOKS OKAY', 'BROKEN'), regexp_extract(INPUT__FILE__NAME, '.*/(.*)/[^/]*', 1) from sequential_update;

-- Check with load
drop table if exists orc_test_txn;
create table  orc_test_txn (`id` integer, name string, dept string) partitioned by(year integer) stored as orc TBLPROPERTIES('transactional'='true');
load data local inpath '../../data/files/load_data_job_acid' into table orc_test_txn;

select * from orc_test_txn;
update orc_test_txn set id=1;

select distinct IF(id==1, 'LOOKS OKAY', 'BROKEN') from orc_test_txn;

alter table orc_test_txn partition(year='2016') compact 'major';
alter table orc_test_txn partition(year='2017') compact 'major';
alter table orc_test_txn partition(year='2018') compact 'major';
select distinct IF(id==1, 'LOOKS OKAY', 'BROKEN') from orc_test_txn;



