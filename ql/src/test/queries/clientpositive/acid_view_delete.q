set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.strict.checks.cartesian.product=false;
set hive.mapred.mode=nostrict;

drop table if exists mydim;
drop table if exists updates_staging_table;
drop view if exists updates_staging_view;

create table mydim (key int, name string, zip string, is_current boolean)
clustered by(key) into 3 buckets
stored as orc tblproperties ('transactional'='true');

insert into mydim values
  (1, 'bob',   '95136', true),
  (2, 'joe',   '70068', true),
  (3, 'steve', '22150', true);

create table updates_staging_table (ks int, newzip string);
insert into updates_staging_table values (1, 87102), (3, 45220);

create view updates_staging_view (kv, newzip) as select ks, newzip from updates_staging_table;

delete from mydim
where mydim.key in (select kv from updates_staging_view where kv >=3);
select * from mydim order by key;

update mydim set is_current = false
where mydim.key not in(select kv from updates_staging_view);
select * from mydim order by key;

update mydim set name = 'Olaf'
where mydim.key in(select kv from updates_staging_view);
select * from mydim order by key;
