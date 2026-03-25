-- SORT_QUERY_RESULTS

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

drop table if exists iceberg_txn_t1;
drop table if exists iceberg_txn_t2;

create external table iceberg_txn_t1(a int) stored by iceberg
tblproperties ('format-version'='2');

create external table iceberg_txn_t2(a int) stored by iceberg
tblproperties ('format-version'='2');

-- Multi-table insert
from (
  select 1 as a union all select 2
) s
insert into iceberg_txn_t1
  select a
insert into iceberg_txn_t2
  select a + 10;

select * from iceberg_txn_t1 order by a;
select * from iceberg_txn_t2 order by a;

-- Multi-table transaction
start transaction;
insert into iceberg_txn_t1 values (3);
update iceberg_txn_t2 set a = a + 1;
commit;

select * from iceberg_txn_t1 order by a;
select * from iceberg_txn_t2 order by a;

-- Multi-statement transaction
start transaction;
insert into iceberg_txn_t1 values (4);
update iceberg_txn_t1 set a = a + 1;
commit;

select * from iceberg_txn_t1 order by a;

-- Read from affected table mid-txn
start transaction;
update iceberg_txn_t1 set a = a + 1;
insert into iceberg_txn_t2 select * from iceberg_txn_t1;
commit;

select * from iceberg_txn_t1 order by a;
select * from iceberg_txn_t2 order by a;

drop table if exists iceberg_txn_t1;
drop table if exists iceberg_txn_t2;
