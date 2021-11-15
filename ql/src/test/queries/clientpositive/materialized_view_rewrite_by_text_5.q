--! qt:authorizer
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.materializedview.rewriting=false;

set user.name=user1;

create database db1;
create table db1.t1(col0 int) STORED AS ORC
                          TBLPROPERTIES ('transactional'='true');

create materialized view db1.mat1 as
SELECT * FROM db1.t1 WHERE col0 = 1;

grant select on table db1.t1 to user user2;

set user.name=user2;

-- user2 has no access to mv -> mv is not used
explain cbo
SELECT * FROM db1.t1 WHERE col0 = 1;

set user.name=user1;
grant all on db1.mat1 to user user2;

set user.name=user2;

-- mv is used
explain cbo
SELECT * FROM db1.t1 WHERE col0 = 1;

set user.name=user1;
drop materialized view db1.mat1;
