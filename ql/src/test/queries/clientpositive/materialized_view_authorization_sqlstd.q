--! qt:authorizer
set hive.vectorized.execution.enabled=false;
set user.name=user1;

create table amvs_table (a int, b varchar(256), c decimal(10,2));

insert into amvs_table values (1, 'alfred', 10.30),(2, 'bob', 3.14),(2, 'bonnie', 172342.2),(3, 'calvin', 978.76),(3, 'charlie', 9.8);

create materialized view amvs_mat_view disable rewrite as select a, c from amvs_table;

show grant user user1 on table amvs_mat_view;

grant select on amvs_mat_view to user user2;

set user.name=user2;
show grant user user2 on table amvs_mat_view;
select * from amvs_mat_view;

set user.name=user3;
show grant user user3 on table amvs_mat_view;


set user.name=hive_admin_user;
set role admin;
show grant on table amvs_mat_view;

set user.name=user1;
revoke select on table amvs_mat_view from user user2;
set user.name=user2;
show grant user user2 on table amvs_mat_view;

set user.name=hive_admin_user;
set role ADMIN;
create role role_v;
grant  role_v to user user4 ;
show role grant user user4;
show roles;

grant all on table amvs_mat_view to role role_v;
show grant role role_v on table amvs_mat_view;
show grant user user4 on table amvs_mat_view;
select * from amvs_mat_view;

set user.name=user1;
grant select on table amvs_table to user user2 with grant option;
set user.name=user2;
create materialized view amvs_mat_view2 disable rewrite as select a, b from amvs_table;

select * from amvs_mat_view2;

drop materialized view amvs_mat_view2;

set user.name=hive_admin_user;
set role ADMIN;

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set user.name=user1;

create database db1;
create table db1.testmvtable(id int, name string) partitioned by(year int) stored as orc TBLPROPERTIES ('transactional'='true');
insert into db1.testmvtable partition(year=2016) values(1,'Name1');

create database db2;
CREATE MATERIALIZED VIEW db2.testmv PARTITIONED ON(year) as select * from db1.testmvtable tmv where year >= 2018;

-- grant all on table to user2
grant all on table db1.testmvtable to user user2;
set user.name=user2;
explain select * from db1.testmvtable where year=2020;

set user.name=user1;
drop materialized view db2.testmv;
drop table db1.testmvtable;
drop database db1 cascade ;
drop database db2 cascade;

