--! qt:authorizer
set user.name=user1;

create database db1;
use db1;
-- check if create table fails as different user. use db.table sytax
create table t1(i int);
use default;

set user.name=user2;
drop table db1.t1;
