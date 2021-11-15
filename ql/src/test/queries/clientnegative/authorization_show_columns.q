--! qt:authorizer

create database db1;
use db1;
-- check query without select privilege fails
create table t1(i int);

set user.name=user1;
show columns in t1;

