--! qt:authorizer

set user.name=user3;
create database db1;
use db1;
create table tab1(i int);

set user.name=user4;
-- create view should fail as view is being created in db that it does not own
create view db1.view1(i) as select * from tab1;
