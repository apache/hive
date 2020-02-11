--! qt:authorizer
set user.name=user1;

-- ensure that drop database cascade works
create database dba1;
create table dba1.tab1(i int);
drop database dba1 cascade;

-- check if drop database fails if the db has a table for which user does not have permission
create database dba2;
create table dba2.tab2(i int);

set user.name=hive_admin_user;
set role ADMIN;
alter database dba2 set owner user user2;

set user.name=user2;
show current roles;
drop database dba2 cascade ;
