--! qt:authorizer
set user.name=hive_admin_user;

set role admin;
-- create role, db, make role the owner of db
create role testrole;
grant role testrole to user hrt_1;
create database testdb;
alter database testdb set owner role testrole;
desc database testdb;

-- actions that require user to be db owner 
-- create table
use testdb;
create table foobar (foo string, bar string);

-- drop db
drop database testdb cascade;
