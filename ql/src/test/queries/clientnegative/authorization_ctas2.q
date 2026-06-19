--! qt:authorizer

set user.name=user_dbowner;
-- check ctas without db ownership
create database ctas_auth;

set user.name=user_unauth;
create table t1(i int);
use ctas_auth;
show tables;
create table t2 as select * from default.t1;
