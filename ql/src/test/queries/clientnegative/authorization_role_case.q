create role mixCaseRole1;
create role mixCaseRole2;

show roles;


create table t1(i int);
grant SELECT  on table t1 to role mixCaseRole1;
-- grant with wrong case should fail with legacy auth
grant UPDATE  on table t1 to role mixcaserole2;
