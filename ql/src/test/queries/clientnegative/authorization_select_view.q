--! qt:authorizer

-- check create view without select privileges
create table t1(i int);
create view v1 as select * from t1;
set user.name=user1;
select * from v1;


