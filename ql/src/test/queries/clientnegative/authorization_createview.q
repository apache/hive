--! qt:authorizer

-- check create view without select privileges
create table t1(i int);
set user.name=user1;
create view v1 as select * from t1;


