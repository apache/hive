--! qt:authorizer
set user.name=user1;

-- check if create table fails as different user
create table t1(i int);
create view vt1 as select * from t1;

set user.name=user2;
drop view vt1;
