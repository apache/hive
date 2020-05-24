--! qt:authorizer

-- check insert without select priv
create table t1(i int);

set user.name=user1;
create table t2(i int);
insert into table t2 select * from t1;

