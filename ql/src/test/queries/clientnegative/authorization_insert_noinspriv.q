--! qt:authorizer

-- check insert without select priv
create table t1(i int);

set user.name=user1;
create table user2tab(i int);
insert into table t1 select * from user2tab;

