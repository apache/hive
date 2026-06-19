--! qt:authorizer
set user.name=user1;

-- check if create table fails as different user
create table t1(i int);

set user.name=user2;
drop table t1;

