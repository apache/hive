--! qt:authorizer

set user.name=user1;
create table t1(i int, j int, k int);

show grant user user2 on table t1;
