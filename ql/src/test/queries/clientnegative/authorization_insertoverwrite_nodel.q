--! qt:authorizer
set user.name=hive_test_user;

-- check insert overwrite without delete priv
create table t1(i int);
grant insert on table t1 to user user1;

show grant user hive_test_user on table t1;

set user.name=user1;
show grant user user1 on table t1;

create table user1tab(i int);
insert overwrite table t1 select * from user1tab;
