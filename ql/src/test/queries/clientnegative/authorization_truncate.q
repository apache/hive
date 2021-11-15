--! qt:authorizer

-- check add partition without insert privilege
create table t1(i int, j int);
set user.name=user1;
truncate table t1;

