--! qt:authorizer

set user.name=user1;
-- check add partition without insert privilege
create table tpart(i int, j int) partitioned by (k string);         

set user.name=user2;
alter table tpart add partition (k = 'abc');
