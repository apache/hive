--! qt:authorizer

-- check truncate on partition
create table auth_trunc2(i int) partitioned by (j int);
alter table auth_trunc2 add partition (j=42);
set user.name=user1;
truncate table auth_trunc2 partition (j=42);

