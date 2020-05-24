--! qt:authorizer
set user.name=user1;

-- check if alter table fails as different user
create table t_show_parts(i int) partitioned by (j string);

set user.name=user2;
show partitions t_show_parts;
