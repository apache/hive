--! qt:authorizer

set user.name=hive_admin_user;
set role admin;
create role role1;


set user.name=user1;
show grant role role1;
