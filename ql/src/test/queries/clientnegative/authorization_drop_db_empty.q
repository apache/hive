--! qt:authorizer
set user.name=user1;

-- check if changing owner and dropping as other user works
create database dba1;

set user.name=hive_admin_user;
set role ADMIN;
alter database dba1 set owner user user2;

set user.name=user2;
show current roles;
drop database dba1;


set user.name=user1;
-- check if dropping db as another user fails
show current roles;
create database dba2;

set user.name=user2;
show current roles;

drop database dba2;
