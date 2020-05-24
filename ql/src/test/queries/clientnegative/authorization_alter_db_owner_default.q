--! qt:authorizer
set user.name=user1;

-- check if alter table owner fails
alter database default set owner user user1;

