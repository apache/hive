--! qt:authorizer
set user.name=user1;

-- check if alter table owner fails 
-- for now, alter db owner is allowed only for admin

create database dbao;
alter database dbao set owner user user2;

