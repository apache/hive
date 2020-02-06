--! qt:authorizer
set user.name=hive_admin_user;

-- admin required for create macro
set role ADMIN;

create temporary macro mymacro1(x double) x * x;

drop temporary macro mymacro1;
