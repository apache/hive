--! qt:authorizer
set user.name=hive_test_user;

-- temp macro creation should fail for non-admin roles
create temporary macro mymacro1(x double) x * x;

