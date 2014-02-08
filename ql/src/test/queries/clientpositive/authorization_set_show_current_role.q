set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory;

show current roles;

create role r1;
grant role r1 to user hive_test_user;
set role r1;
show current roles;

set role PUBLIC;
show current roles;

set role NONE;
show current roles;

drop role r1;

