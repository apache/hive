set hive.users.in.admin.role=hive_admin_user;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set user.name=hive_test_user;

-- verify that sql std auth can be set as the authorizer with hive cli
-- and that the create table/view result in correct permissions (suitable for sql std auth mode)

create table t_cli(i int);
show grant user hive_test_user on t_cli;

create view v_cli (i) as select i from t_cli;
show grant user hive_test_user on v_cli;
