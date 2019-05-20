set hive.users.in.admin.role=hive_admin_user;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set hive.metastore.pre.event.listeners=org.apache.hadoop.hive.ql.security.authorization.AuthorizationPreEventListener;
set hive.security.metastore.authorization.manager=org.apache.hadoop.hive.ql.security.MetastoreAuthzAPIDisallowAuthorizer;
set user.name=hive_test_user;

-- verify that sql std auth can be set as the authorizer with hive cli, while metastore authorization api calls are disabled (for cli)

create table t_cli_n1(i int);

create view v_cli_n0 (i) as select i from t_cli_n1;
