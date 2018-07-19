set hive.users.in.admin.role=hive_admin_user;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdConfOnlyAuthorizerFactory;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set hive.security.authorization.enabled=true;

-- verify that SQLStdConfOnlyAuthorizerFactory as the authorizer factory with hive cli, with hive.security.authorization.enabled=true
-- authorization verification would be just no-op

create table t_cli_n0(i int);
describe t_cli_n0;
