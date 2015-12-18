set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;
set user.name=hive_test_user;
set hive.security.authorization.enabled=true;

-- verify that sql std auth throws an error with hive cli, if auth is enabled
show tables 'src';
