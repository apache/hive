set hive.test.authz.sstd.hs2.mode=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;

set user.name=user1;
create table authorization_fail (key int, value string) partitioned by (ds string);
GRANT SELECT ON authorization_fail TO USER user2 WITH GRANT OPTION;

set user.name=user2;
SHOW GRANT USER user2 ON TABLE authorization_fail;
-- user2 current has grant option, this should work
GRANT SELECT ON authorization_fail TO USER user3;
REVOKE SELECT ON authorization_fail FROM USER user3;

set user.name=user1;
REVOKE GRANT OPTION FOR SELECT ON authorization_fail FROM USER user2;

set user.name=user2;
SHOW GRANT USER user2 ON TABLE authorization_fail;
-- Now that grant option has been revoked, granting to other users should fail
GRANT SELECT ON authorization_fail TO USER user3;

