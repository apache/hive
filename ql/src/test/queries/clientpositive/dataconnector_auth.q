set hive.test.authz.sstd.hs2.mode=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authenticator.manager=org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator;

set user.name=hive_admin_user;
set role ADMIN;

SET hive.security.authorization.enabled=true;

-- CREATE IF NOT EXISTS already
CREATE CONNECTOR IF NOT EXISTS mysql_auth_dc
TYPE 'mysql'
URL 'jdbc:mysql://nightly1.apache.org:3306/hive1'
COMMENT 'test connector'
WITH DCPROPERTIES (
"hive.sql.dbcp.username"="hive1",
"hive.sql.dbcp.password"="hive1");
SHOW CONNECTORS;

-- alter connector set URL
alter connector mysql_auth_dc set URL 'jdbc:mysql://nightly1.apache.org:3306/hive2';
DESCRIBE CONNECTOR extended mysql_auth_dc;

-- alter connector set DCPROPERTIES
alter connector mysql_auth_dc set DCPROPERTIES("hive.sql.dbcp.username"="hive2","hive.sql.dbcp.password"="hive2");
DESCRIBE CONNECTOR extended mysql_auth_dc;

-- alter connector set owner
alter connector mysql_auth_dc set OWNER USER newuser;
DESCRIBE CONNECTOR extended mysql_auth_dc;

-- create remote database
CREATE REMOTE DATABASE db_mysql_auth USING mysql_auth_dc with DBPROPERTIES("connector.remoteDbName"="hive1");

DROP CONNECTOR mysql_auth_dc;
SHOW CONNECTORS;

