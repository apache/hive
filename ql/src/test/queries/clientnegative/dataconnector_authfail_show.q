-- CREATE IF NOT EXISTS already
CREATE CONNECTOR IF NOT EXISTS mysql_auth_show
TYPE 'mysql'
URL 'jdbc:mysql://nightly1.apache.org:3306/hive1'
COMMENT 'test connector'
WITH DCPROPERTIES (
"hive.sql.dbcp.username"="hive1",
"hive.sql.dbcp.password"="hive1");

-- test data connector authorization feature
SET hive.security.authorization.enabled=true;

-- SHOW fail: enumerating connectors is expected to carry the same requirement as the
-- create / alter / drop counterparts
SHOW CONNECTORS;
