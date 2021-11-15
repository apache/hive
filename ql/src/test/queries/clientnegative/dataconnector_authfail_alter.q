-- CREATE IF NOT EXISTS already
CREATE CONNECTOR IF NOT EXISTS derby_auth
TYPE 'derby'
URL 'jdbc:derby:./target/tmp/junit_metastore_db;create=true'
COMMENT 'test derby connector'
WITH DCPROPERTIES (
"hive.sql.dbcp.username"="APP",
"hive.sql.dbcp.password"="mine");

-- test data connector authorization feature
SET hive.security.authorization.enabled=true;

-- ALTER fail
alter connector derby_auth set DCPROPERTIES("hive.sql.dbcp.username"="PPA", "hive.sql.dbcp.password"="yours");
