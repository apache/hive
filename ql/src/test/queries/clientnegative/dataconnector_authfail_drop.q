--!qt:database:derby:qdb
-- CREATE IF NOT EXISTS already
CREATE CONNECTOR IF NOT EXISTS derby_auth
TYPE 'derby'
URL '${system:hive.test.database.qdb.jdbc.url}'
COMMENT 'test derby connector'
WITH DCPROPERTIES (
"hive.sql.dbcp.username"="APP",
"hive.sql.dbcp.password"="mine");

-- test data connector authorization feature
SET hive.security.authorization.enabled=true;

-- DROP fail
DROP CONNECTOR derby_auth;
