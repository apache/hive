-- CREATE IF NOT EXISTS already
CREATE CONNECTOR IF NOT EXISTS derby_test
TYPE 'derby'
URL 'jdbc:derby:./target/db_for_connectortest.db;create=true'
COMMENT 'test derby connector'
WITH DCPROPERTIES (
"hive.sql.dbcp.username"="APP",
"hive.sql.dbcp.password"="mine");
SHOW CONNECTORS;

-- CREATE and USE remote database
CREATE REMOTE DATABASE db_derby USING derby_test with DBPROPERTIES("connector.remoteDbName"="APP");
SHOW DATABASES;
USE db_derby;
SHOW TABLES;

-- ALTER TABLE is not allowed in remote database
ALTER TABLE TESTTABLE1 RENAME TO ALTERTABLE1;