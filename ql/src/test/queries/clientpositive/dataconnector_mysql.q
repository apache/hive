--!qt:database:mysql:qdb:q_test_country_state_city_tables.sql
-- CREATE with comment
CREATE CONNECTOR mysql_qtest
TYPE 'mysql'
URL '${system:hive.test.database.qdb.jdbc.url}'
COMMENT 'test connector'
WITH DCPROPERTIES (
"hive.sql.dbcp.username"="${system:hive.test.database.qdb.jdbc.username}",
"hive.sql.dbcp.password"="${system:hive.test.database.qdb.jdbc.password}",
"hive.connector.autoReconnect"="true",
"hive.connector.maxReconnects"="3",
"hive.connector.connectTimeout"="10000");
SHOW CONNECTORS;

CREATE REMOTE DATABASE db_mysql USING mysql_qtest with DBPROPERTIES("connector.remoteDbName"="qdb");
SHOW DATABASES;
USE db_mysql;
SHOW TABLES;

SHOW CREATE TABLE country;
SELECT * FROM country;

-- clean up so it won't affect other tests
DROP DATABASE db_mysql;
DROP CONNECTOR mysql_qtest;

