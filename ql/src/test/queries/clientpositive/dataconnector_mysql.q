--!qt:database:mysql:q_test_country_state_city_tables.sql
-- CREATE with comment
CREATE CONNECTOR mysql_qtest
TYPE 'mysql'
URL 'jdbc:mysql://localhost:3306/qtestDB'
COMMENT 'test connector'
WITH DCPROPERTIES (
"hive.sql.dbcp.username"="root",
"hive.sql.dbcp.password"="qtestpassword",
"hive.connector.autoReconnect"="true",
"hive.connector.maxReconnects"="3",
"hive.connector.connectTimeout"="10000");
SHOW CONNECTORS;

CREATE REMOTE DATABASE db_mysql USING mysql_qtest with DBPROPERTIES("connector.remoteDbName"="qtestDB");
SHOW DATABASES;
USE db_mysql;
SHOW TABLES;

SHOW CREATE TABLE country;
SELECT * FROM country;

-- clean up so it won't affect other tests
DROP DATABASE db_mysql;
DROP CONNECTOR mysql_qtest;

