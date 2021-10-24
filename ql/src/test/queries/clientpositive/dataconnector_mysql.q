--!qt:database:mysql:q_test_country_state_city_tables.sql
-- CREATE with comment
CREATE CONNECTOR mysql_qtest
TYPE 'mysql'
URL 'jdbc:mysql://localhost:3306/qtestDB'
COMMENT 'test connector'
WITH DCPROPERTIES (
"hive.sql.dbcp.username"="qtestuser",
"hive.sql.dbcp.password"="qtestpassword");
SHOW CONNECTORS;

CREATE REMOTE DATABASE db_mysql USING mysql_qtest with DBPROPERTIES("connector.remoteDbName"="qtestDB");
SHOW DATABASES;
USE db_mysql;
SHOW TABLES;

-- clean up so it won't affect other tests
DROP DATABASE db_mysql;
DROP CONNECTOR mysql_qtest;

