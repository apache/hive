--!qt:database:mysql:q_test_mysql_datatype_mapping.sql
-- CREATE with comment
CREATE CONNECTOR mysql_qtest
TYPE 'mysql'
URL 'jdbc:mysql://localhost:3306/qtestDB'
COMMENT 'test connector'
WITH DCPROPERTIES (
"hive.sql.dbcp.username"="root",
"hive.sql.dbcp.password"="qtestpassword");

CREATE REMOTE DATABASE db_mysql USING mysql_qtest with DBPROPERTIES("connector.remoteDbName"="qtestDB");

USE db_mysql;
SHOW TABLES;

SHOW CREATE TABLE work_calendar;
-- SELECT * FROM work_calendar;  //Wait for HIVE-26192 to be fixed

SHOW CREATE TABLE work_attendance;
-- SELECT * FROM work_attendance;  //Wait for HIVE-26192 to be fixed

-- clean up so it won't affect other tests
DROP DATABASE db_mysql;
DROP CONNECTOR mysql_qtest;
