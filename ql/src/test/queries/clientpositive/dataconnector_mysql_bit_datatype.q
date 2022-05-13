--!qt:database:mysql:q_test_mysql_datatype_mapping.sql
-- CREATE with comment
CREATE CONNECTOR mysql_data_type_dc
TYPE 'mysql'
URL 'jdbc:mysql://localhost:3306/qtestDB'
COMMENT 'test connector'
WITH DCPROPERTIES (
"hive.sql.dbcp.username"="root",
"hive.sql.dbcp.password"="qtestpassword");

CREATE REMOTE DATABASE mysql_data_type_db USING mysql_data_type_dc with DBPROPERTIES("connector.remoteDbName"="qtestDB");

USE mysql_data_type_db;
SHOW TABLES;

SHOW CREATE TABLE work_calendar;
-- SELECT * FROM work_calendar;  //Wait for HIVE-26192 to be fixed

SHOW CREATE TABLE work_attendance;
-- SELECT * FROM work_attendance;  //Wait for HIVE-26192 to be fixed

-- clean up so it won't affect other tests
DROP DATABASE mysql_data_type_db;
DROP CONNECTOR mysql_data_type_dc;
