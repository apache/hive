-- CREATE IF NOT EXISTS already
CREATE CONNECTOR IF NOT EXISTS mysql_test
TYPE 'mysql'
URL 'jdbc:mysql://nightly1.apache.org:3306/hive1'
COMMENT 'test connector'
WITH DCPROPERTIES (
"hive.sql.dbcp.username"="hive1",
"hive.sql.dbcp.password"="hive1");
SHOW CONNECTORS;

-- CREATE and USE remote database
CREATE REMOTE database mysql_db using mysql_test with DBPROPERTIES("connector.remoteDbName"="hive1");
USE mysql_db;

-- CREATE TABLE is not allowed in remote database
create table bees (id int, name string);
