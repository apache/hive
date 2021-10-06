-- SORT_QUERY_RESULTS
SHOW CONNECTORS;

-- CREATE with comment
CREATE CONNECTOR mysql_test
TYPE 'mysql'
URL 'jdbc:mysql://nightly1.apache.org:3306/hive1'
COMMENT 'test connector'
WITH DCPROPERTIES (
"hive.sql.dbcp.username"="hive1",
"hive.sql.dbcp.password"="hive1");
SHOW CONNECTORS;

-- CREATE IF NOT EXISTS already
CREATE CONNECTOR IF NOT EXISTS mysql_test
TYPE 'mysql'
URL 'jdbc:mysql://nightly1.apache.org:3306/hive1'
COMMENT 'test connector'
WITH DCPROPERTIES (
"hive.sql.dbcp.username"="hive1",
"hive.sql.dbcp.password"="hive1");
SHOW CONNECTORS;

-- CREATE IF NOT EXISTS already
CREATE CONNECTOR IF NOT EXISTS derby_test
TYPE 'derby'
URL 'jdbc:derby:./target/db_for_connectortest.db;create=true'
COMMENT 'test derby connector'
WITH DCPROPERTIES (
"hive.sql.dbcp.username"="APP",
"hive.sql.dbcp.password"="mine");

-- DROP
DROP CONNECTOR mysql_test;
SHOW CONNECTORS;

-- DROP IF exists
DROP CONNECTOR IF EXISTS mysql_test;
SHOW CONNECTORS;

-- recreate with same name
CREATE CONNECTOR mysql_test
TYPE 'mysql'
URL 'jdbc:mysql://nightly1.apache.org:3306/hive1'
COMMENT 'test connector'
WITH DCPROPERTIES (
"hive.sql.dbcp.username"="hive1",
"hive.sql.dbcp.password"="hive1");
SHOW CONNECTORS;

CREATE REMOTE DATABASE db_derby USING derby_test with DBPROPERTIES("connector.remoteDbName"="APP");
SHOW DATABASES;
USE db_derby;
SHOW TABLES;

-- alter connector set URL
alter connector mysql_test set URL 'jdbc:mysql://nightly1.apache.org:3306/hive2';
DESCRIBE CONNECTOR extended mysql_test;

-- alter connector set DCPROPERTIES
alter connector mysql_test set DCPROPERTIES("hive.sql.dbcp.username"="hive2","hive.sql.dbcp.password"="hive2");
DESCRIBE CONNECTOR extended mysql_test;

-- alter connector set owner
alter connector mysql_test set OWNER USER newuser;
DESCRIBE CONNECTOR extended mysql_test;

-- drop remote database and connector
DROP DATABASE db_derby;
SHOW DATABASES;
DROP CONNECTOR mysql_test;
SHOW CONNECTORS;
DROP CONNECTOR derby_test;
SHOW CONNECTORS;
