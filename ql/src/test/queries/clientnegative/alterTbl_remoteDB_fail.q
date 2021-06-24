-- CREATE IF NOT EXISTS already
CREATE CONNECTOR IF NOT EXISTS mysql_test_2
TYPE 'mysql'
URL 'jdbc:mysql://nightly7x-unsecure-1.nightly7x-unsecure.root.hwx.site:3306/hive1'
COMMENT 'test connector'
WITH DCPROPERTIES (
"hive.sql.dbcp.username"="hive1",
"hive.sql.dbcp.password"="hive1");
SHOW CONNECTORS;

-- CREATE and USE remote database
CREATE REMOTE database mysql_db_2 using mysql_test_2 with DBPROPERTIES("connector.remoteDbName"="hive1");
USE mysql_db_2;

-- ALTER TABLE is not allowed in remote database
ALTER TABLE temp_tbls RENAME TO perm_tbls;