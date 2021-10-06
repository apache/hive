-- CREATE IF NOT EXISTS already
CREATE CONNECTOR IF NOT EXISTS mysql_test
TYPE 'mysql'
URL 'jdbc:mysql://nightly1.apache.org:3306/hive1'
COMMENT 'test connector'
WITH DCPROPERTIES (
"hive.sql.dbcp.username"="hive1",
"hive.sql.dbcp.password"="hive1");

-- reject location and managedlocation config in remote database
create REMOTE database mysql_rej location '/tmp/rej1.db' managedlocation '/tmp/rej2.db' using mysql_test with DBPROPERTIES("connector.remoteDbName"="hive1");
