-- SORT_QUERY_RESULTS
SHOW CONNECTORS;

-- CREATE with comment
CREATE CONNECTOR redshift_test
TYPE 'redshift'
URL 'jdbc:redshift://redshift-cluster-1.c1gffkxfot1v.us-east-2.redshift.amazonaws.com:5439/dev'
COMMENT 'redshift test connector'
WITH DCPROPERTIES (
"hive.sql.dbcp.username"="user",
"hive.sql.dbcp.password"="password");
SHOW CONNECTORS;

-- CREATE IF NOT EXISTS already
CREATE CONNECTOR IF NOT EXISTS redshift_test
TYPE 'redshift'
URL 'jdbc:redshift://redshift-cluster-1.c1gffkxfot1v.us-east-2.redshift.amazonaws.com:5439/dev'
COMMENT 'redshift test connector'
WITH DCPROPERTIES (
"hive.sql.dbcp.username"="user",
"hive.sql.dbcp.password"="password");
SHOW CONNECTORS;

-- DROP
DROP CONNECTOR redshift_test;
SHOW CONNECTORS;

-- DROP IF exists
DROP CONNECTOR IF EXISTS redshift_test;
SHOW CONNECTORS;

-- recreate with same name
CREATE CONNECTOR redshift_test
TYPE 'redshift'
URL 'jdbc:redshift://redshift-cluster-1.c1gffkxfot1v.us-east-2.redshift.amazonaws.com:5439/dev'
COMMENT 'redshift test connector'
WITH DCPROPERTIES (
"hive.sql.dbcp.username"="user",
"hive.sql.dbcp.password"="password");
SHOW CONNECTORS;

CREATE REMOTE DATABASE db_redshift USING redshift_test with DBPROPERTIES("connector.remoteDbName"="dev");
SHOW DATABASES;
USE db_redshift;
SHOW TABLES;

-- alter connector set URL
alter connector redshift_test set URL 'jdbc:redshift://redshift-cluster-1.c1gffkxfot1v.us-west-2.redshift.amazonaws.com:5439/dev2';
DESCRIBE CONNECTOR extended redshift_test;

-- alter connector set DCPROPERTIES
alter connector redshift_test set DCPROPERTIES("hive.sql.dbcp.username"="user2","hive.sql.dbcp.password"="password2");
DESCRIBE CONNECTOR extended redshift_test;

-- alter connector set owner
alter connector redshift_test set OWNER USER newuser;
DESCRIBE CONNECTOR extended redshift_test;

-- drop remote database and connector
DROP DATABASE db_redshift;
SHOW DATABASES;
DROP CONNECTOR redshift_test;
SHOW CONNECTORS;
DROP CONNECTOR IF EXISTS redshift_test;
SHOW CONNECTORS;
