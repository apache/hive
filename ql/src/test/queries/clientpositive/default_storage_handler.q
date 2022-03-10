-- Set a default storage handler class
SET hive.default.storage.handler.class=org.apache.hadoop.hive.ql.log.syslog.SyslogStorageHandler;

CREATE EXTERNAL TABLE test_tbl_with_default(a string, b int);

DESCRIBE FORMATTED test_tbl_with_default;

-- Set a default storage handler class,
-- create table command contains a STORED AS clause to be ignored
SET hive.default.storage.handler.class=org.apache.hadoop.hive.ql.log.syslog.SyslogStorageHandler;

CREATE EXTERNAL TABLE test_tbl_with_default_orc(a string, b int) STORED AS ORC;

DESCRIBE FORMATTED test_tbl_with_default_orc;

-- Default storage handler is set, but overridden explicitly in the create table command
SET hive.default.storage.handler.class=org.apache.hadoop.hive.ql.log.syslog.SyslogStorageHandler;

CREATE EXTERNAL TABLE test_tbl_overridden(DB_ID bigint, NAME STRING)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" = "SELECT DB_ID, NAME FROM DBS"
);

DESCRIBE FORMATTED test_tbl_overridden;

-- Unset default storage handler prop
SET hive.default.storage.handler.class=;

CREATE EXTERNAL TABLE test_tbl_no_default(a string, b int) STORED AS ORC;

DESCRIBE FORMATTED test_tbl_no_default;
