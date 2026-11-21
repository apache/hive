--! qt:database:mssql:qdb:q_test_case_sensitive_country_table.mssql.sql

CREATE EXTERNAL TABLE country_lower (id int, name varchar(20))
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
    "hive.sql.database.type" = "MSSQL",
    "hive.sql.jdbc.driver" = "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "hive.sql.jdbc.url" = "${system:hive.test.database.qdb.jdbc.url};DatabaseName=worldcs;",
    "hive.sql.dbcp.username" = "${system:hive.test.database.qdb.jdbc.username}",
    "hive.sql.dbcp.password" = "${system:hive.test.database.qdb.jdbc.password}",
    "hive.sql.schema" = "bob",
    "hive.sql.table" = "country");

EXPLAIN CBO SELECT COUNT(*) FROM country_lower;
SELECT COUNT(*) FROM country_lower;

-- The bracket-quoted mixed-case table must resolve to bob.[Country] (2 rows).
CREATE EXTERNAL TABLE country_mixed (id int, name varchar(20))
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
    "hive.sql.database.type" = "MSSQL",
    "hive.sql.jdbc.driver" = "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "hive.sql.jdbc.url" = "${system:hive.test.database.qdb.jdbc.url};DatabaseName=worldcs;",
    "hive.sql.dbcp.username" = "${system:hive.test.database.qdb.jdbc.username}",
    "hive.sql.dbcp.password" = "${system:hive.test.database.qdb.jdbc.password}",
    "hive.sql.schema" = "bob",
    "hive.sql.table" = "[Country]");

EXPLAIN CBO SELECT COUNT(*) FROM country_mixed;
SELECT COUNT(*) FROM country_mixed;

