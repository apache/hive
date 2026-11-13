--! qt:database:oracle:qdb:q_test_case_sensitive_country_table.oracle.sql
-- Oracle folds unquoted identifiers to uppercase and preserves quoted ones verbatim.

CREATE EXTERNAL TABLE country_upper (id int, name varchar(20))
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
    "hive.sql.database.type" = "ORACLE",
    "hive.sql.jdbc.driver" = "oracle.jdbc.OracleDriver",
    "hive.sql.jdbc.url" = "jdbc:oracle:thin:@//${system:hive.test.database.qdb.host}:${system:hive.test.database.qdb.port}/XEPDB1",
    "hive.sql.dbcp.username" = "bob",
    "hive.sql.dbcp.password" = "bobpass",
    "hive.sql.schema" = "BOB",
    "hive.sql.table" = "COUNTRY");

EXPLAIN CBO SELECT COUNT(*) FROM country_upper;
SELECT COUNT(*) FROM country_upper;

-- The quoted mixed-case table must resolve to bob."Country" (2 rows).
CREATE EXTERNAL TABLE country_mixed (id int, name varchar(20))
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
    "hive.sql.database.type" = "ORACLE",
    "hive.sql.jdbc.driver" = "oracle.jdbc.OracleDriver",
    "hive.sql.jdbc.url" = "jdbc:oracle:thin:@//${system:hive.test.database.qdb.host}:${system:hive.test.database.qdb.port}/XEPDB1",
    "hive.sql.dbcp.username" = "bob",
    "hive.sql.dbcp.password" = "bobpass",
    "hive.sql.schema" = "BOB",
    "hive.sql.table" = '"Country"');

EXPLAIN CBO SELECT COUNT(*) FROM country_mixed;
SELECT COUNT(*) FROM country_mixed;

