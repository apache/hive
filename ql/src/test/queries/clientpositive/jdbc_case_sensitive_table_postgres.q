--! qt:database:postgres:qdb:q_test_case_sensitive_country_table.postgres.sql
-- Postgres folds unquoted identifiers to lowercase and preserves quoted ones verbatim.

CREATE EXTERNAL TABLE country_lower (id int, name varchar(20))
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
    "hive.sql.database.type" = "POSTGRES",
    "hive.sql.jdbc.driver" = "org.postgresql.Driver",
    "hive.sql.jdbc.url" = "${system:hive.test.database.qdb.jdbc.url}",
    "hive.sql.dbcp.username" = "${system:hive.test.database.qdb.jdbc.username}",
    "hive.sql.dbcp.password" = "${system:hive.test.database.qdb.jdbc.password}",
    "hive.sql.schema" = "bob",
    "hive.sql.table" = "country");

EXPLAIN CBO SELECT COUNT(*) FROM country_lower;
SELECT COUNT(*) FROM country_lower;

-- The quoted mixed-case table must resolve to bob."Country" (2 rows), not bob.country.
CREATE EXTERNAL TABLE country_mixed (id int, name varchar(20))
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
    "hive.sql.database.type" = "POSTGRES",
    "hive.sql.jdbc.driver" = "org.postgresql.Driver",
    "hive.sql.jdbc.url" = "${system:hive.test.database.qdb.jdbc.url}",
    "hive.sql.dbcp.username" = "${system:hive.test.database.qdb.jdbc.username}",
    "hive.sql.dbcp.password" = "${system:hive.test.database.qdb.jdbc.password}",
    "hive.sql.schema" = "bob",
    "hive.sql.table" = '"Country"');

EXPLAIN CBO SELECT COUNT(*) FROM country_mixed;
SELECT COUNT(*) FROM country_mixed;



