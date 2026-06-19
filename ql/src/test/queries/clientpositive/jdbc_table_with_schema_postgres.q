--! qt:database:postgres:qdb:q_test_country_table_with_schema.postgres.sql
-- Postgres allows multiple schemas per database so to disambiguate between tables in different schemas it
-- is necessary to set the hive.sql.schema property properly.

-- Some JDBC APIs require the catalog, schema, and table names to be passed exactly as they are stored in the database.
-- Postgres stores unquoted identifiers by first converting them to lowercase thus the hive.sql.schema and
-- hive.sql.table properties below are specified in lowercase.

CREATE EXTERNAL TABLE country_0 (id int, name varchar(20))
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
    "hive.sql.database.type" = "POSTGRES",
    "hive.sql.jdbc.driver" = "org.postgresql.Driver",
    "hive.sql.jdbc.url" = "${system:hive.test.database.qdb.jdbc.url}",
    "hive.sql.dbcp.username" = "${system:hive.test.database.qdb.jdbc.username}",
    "hive.sql.dbcp.password" = "${system:hive.test.database.qdb.jdbc.password}",
    "hive.sql.schema" = "bob",
    "hive.sql.table" = "country");

EXPLAIN CBO SELECT COUNT(*) FROM country_0;
SELECT COUNT(*) FROM country_0;

CREATE EXTERNAL TABLE country_1 (id int, name varchar(20))
    STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
    TBLPROPERTIES (
        "hive.sql.database.type" = "POSTGRES",
        "hive.sql.jdbc.driver" = "org.postgresql.Driver",
        "hive.sql.jdbc.url" = "${system:hive.test.database.qdb.jdbc.url}",
        "hive.sql.dbcp.username" = "${system:hive.test.database.qdb.jdbc.username}",
        "hive.sql.dbcp.password" = "${system:hive.test.database.qdb.jdbc.password}",
        "hive.sql.schema" = "alice",
        "hive.sql.table" = "country");

EXPLAIN CBO SELECT COUNT(*) FROM country_1;
SELECT COUNT(*) FROM country_1;

-- Test DML statements are working fine when accessing table in non-default schema
INSERT INTO country_1 VALUES (8, 'Hungary');
SELECT * FROM country_1 ORDER BY id;

-- A user in Postgres can be assigned a default schema (aka. search_path). In that case specifying the
-- hive.sql.schema property is redundant.
CREATE EXTERNAL TABLE country_2 (id int, name varchar(20))
    STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
    TBLPROPERTIES (
        "hive.sql.database.type" = "POSTGRES",
        "hive.sql.jdbc.driver" = "org.postgresql.Driver",
        "hive.sql.jdbc.url" = "${system:hive.test.database.qdb.jdbc.url}",
        "hive.sql.dbcp.username" = "greg",
        "hive.sql.dbcp.password" = "GregPass123!$",
        "hive.sql.table" = "country");

EXPLAIN CBO SELECT COUNT(*) FROM country_2;
SELECT COUNT(*) FROM country_2;
