--! qt:database:mssql:q_test_country_table_with_schema.mssql.sql
-- Microsoft SQL server allows multiple schemas per database so to disambiguate between tables in different schemas it
-- is necessary to set the hive.sql.schema property properly.

-- Some JDBC APIs require the catalog, schema, and table names to be passed exactly as they are stored in the database.
-- MSSQL stores unquoted identifiers by first converting them to lowercase thus the hive.sql.schema and
-- hive.sql.table properties below are specified in lowercase.

CREATE EXTERNAL TABLE country_0 (id int, name varchar(20))
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
    "hive.sql.database.type" = "MSSQL",
    "hive.sql.jdbc.driver" = "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "hive.sql.jdbc.url" = "jdbc:sqlserver://localhost:1433;DatabaseName=world;",
    "hive.sql.dbcp.username" = "sa",
    "hive.sql.dbcp.password" = "Its-a-s3cret",
    "hive.sql.schema" = "bob",
    "hive.sql.table" = "country");

EXPLAIN CBO SELECT COUNT(*) FROM country_0;
SELECT COUNT(*) FROM country_0;

CREATE EXTERNAL TABLE country_1 (id int, name varchar(20))
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
    "hive.sql.database.type" = "MSSQL",
    "hive.sql.jdbc.driver" = "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "hive.sql.jdbc.url" = "jdbc:sqlserver://localhost:1433;DatabaseName=world;",
    "hive.sql.dbcp.username" = "sa",
    "hive.sql.dbcp.password" = "Its-a-s3cret",
    "hive.sql.schema" = "alice",
    "hive.sql.table" = "country");

EXPLAIN CBO SELECT COUNT(*) FROM country_1;
SELECT COUNT(*) FROM country_1;

-- Test DML statements are working fine when accessing table in non-default schema
INSERT INTO country_1 VALUES (8, 'Hungary');
SELECT * FROM country_1;

-- A user in MSSQL server can be assigned a default schema. In that case specifying the hive.sql.schema property is
-- redundant. 
CREATE EXTERNAL TABLE country_2 (id int, name varchar(20))
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
    "hive.sql.database.type" = "MSSQL",
    "hive.sql.jdbc.driver" = "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "hive.sql.jdbc.url" = "jdbc:sqlserver://localhost:1433;DatabaseName=world;",
    "hive.sql.dbcp.username" = "greg",
    "hive.sql.dbcp.password" = "GregPass123!$",
    "hive.sql.table" = "country");

EXPLAIN CBO SELECT COUNT(*) FROM country_2;
SELECT COUNT(*) FROM country_2;
