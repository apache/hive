--! qt:database:mariadb:q_test_country_table_with_schema.mariadb.sql

-- In MariaDB (and MySQL) CREATE SCHEMA is a synonym to CREATE DATABASE so the use of hive.sql.schema is not required.
-- A MariaDB table can be uniquely identified by including the database/schema name in the JDBC URL and specifying the
-- hive.sql.table property.  

-- Connecting to MariaDB without specifying a database name (e.g., jdbc:mariadb://localhost:3309/) may create problems
-- when a table with the same name exists in multiple databases. The problem could be avoided by setting the
-- hive.sql.schema property but unfortunately the JDBC driver of MariaDB ignores schema information.

-- Some JDBC APIs require the catalog, schema, and table names to be passed exactly as they are stored in the database.
-- MariaDB stores unquoted identifiers by first converting them to lowercase thus the hive.sql.schema and
-- hive.sql.table properties below are specified in lowercase.

-- The hive.sql.schema property is optional; bob schema is inferred from the JDBC URL 
CREATE EXTERNAL TABLE country_0 (id int, name varchar(20))
    STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
    TBLPROPERTIES (
        "hive.sql.database.type" = "MYSQL",
        "hive.sql.jdbc.driver" = "org.mariadb.jdbc.Driver",
        "hive.sql.jdbc.url" = "jdbc:mariadb://localhost:3309/bob",
        "hive.sql.dbcp.username" = "root",
        "hive.sql.dbcp.password" = "qtestpassword",
        "hive.sql.table" = "country");

EXPLAIN CBO SELECT COUNT(*) FROM country_0;
SELECT COUNT(*) FROM country_0;

-- The hive.sql.schema property can be specified with the same value as the database name in the URL but does not
-- provide any additional benefits. 
CREATE EXTERNAL TABLE country_1 (id int, name varchar(20))
    STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
    TBLPROPERTIES (
        "hive.sql.database.type" = "MYSQL",
        "hive.sql.jdbc.driver" = "org.mariadb.jdbc.Driver",
        "hive.sql.jdbc.url" = "jdbc:mariadb://localhost:3309/bob",
        "hive.sql.dbcp.username" = "root",
        "hive.sql.dbcp.password" = "qtestpassword",
        "hive.sql.schema" = "bob",
        "hive.sql.table" = "country");

EXPLAIN CBO SELECT COUNT(*) FROM country_1;
SELECT COUNT(*) FROM country_1;

CREATE EXTERNAL TABLE country_2 (id int, name varchar(20))
    STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
    TBLPROPERTIES (
        "hive.sql.database.type" = "MYSQL",
        "hive.sql.jdbc.driver" = "org.mariadb.jdbc.Driver",
        "hive.sql.jdbc.url" = "jdbc:mariadb://localhost:3309/alice",
        "hive.sql.dbcp.username" = "root",
        "hive.sql.dbcp.password" = "qtestpassword",
        "hive.sql.table" = "country");

EXPLAIN CBO SELECT COUNT(*) FROM country_2;
SELECT COUNT(*) FROM country_2;

-- It is possible to have the JDBC URL and hive.sql.schema pointing to different databases/schemas but it is confusing
-- and leads to the same result which could be achieved by using exclusively the URL.
CREATE EXTERNAL TABLE country_3 (id int, name varchar(20))
    STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
    TBLPROPERTIES (
        "hive.sql.database.type" = "MYSQL",
        "hive.sql.jdbc.driver" = "org.mariadb.jdbc.Driver",
        "hive.sql.jdbc.url" = "jdbc:mariadb://localhost:3309/bob",
        "hive.sql.dbcp.username" = "root",
        "hive.sql.dbcp.password" = "qtestpassword",
        "hive.sql.schema" = "alice",
        "hive.sql.table" = "country");

EXPLAIN CBO SELECT COUNT(*) FROM country_3;
SELECT COUNT(*) FROM country_3;
