--! qt:database:oracle:q_test_country_table_with_schema.oracle.sql
-- Oracle does not allow explicitly the creation of different namespaces/schemas in the same database. This can be
-- achieved by creating different users where each user is associated with a schema having the same name.

-- Some JDBC APIs require the catalog, schema, and table names to be passed exactly as they are stored in the database.
-- Oracle stores unquoted identifiers by first converting them to uppercase thus the hive.sql.schema and hive.sql.table
-- properties below are specified in uppercase.

-- Accessing table in the same namespace/schema with the user with explicit schema declaration
CREATE EXTERNAL TABLE country_0 (id int, name varchar(20))
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
    "hive.sql.database.type" = "ORACLE",
    "hive.sql.jdbc.driver" = "oracle.jdbc.OracleDriver",
    "hive.sql.jdbc.url" = "jdbc:oracle:thin:@//localhost:1521/XEPDB1",
    "hive.sql.dbcp.username" = "bob",
    "hive.sql.dbcp.password" = "bobpass",
    "hive.sql.schema" = "BOB",
    "hive.sql.table" = "COUNTRY"
    ); 
EXPLAIN CBO SELECT COUNT(*) FROM country_0;
SELECT COUNT(*) FROM country_0;
    
-- Accessing table in the same namespace/schema with the user without explicit schema declaration; schema inferred
-- automatically by Oracle.
CREATE EXTERNAL TABLE country_1 (id int, name varchar(20))
    STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
    TBLPROPERTIES (
        "hive.sql.database.type" = "ORACLE",
        "hive.sql.jdbc.driver" = "oracle.jdbc.OracleDriver",
        "hive.sql.jdbc.url" = "jdbc:oracle:thin:@//localhost:1521/XEPDB1",
        "hive.sql.dbcp.username" = "bob",
        "hive.sql.dbcp.password" = "bobpass",
        "hive.sql.table" = "COUNTRY"
        );
EXPLAIN CBO SELECT COUNT(*) FROM country_1;
SELECT COUNT(*) FROM country_1;

-- Accessing table in a different namespace/schema with the user; schema declaration is mandatory.
-- User must have the necessary permissions to access another schema 
CREATE EXTERNAL TABLE country_2 (id int, name varchar(20))
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
    "hive.sql.database.type" = "ORACLE",
    "hive.sql.jdbc.driver" = "oracle.jdbc.OracleDriver",
    "hive.sql.jdbc.url" = "jdbc:oracle:thin:@//localhost:1521/XEPDB1",
    "hive.sql.dbcp.username" = "bob",
    "hive.sql.dbcp.password" = "bobpass",
    "hive.sql.schema" = "ALICE",
    "hive.sql.table" = "COUNTRY"
    );
EXPLAIN CBO SELECT COUNT(*) FROM country_2;
SELECT COUNT(*) FROM country_2;

-- Test DML statements are working fine when accessing table in non-default schema
INSERT INTO country_2 VALUES (8, 'Hungary');
SELECT * FROM country_2 ORDER BY id;
