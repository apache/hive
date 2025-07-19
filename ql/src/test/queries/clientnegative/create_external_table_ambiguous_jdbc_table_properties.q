CREATE EXTERNAL TABLE test_external_table_postgres
(
    id INT,
    name STRING
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
    "hive.sql.database.type" = "POSTGRES",
    "hive.sql.jdbc.driver" = "org.postgresql.Driver",
    "hive.sql.jdbc.url" = "jdbc:postgresql://localhost:5432/test",
    "hive.sql.dbcp.username" = "hiveuser",
    "hive.sql.dbcp.password" = "password",
    "hive.sql.table" = "test",
    "hive.sql.query" = "select id, name from test"
);
