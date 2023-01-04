--!qt:database:mssql:q_test_country_table.sql
CREATE EXTERNAL TABLE country
(
    id int,
    name varchar(20)
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
    "hive.sql.database.type" = "MSSQL", 
    "hive.sql.jdbc.driver" = "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "hive.sql.jdbc.url" = "jdbc:sqlserver://localhost:1433",
    "hive.sql.dbcp.username" = "sa",
    "hive.sql.dbcp.password" = "Its-a-s3cret",
    "hive.sql.table" = "country"
    );
SELECT * FROM country;
