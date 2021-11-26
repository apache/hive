--!qt:database:postgres:q_test_country_table.sql
CREATE EXTERNAL TABLE country
(
    id int,
    name varchar(20)
)
    STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
    TBLPROPERTIES (
        "hive.sql.database.type" = "POSTGRES",
        "hive.sql.jdbc.driver" = "org.postgresql.Driver",
        "hive.sql.jdbc.url" = "jdbc:postgresql://localhost:5432/qtestDB",
        "hive.sql.dbcp.username" = "qtestuser",
        "hive.sql.dbcp.password" = "qtestpassword",
        "hive.sql.table" = "country"
        );
SELECT * FROM country;
