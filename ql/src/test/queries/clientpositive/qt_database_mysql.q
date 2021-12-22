--!qt:database:mysql:q_test_country_table.sql
CREATE EXTERNAL TABLE country
(
    id int,
    name varchar(20)
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
    "hive.sql.database.type" = "MYSQL",
    "hive.sql.jdbc.driver" = "com.mysql.jdbc.Driver",
    "hive.sql.jdbc.url" = "jdbc:mysql://localhost:3306/qtestDB",
    "hive.sql.dbcp.username" = "root",
    "hive.sql.dbcp.password" = "qtestpassword",
    "hive.sql.table" = "country"
    );
SELECT * FROM country;
