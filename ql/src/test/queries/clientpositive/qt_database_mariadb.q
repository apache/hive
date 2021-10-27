--!qt:database:mariadb:q_test_country_table.sql
CREATE EXTERNAL TABLE country
(
    id int,
    name varchar(20)
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
    "hive.sql.database.type" = "MYSQL", 
    "hive.sql.jdbc.driver" = "org.mariadb.jdbc.Driver",
    "hive.sql.jdbc.url" = "jdbc:mariadb://localhost:3309/qtestDB",
    "hive.sql.dbcp.username" = "root",
    "hive.sql.dbcp.password" = "qtestpassword",
    "hive.sql.table" = "country"
    );
-- hive.sql.database.type above is not MARIADB cause at the moment it doesn't exist in org.apache.hive.storage.jdbc.conf.DatabaseType
SELECT * FROM country;
