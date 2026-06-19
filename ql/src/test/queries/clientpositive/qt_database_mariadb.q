--!qt:database:mariadb:qdb:q_test_country_table.sql
CREATE EXTERNAL TABLE country
(
    id int,
    name varchar(20)
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
    "hive.sql.database.type" = "MYSQL", 
    "hive.sql.jdbc.driver" = "org.mariadb.jdbc.Driver",
    "hive.sql.jdbc.url" = "${system:hive.test.database.qdb.jdbc.url}",
    "hive.sql.dbcp.username" = "${system:hive.test.database.qdb.jdbc.username}",
    "hive.sql.dbcp.password" = "${system:hive.test.database.qdb.jdbc.password}",
    "hive.sql.table" = "country"
    );
-- hive.sql.database.type above is not MARIADB cause at the moment it doesn't exist in org.apache.hive.storage.jdbc.conf.DatabaseType
SELECT * FROM country;
