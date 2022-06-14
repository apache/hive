--! qt:database:mariadb:q_test_country_table_with_schema.mariadb.sql

-- Create jdbc table with wrong MySQL url(hive.sql.jdbc.url)
CREATE EXTERNAL TABLE country_test3 (id int, name varchar(20))
    STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
    TBLPROPERTIES (
        "hive.sql.database.type" = "MYSQL",
        "hive.sql.jdbc.driver" = "org.mariadb.jdbc.Driver",
        "hive.sql.jdbc.url" = "jdbc:mariadb://localhost_no_exists:3309/bob",
        "hive.sql.dbcp.username" = "root",
        "hive.sql.dbcp.password" = "qtestpassword",
        "hive.sql.table" = "country");
