--! qt:database:mariadb:q_test_country_table_with_schema.mariadb.sql

-- Create jdbc table with wrong password(hive.sql.dbcp.password)
CREATE EXTERNAL TABLE country_test1 (id int, name varchar(20))
    STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
    TBLPROPERTIES (
        "hive.sql.database.type" = "MYSQL",
        "hive.sql.jdbc.driver" = "org.mariadb.jdbc.Driver",
        "hive.sql.jdbc.url" = "jdbc:mariadb://localhost:3309/bob",
        "hive.sql.dbcp.username" = "root",
        "hive.sql.dbcp.password" = "qtestpassword_wrong",
        "hive.sql.table" = "country");
