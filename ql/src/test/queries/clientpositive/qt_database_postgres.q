--!qt:database:postgres:qdb:q_test_country_table.sql
CREATE EXTERNAL TABLE country
(
    id int,
    name varchar(20)
)
    STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
    TBLPROPERTIES (
        "hive.sql.database.type" = "POSTGRES",
        "hive.sql.jdbc.driver" = "org.postgresql.Driver",
        "hive.sql.jdbc.url" = "${system:hive.test.database.qdb.jdbc.url}",
        "hive.sql.dbcp.username" = "${system:hive.test.database.qdb.jdbc.username}",
        "hive.sql.dbcp.password" = "${system:hive.test.database.qdb.jdbc.password}",
        "hive.sql.table" = "country"
        );
SELECT * FROM country;
