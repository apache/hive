--! qt:database:mariadb:qdb:q_test_case_sensitive_country_table.mariadb.sql

CREATE EXTERNAL TABLE country_lower (id int, name varchar(20))
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
    "hive.sql.database.type" = "MYSQL",
    "hive.sql.jdbc.driver" = "org.mariadb.jdbc.Driver",
    "hive.sql.jdbc.url" = "jdbc:mariadb://${system:hive.test.database.qdb.host}:${system:hive.test.database.qdb.port}/bob",
    "hive.sql.dbcp.username" = "${system:hive.test.database.qdb.jdbc.username}",
    "hive.sql.dbcp.password" = "${system:hive.test.database.qdb.jdbc.password}",
    "hive.sql.table" = "country");

EXPLAIN CBO SELECT COUNT(*) FROM country_lower;
SELECT COUNT(*) FROM country_lower;

-- The back-tick quoted mixed-case table must resolve to `Country` (2 rows).
CREATE EXTERNAL TABLE country_mixed (id int, name varchar(20))
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
    "hive.sql.database.type" = "MYSQL",
    "hive.sql.jdbc.driver" = "org.mariadb.jdbc.Driver",
    "hive.sql.jdbc.url" = "jdbc:mariadb://${system:hive.test.database.qdb.host}:${system:hive.test.database.qdb.port}/bob",
    "hive.sql.dbcp.username" = "${system:hive.test.database.qdb.jdbc.username}",
    "hive.sql.dbcp.password" = "${system:hive.test.database.qdb.jdbc.password}",
    "hive.sql.table" = "`Country`");

EXPLAIN CBO SELECT COUNT(*) FROM country_mixed;
SELECT COUNT(*) FROM country_mixed;
SELECT * FROM country_mixed ORDER BY id;

