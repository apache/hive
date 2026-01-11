--! qt:database:mariadb:qdb:q_test_case_sensitive.mariadb.sql

-- ==============================================================
-- Test with AUTHORIZATION ENABLED
-- ==============================================================

set hive.test.authz.sstd.hs2.mode=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.security.authorization.enabled=true;

-- Test Case-Sensitive Schema and Table
CREATE EXTERNAL TABLE country_test (id int, name varchar(20))
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
    "hive.sql.database.type" = "MYSQL",
    "hive.sql.jdbc.driver" = "org.mariadb.jdbc.Driver",
    "hive.sql.jdbc.url" = "jdbc:mariadb://${system:hive.test.database.qdb.host}:${system:hive.test.database.qdb.port}/WorldData",
    "hive.sql.dbcp.username" = "${system:hive.test.database.qdb.jdbc.username}",
    "hive.sql.dbcp.password" = "${system:hive.test.database.qdb.jdbc.password}",
    "hive.sql.schema" = "\"WorldData\"",
    "hive.sql.table" = "\"Country\""
);
SELECT * FROM country_test;


-- Test Case-Sensitive Partition Column
CREATE EXTERNAL TABLE cities_test (id int, name varchar(20), regionid int)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
    "hive.sql.database.type" = "MYSQL",
    "hive.sql.jdbc.driver" = "org.mariadb.jdbc.Driver",
    "hive.sql.jdbc.url" = "jdbc:mariadb://${system:hive.test.database.qdb.host}:${system:hive.test.database.qdb.port}/WorldData",
    "hive.sql.dbcp.username" = "${system:hive.test.database.qdb.jdbc.username}",
    "hive.sql.dbcp.password" = "${system:hive.test.database.qdb.jdbc.password}",
    "hive.sql.schema" = "\"WorldData\"",
    "hive.sql.table" = "\"Cities\"",
    "hive.sql.partitionColumn" = "RegionID",
    "hive.sql.numPartitions" = "2"
);
SELECT * FROM cities_test where regionid >= 20;


-- Test Case-Sensitive Query Field Names
CREATE EXTERNAL TABLE geography_test (id int, description varchar(50))
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
    "hive.sql.database.type" = "MYSQL",
    "hive.sql.jdbc.driver" = "org.mariadb.jdbc.Driver",
    "hive.sql.jdbc.url" = "jdbc:mariadb://${system:hive.test.database.qdb.host}:${system:hive.test.database.qdb.port}/WorldData",
    "hive.sql.dbcp.username" = "${system:hive.test.database.qdb.jdbc.username}",
    "hive.sql.dbcp.password" = "${system:hive.test.database.qdb.jdbc.password}",
    "hive.sql.query" = "SELECT id, `Description` FROM `WorldData`.`Geography`"
);
SELECT * FROM geography_test;

-- ==============================================================
-- Test with AUTHORIZATION DISABLED
-- ==============================================================

set hive.security.authorization.enabled=false;

SELECT * FROM country_test;

SELECT * FROM cities_test where regionid >= 20;

SELECT * FROM geography_test;


-- Cleanup
DROP TABLE country_test;
DROP TABLE cities_test;
DROP TABLE geography_test;
