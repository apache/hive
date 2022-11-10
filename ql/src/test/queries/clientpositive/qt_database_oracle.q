--!qt:database:oracle:q_test_country_table.sql
CREATE EXTERNAL TABLE country
(
    id int,
    name varchar(20)
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
    "hive.sql.database.type" = "ORACLE", 
    "hive.sql.jdbc.driver" = "oracle.jdbc.OracleDriver",
    "hive.sql.jdbc.url" = "jdbc:oracle:thin:@//localhost:1521/xe",
    "hive.sql.dbcp.username" = "SYS as SYSDBA",
    "hive.sql.dbcp.password" = "oracle",
    "hive.sql.table" = "COUNTRY"
);
-- Case sensitivity is important in the default conf of Oracle. Notice the capital letters in hive.sql.table 
SELECT * FROM country;
