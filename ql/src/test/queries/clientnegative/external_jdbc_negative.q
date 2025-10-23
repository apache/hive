--!qt:database:derby:qdb
--! qt:dataset:src

CREATE TEMPORARY FUNCTION dboutput AS 'org.apache.hadoop.hive.contrib.genericudf.example.GenericUDFDBOutput';

FROM src
SELECT
dboutput ('${system:hive.test.database.qdb.jdbc.url}','user1','passwd1',
'CREATE TABLE EXTERNAL_JDBC_NEGATIVE_TABLE1 ("ikey" INTEGER)' ),
dboutput('${system:hive.test.database.qdb.jdbc.url}','user1','passwd1',
'INSERT INTO EXTERNAL_JDBC_NEGATIVE_TABLE1 ("ikey") VALUES (?,?,?,?)','20')
limit 1;

CREATE EXTERNAL TABLE db1_ext_negative1
(
 ikey int,
 bkey bigint
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
                "hive.sql.database.type" = "DERBY",
                "hive.sql.jdbc.driver" = "org.apache.derby.iapi.jdbc.AutoloadedDriver",
                "hive.sql.jdbc.url" = "${system:hive.test.database.qdb.jdbc.url};collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "user1",
                "hive.sql.dbcp.password" = "passwd1",
                "hive.sql.table" = "EXTERNAL_JDBC_NEGATIVE_TABLE1",
                "hive.sql.dbcp.maxActive" = "1"
);

SELECT * FROM db1_ext_negative1;
