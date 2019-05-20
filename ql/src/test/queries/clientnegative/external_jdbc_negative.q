--! qt:dataset:src

CREATE TEMPORARY FUNCTION dboutput AS 'org.apache.hadoop.hive.contrib.genericudf.example.GenericUDFDBOutput';

FROM src
SELECT
dboutput ('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_negative;create=true','user1','passwd1',
'CREATE TABLE EXTERNAL_JDBC_NEGATIVE_TABLE1 ("ikey" INTEGER)' ),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_negative','user1','passwd1',
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
                "hive.sql.jdbc.driver" = "org.apache.derby.jdbc.EmbeddedDriver",
                "hive.sql.jdbc.url" = "jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_negative;collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "user1",
                "hive.sql.dbcp.password" = "passwd1",
                "hive.sql.table" = "EXTERNAL_JDBC_NEGATIVE_TABLE1",
                "hive.sql.dbcp.maxActive" = "1"
);

SELECT * FROM db1_ext_negative1;
