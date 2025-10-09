--! qt:dataset:src

-- SORT_QUERY_RESULTS

set hive.jdbc.pushdown.safe.enable=true;


CREATE TEMPORARY FUNCTION dboutput AS 'org.apache.hadoop.hive.contrib.genericudf.example.GenericUDFDBOutput';

FROM src
SELECT
dboutput ('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_auth3_1;create=true','user1','passwd1',
'CREATE TABLE EXTERNAL_JDBC_SIMPLE_DERBY2_TABLE1 ("ikey" INTEGER, "bkey" BIGINT, "fkey" REAL, "dkey" DOUBLE)' ),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_auth3_1','user1','passwd1',
'INSERT INTO EXTERNAL_JDBC_SIMPLE_DERBY2_TABLE1 ("ikey","bkey","fkey","dkey") VALUES (?,?,?,?)','20','20','20.0','20.0'),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_auth3_1','user1','passwd1',
'INSERT INTO EXTERNAL_JDBC_SIMPLE_DERBY2_TABLE1 ("ikey","bkey","fkey","dkey") VALUES (?,?,?,?)','-20','-20','-20.0','-20.0'),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_auth3_1','user1','passwd1',
'INSERT INTO EXTERNAL_JDBC_SIMPLE_DERBY2_TABLE1 ("ikey","bkey","fkey","dkey") VALUES (?,?,?,?)','100','-15','65.0','-74.0'),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_auth3_1','user1','passwd1',
'INSERT INTO EXTERNAL_JDBC_SIMPLE_DERBY2_TABLE1 ("ikey","bkey","fkey","dkey") VALUES (?,?,?,?)','44','53','-455.454','330.76')
limit 1;

FROM src
SELECT
dboutput ('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_auth3_1','user1','passwd1',
'CREATE TABLE EXTERNAL_JDBC_SIMPLE_DERBY2_TABLE2 ("ikey" INTEGER, "bkey" BIGINT, "fkey" REAL, "dkey" DOUBLE )' ),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_auth3_1','user1','passwd1',
'INSERT INTO EXTERNAL_JDBC_SIMPLE_DERBY2_TABLE2 ("ikey","bkey","fkey","dkey") VALUES (?,?,?,?)','20','20','20.0','20.0'),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_auth3_1','user1','passwd1',
'INSERT INTO EXTERNAL_JDBC_SIMPLE_DERBY2_TABLE2 ("ikey","bkey","fkey","dkey") VALUES (?,?,?,?)','-20','8','9.0','11.0'),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_auth3_1','user1','passwd1',
'INSERT INTO EXTERNAL_JDBC_SIMPLE_DERBY2_TABLE2 ("ikey","bkey","fkey","dkey") VALUES (?,?,?,?)','101','-16','66.0','-75.0'),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_auth3_1','user1','passwd1',
'INSERT INTO EXTERNAL_JDBC_SIMPLE_DERBY2_TABLE2 ("ikey","bkey","fkey","dkey") VALUES (?,?,?,?)','40','50','-455.4543','330.767')
limit 1;



CREATE EXTERNAL TABLE db1_ext_auth3_1
(
 ikey int,
 bkey bigint,
 fkey float,
 dkey double
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
                "hive.sql.database.type" = "DERBY",
                "hive.sql.jdbc.driver" = "org.apache.derby.jdbc.EmbeddedDriver",
                "hive.sql.jdbc.url" = "jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_auth3_1;collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "user1",
                "hive.sql.dbcp.password" = "passwd1",
                "hive.sql.table" = "EXTERNAL_JDBC_SIMPLE_DERBY2_TABLE1",
                "hive.sql.dbcp.maxActive" = "1"
);

CREATE EXTERNAL TABLE db1_ext_auth3_2
(
 ikey int,
 bkey bigint,
 fkey float,
 dkey double
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
                "hive.sql.database.type" = "DERBY",
                "hive.sql.jdbc.driver" = "org.apache.derby.jdbc.EmbeddedDriver",
                "hive.sql.jdbc.url" = "jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_auth3_1;collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "user1",
                "hive.sql.dbcp.password" = "passwd1",
                "hive.sql.table" = "EXTERNAL_JDBC_SIMPLE_DERBY2_TABLE2",
                "hive.sql.dbcp.maxActive" = "1"
);


SELECT * FROM db1_ext_auth3_1;

SELECT * FROM db1_ext_auth3_2;

EXPLAIN
SELECT db1_ext_auth3_1.fkey, db1_ext_auth3_2.bkey FROM db1_ext_auth3_1 JOIN db1_ext_auth3_2 WHERE db1_ext_auth3_1.ikey = db1_ext_auth3_2.ikey AND db1_ext_auth3_1.ikey > 20;

SELECT db1_ext_auth3_1.fkey, db1_ext_auth3_2.bkey FROM db1_ext_auth3_1 JOIN db1_ext_auth3_2 WHERE db1_ext_auth3_1.ikey = db1_ext_auth3_2.ikey AND db1_ext_auth3_1.ikey > 20;

EXPLAIN
SELECT bkey, fkey FROM db1_ext_auth3_1 WHERE ikey > 20 UNION ALL SELECT bkey, fkey FROM db1_ext_auth3_2 WHERE ikey > 100;

SELECT bkey, fkey FROM db1_ext_auth3_1 WHERE ikey > 20 UNION ALL SELECT bkey, fkey FROM db1_ext_auth3_2 WHERE ikey > 100;

DROP TABLE db1_ext_auth3_1;
DROP TABLE db1_ext_auth3_2;

FROM src
SELECT
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_auth3_1','user1','passwd1',
'DROP TABLE EXTERNAL_JDBC_SIMPLE_DERBY2_TABLE1' ),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_auth3_1','user1','passwd1',
'DROP TABLE EXTERNAL_JDBC_SIMPLE_DERBY2_TABLE2' )
limit 1;
