--! qt:dataset:src

-- SORT_QUERY_RESULTS

CREATE TEMPORARY FUNCTION dboutput AS 'org.apache.hadoop.hive.contrib.genericudf.example.GenericUDFDBOutput';

FROM src
SELECT
dboutput ('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_auth1;create=true','user1','passwd1',
'CREATE TABLE EXTERNAL_JDBC_SIMPLE_DERBY2_TABLE1 ("IKEY" INTEGER, "bkey" BIGINT, "fkey" REAL, "dkey" DOUBLE)' ),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_auth1','user1','passwd1',
'INSERT INTO EXTERNAL_JDBC_SIMPLE_DERBY2_TABLE1 ("IKEY","bkey","fkey","dkey") VALUES (?,?,?,?)','20','20','20.0','20.0'),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_auth1','user1','passwd1',
'INSERT INTO EXTERNAL_JDBC_SIMPLE_DERBY2_TABLE1 ("IKEY","bkey","fkey","dkey") VALUES (?,?,?,?)','-20','-20','-20.0','-20.0'),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_auth1','user1','passwd1',
'INSERT INTO EXTERNAL_JDBC_SIMPLE_DERBY2_TABLE1 ("IKEY","bkey","fkey","dkey") VALUES (?,?,?,?)','100','-15','65.0','-74.0'),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_auth1','user1','passwd1',
'INSERT INTO EXTERNAL_JDBC_SIMPLE_DERBY2_TABLE1 ("IKEY","bkey","fkey","dkey") VALUES (?,?,?,?)','44','53','-455.454','330.76')
limit 1;

FROM src
SELECT
dboutput ('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_auth2;create=true','user2','passwd2',
'CREATE TABLE EXTERNAL_JDBC_SIMPLE_DERBY2_TABLE2 ("ikey" INTEGER, "BKEY" BIGINT, "fkey" REAL, "dkey" DOUBLE )' ),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_auth2','user2','passwd2',
'INSERT INTO EXTERNAL_JDBC_SIMPLE_DERBY2_TABLE2 ("ikey","BKEY","fkey","dkey") VALUES (?,?,?,?)','20','20','20.0','20.0'),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_auth2','user2','passwd2',
'INSERT INTO EXTERNAL_JDBC_SIMPLE_DERBY2_TABLE2 ("ikey","BKEY","fkey","dkey") VALUES (?,?,?,?)','-20','8','9.0','11.0'),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_auth2','user2','passwd2',
'INSERT INTO EXTERNAL_JDBC_SIMPLE_DERBY2_TABLE2 ("ikey","BKEY","fkey","dkey") VALUES (?,?,?,?)','101','-16','66.0','-75.0'),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_auth2','user2','passwd2',
'INSERT INTO EXTERNAL_JDBC_SIMPLE_DERBY2_TABLE2 ("ikey","BKEY","fkey","dkey") VALUES (?,?,?,?)','40','50','-455.4543','330.767')
limit 1;

FROM src
SELECT
dboutput ('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_auth1','user1','passwd1',
'CREATE TABLE EXTERNAL_JDBC_SIMPLE_DERBY2_TABLE2 ("ikey" INTEGER, "bkey" BIGINT, "FKEY" REAL, "DKEY" DOUBLE )' ),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_auth1','user1','passwd1',
'INSERT INTO EXTERNAL_JDBC_SIMPLE_DERBY2_TABLE2 ("ikey","bkey","FKEY","DKEY") VALUES (?,?,?,?)','20','20','20.0','20.0'),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_auth1','user1','passwd1',
'INSERT INTO EXTERNAL_JDBC_SIMPLE_DERBY2_TABLE2 ("ikey","bkey","FKEY","DKEY") VALUES (?,?,?,?)','-20','8','9.0','11.0'),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_auth1','user1','passwd1',
'INSERT INTO EXTERNAL_JDBC_SIMPLE_DERBY2_TABLE2 ("ikey","bkey","FKEY","DKEY") VALUES (?,?,?,?)','101','-16','66.0','-75.0'),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_auth1','user1','passwd1',
'INSERT INTO EXTERNAL_JDBC_SIMPLE_DERBY2_TABLE2 ("ikey","bkey","FKEY","DKEY") VALUES (?,?,?,?)','40','50','-455.4543','330.767')
limit 1;



CREATE EXTERNAL TABLE db1_ext_auth1
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
                "hive.sql.jdbc.url" = "jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_auth1;collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "user1",
                "hive.sql.dbcp.password" = "passwd1",
                "hive.sql.table" = "EXTERNAL_JDBC_SIMPLE_DERBY2_TABLE1",
                "hive.sql.dbcp.maxActive" = "1"
);

CREATE EXTERNAL TABLE db2_ext_auth2
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
                "hive.sql.jdbc.url" = "jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_auth2;collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "user2",
                "hive.sql.dbcp.password" = "passwd2",
                "hive.sql.table" = "EXTERNAL_JDBC_SIMPLE_DERBY2_TABLE2",
                "hive.sql.dbcp.maxActive" = "1"
);

CREATE EXTERNAL TABLE db1_ext_auth2
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
                "hive.sql.jdbc.url" = "jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_auth1;collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "user1",
                "hive.sql.dbcp.password" = "passwd1",
                "hive.sql.table" = "EXTERNAL_JDBC_SIMPLE_DERBY2_TABLE2",
                "hive.sql.dbcp.maxActive" = "1"
);


SELECT * FROM db1_ext_auth1;

SELECT * FROM db2_ext_auth2;

SELECT * FROM db1_ext_auth2;

EXPLAIN
SELECT * FROM db1_ext_auth1 JOIN db2_ext_auth2 ON db1_ext_auth1.ikey = db2_ext_auth2.ikey;

SELECT * FROM db1_ext_auth1 JOIN db2_ext_auth2 ON db1_ext_auth1.ikey = db2_ext_auth2.ikey;

EXPLAIN
SELECT * FROM db1_ext_auth1 JOIN db1_ext_auth2 ON db1_ext_auth1.ikey = db1_ext_auth2.ikey;

SELECT * FROM db1_ext_auth1 JOIN db1_ext_auth2 ON db1_ext_auth1.ikey = db1_ext_auth2.ikey;

EXPLAIN
SELECT db1_ext_auth1.ikey, b.ikey * 2 FROM db1_ext_auth1 JOIN (SELECT * FROM db1_ext_auth1) b;

SELECT db1_ext_auth1.ikey, b.ikey * 2 FROM db1_ext_auth1 JOIN (SELECT * FROM db1_ext_auth1) b;

EXPLAIN
SELECT db1_ext_auth1.ikey FROM db1_ext_auth1 LIMIT 10;

SELECT db1_ext_auth1.ikey FROM db1_ext_auth1 LIMIT 10;

-- Set CBO off to test column names.
set hive.cbo.enable=false;

SELECT * FROM db1_ext_auth1;

SELECT * FROM db2_ext_auth2;

SELECT * FROM db1_ext_auth2;

set hive.cbo.enable=true;

DROP TABLE db1_ext_auth1;
DROP TABLE db2_ext_auth2;
DROP TABLE db1_ext_auth2;

FROM src
SELECT
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_auth1','user1','passwd1',
'DROP TABLE EXTERNAL_JDBC_SIMPLE_DERBY2_TABLE1' ),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_auth1','user1','passwd1',
'DROP TABLE EXTERNAL_JDBC_SIMPLE_DERBY2_TABLE2' ),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_auth2','user2','passwd2',
'DROP TABLE EXTERNAL_JDBC_SIMPLE_DERBY2_TABLE2' )
limit 1;
