--!qt:database:derby:qdb
--!qt:database:derby:qdb2
--! qt:dataset:src

CREATE TEMPORARY FUNCTION dboutput AS 'org.apache.hadoop.hive.contrib.genericudf.example.GenericUDFDBOutput';

FROM src

SELECT

dboutput ( '${system:hive.test.database.qdb.jdbc.url}','user1','passwd1',
'CREATE TABLE SIMPLE_DERBY_TABLE1 ("ikey" INTEGER, "bkey" BIGINT, "fkey" REAL, "dkey" DOUBLE)' ),

dboutput ( '${system:hive.test.database.qdb.jdbc.url}','user1','passwd1',
'CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(\'derby.connection.requireAuthentication\', \'true\')' ),

dboutput ( '${system:hive.test.database.qdb.jdbc.url}','user1','passwd1',
'CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(\'derby.authentication.provider\', \'BUILTIN\')' ),

dboutput ( '${system:hive.test.database.qdb.jdbc.url}','user1','passwd1',
'CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(\'derby.user.user1\', \'passwd1\')' ),

dboutput ( '${system:hive.test.database.qdb.jdbc.url}','user1','passwd1',
'CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(\'derby.database.propertiesOnly\', \'true\')' ),

dboutput('${system:hive.test.database.qdb.jdbc.url}','user1','passwd1',
'INSERT INTO SIMPLE_DERBY_TABLE1 ("ikey","bkey","fkey","dkey") VALUES (?,?,?,?)','20','20','20.0','20.0'),

dboutput('${system:hive.test.database.qdb.jdbc.url}','user1','passwd1',
'INSERT INTO SIMPLE_DERBY_TABLE1 ("ikey","bkey","fkey","dkey") VALUES (?,?,?,?)','-20','-20','-20.0','-20.0'),

dboutput('${system:hive.test.database.qdb.jdbc.url}','user1','passwd1',
'INSERT INTO SIMPLE_DERBY_TABLE1 ("ikey","bkey","fkey","dkey") VALUES (?,?,?,?)','100','-15','65.0','-74.0'),

dboutput('${system:hive.test.database.qdb.jdbc.url}','user1','passwd1',
'INSERT INTO SIMPLE_DERBY_TABLE1 ("ikey","bkey","fkey","dkey") VALUES (?,?,?,?)','44','53','-455.454','330.76')

limit 1;

FROM src

SELECT

dboutput ( '${system:hive.test.database.qdb2.jdbc.url}','user2','passwd2',
'CREATE TABLE SIMPLE_DERBY_TABLE2 ("ikey" INTEGER, "bkey" BIGINT, "fkey" REAL, "dkey" DOUBLE )' ),

dboutput ( '${system:hive.test.database.qdb2.jdbc.url}','user2','passwd2',
'CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(\'derby.connection.requireAuthentication\', \'true\')' ),

dboutput ( '${system:hive.test.database.qdb2.jdbc.url}','user2','passwd2',
'CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(\'derby.authentication.provider\', \'BUILTIN\')' ),

dboutput ( '${system:hive.test.database.qdb2.jdbc.url}','user2','passwd2',
'CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(\'derby.user.user2\', \'passwd2\')' ),

dboutput ( '${system:hive.test.database.qdb2.jdbc.url}','user2','passwd2',
'CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(\'derby.database.propertiesOnly\', \'true\')' ),

dboutput('${system:hive.test.database.qdb2.jdbc.url}','user2','passwd2',
'INSERT INTO SIMPLE_DERBY_TABLE2 ("ikey","bkey","fkey","dkey") VALUES (?,?,?,?)','20','20','20.0','20.0'),

dboutput('${system:hive.test.database.qdb2.jdbc.url}','user2','passwd2',
'INSERT INTO SIMPLE_DERBY_TABLE2 ("ikey","bkey","fkey","dkey") VALUES (?,?,?,?)','-20','8','9.0','11.0'),

dboutput('${system:hive.test.database.qdb2.jdbc.url}','user2','passwd2',
'INSERT INTO SIMPLE_DERBY_TABLE2 ("ikey","bkey","fkey","dkey") VALUES (?,?,?,?)','101','-16','66.0','-75.0'),

dboutput('${system:hive.test.database.qdb2.jdbc.url}','user2','passwd2',
'INSERT INTO SIMPLE_DERBY_TABLE2 ("ikey","bkey","fkey","dkey") VALUES (?,?,?,?)','40','50','-455.4543','330.767')

limit 1;


CREATE EXTERNAL TABLE ext_auth1
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
                "hive.sql.jdbc.url" = "${system:hive.test.database.qdb.jdbc.url};collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "user1",
                "hive.sql.dbcp.password.keystore" = "jceks://file/${system:test.tmp.dir}/../../../data/files/test.jceks",
                "hive.sql.dbcp.password.key" = "test_derby_auth1.password",
                "hive.sql.table" = "SIMPLE_DERBY_TABLE1",
                "hive.sql.dbcp.maxActive" = "1"
);


CREATE EXTERNAL TABLE ext_auth2
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
                "hive.sql.jdbc.url" = "${system:hive.test.database.qdb2.jdbc.url};collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "user2",
                "hive.sql.dbcp.password.keystore" = "jceks://file/${system:test.tmp.dir}/../../../data/files/test.jceks",
                "hive.sql.dbcp.password.key" = "test_derby_auth2.password",
                "hive.sql.table" = "SIMPLE_DERBY_TABLE2",
                "hive.sql.dbcp.maxActive" = "1"
);

CREATE TABLE hive_table
(
  ikey int
);

INSERT INTO hive_table VALUES(20);

(SELECT * FROM ext_auth1 JOIN hive_table ON ext_auth1.ikey=hive_table.ikey) UNION ALL (SELECT * FROM ext_auth2 JOIN hive_table ON ext_auth2.ikey=hive_table.ikey);

ALTER TABLE ext_auth1 SET TBLPROPERTIES ("hive.sql.dbcp.password.keystore" = "jceks://file/${system:test.tmp.dir}/../../../data/files/test.jceks");
