--! qt:dataset:src

CREATE TEMPORARY FUNCTION dboutput AS 'org.apache.hadoop.hive.contrib.genericudf.example.GenericUDFDBOutput';

-- convert varchar to numeric/decimal/date/timestamp
FROM src
SELECT
dboutput ('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby2;create=true','user','passwd',
'CREATE TABLE EXTERNAL_JDBC_TYPE_CONVERSION_TABLE1 ("ikey" VARCHAR(20), "bkey" VARCHAR(20), "fkey" VARCHAR(20), "dkey" VARCHAR(20), "chkey" VARCHAR(20), "dekey" VARCHAR(20), "dtkey" VARCHAR(20), "tkey" VARCHAR(50))' ),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby2','user','passwd',
'INSERT INTO EXTERNAL_JDBC_TYPE_CONVERSION_TABLE1 ("ikey","bkey","fkey","dkey","chkey","dekey","dtkey","tkey") VALUES (?,?,?,?,?,?,?,?)','1','1000','20.0','40.0','aaa','3.1415','2010-01-01','2018-01-01 12:00:00.000000000'),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby2','user','passwd',
'INSERT INTO EXTERNAL_JDBC_TYPE_CONVERSION_TABLE1 ("ikey","bkey","fkey","dkey","chkey","dekey","dtkey","tkey") VALUES (?,?,?,?,?,?,?,?)','5','9000',null,'10.0','bbb','2.7182','2018-01-01','2010-06-01 14:00:00.000000000'),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby2','user','passwd',
'INSERT INTO EXTERNAL_JDBC_TYPE_CONVERSION_TABLE1 ("ikey","bkey","fkey","dkey","chkey","dekey","dtkey","tkey") VALUES (?,?,?,?,?,?,?,?)','3','4000','120.0','25.4','hello','2.7182','2017-06-05','2011-11-10 18:00:08.000000000'),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby2','user','passwd',
'INSERT INTO EXTERNAL_JDBC_TYPE_CONVERSION_TABLE1 ("ikey","bkey","fkey","dkey","chkey","dekey","dtkey","tkey") VALUES (?,?,?,?,?,?,?,?)','8','3000','180.0','35.8','world','3.1415','2014-03-03','2016-07-04 13:00:00.000000000'),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby2','user','passwd',
'INSERT INTO EXTERNAL_JDBC_TYPE_CONVERSION_TABLE1 ("ikey","bkey","fkey","dkey","chkey","dekey","dtkey","tkey") VALUES (?,?,?,?,?,?,?,?)','4','8000','120.4','31.3','ccc',null,'2014-03-04','2018-07-08 11:00:00.000000000')
limit 1;

CREATE EXTERNAL TABLE jdbc_type_conversion_table1
(
 ikey int,
 bkey bigint,
 fkey float,
 dkey double,
 chkey string,
 dekey decimal(5,3),
 dtkey date,
 tkey timestamp
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
                "hive.sql.database.type" = "DERBY",
                "hive.sql.jdbc.driver" = "org.apache.derby.jdbc.EmbeddedDriver",
                "hive.sql.jdbc.url" = "jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby2;collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "user",
                "hive.sql.dbcp.password" = "passwd",
                "hive.sql.table" = "EXTERNAL_JDBC_TYPE_CONVERSION_TABLE1",
                "hive.sql.dbcp.maxActive" = "1"
);

SELECT * FROM jdbc_type_conversion_table1;

-- convert numeric/decimal/date/timestamp to varchar

FROM src
SELECT
dboutput ('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby2;create=true','user','passwd',
'CREATE TABLE EXTERNAL_JDBC_TYPE_CONVERSION_TABLE2 ("ikey" INTEGER, "bkey" BIGINT, "fkey" REAL, "dkey" DOUBLE, "chkey" VARCHAR(20), "dekey" DECIMAL(6,4), "dtkey" DATE, "tkey" TIMESTAMP)' ),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby2','user','passwd',
'INSERT INTO EXTERNAL_JDBC_TYPE_CONVERSION_TABLE2 ("ikey","bkey","fkey","dkey","chkey","dekey","dtkey","tkey") VALUES (?,?,?,?,?,?,?,?)','1','1000','20.0','40.0','aaa','3.1415','2010-01-01','2018-01-01 12:00:00.000000000'),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby2','user','passwd',
'INSERT INTO EXTERNAL_JDBC_TYPE_CONVERSION_TABLE2 ("ikey","bkey","fkey","dkey","chkey","dekey","dtkey","tkey") VALUES (?,?,?,?,?,?,?,?)','5','9000',null,'10.0','bbb','2.7182','2018-01-01','2010-06-01 14:00:00.000000000'),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby2','user','passwd',
'INSERT INTO EXTERNAL_JDBC_TYPE_CONVERSION_TABLE2 ("ikey","bkey","fkey","dkey","chkey","dekey","dtkey","tkey") VALUES (?,?,?,?,?,?,?,?)','3','4000','120.0','25.4','hello','2.7182','2017-06-05','2011-11-10 18:00:08.000000000'),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby2','user','passwd',
'INSERT INTO EXTERNAL_JDBC_TYPE_CONVERSION_TABLE2 ("ikey","bkey","fkey","dkey","chkey","dekey","dtkey","tkey") VALUES (?,?,?,?,?,?,?,?)','8','3000','180.0','35.8','world','3.1415','2014-03-03','2016-07-04 13:00:00.000000000'),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby2','user','passwd',
'INSERT INTO EXTERNAL_JDBC_TYPE_CONVERSION_TABLE2 ("ikey","bkey","fkey","dkey","chkey","dekey","dtkey","tkey") VALUES (?,?,?,?,?,?,?,?)','4','8000','120.4','31.3','ccc',null,'2014-03-04','2018-07-08 11:00:00.000000000')
limit 1;

CREATE EXTERNAL TABLE jdbc_type_conversion_table2
(
 ikey string,
 bkey string,
 fkey string,
 dkey string,
 chkey string,
 dekey string,
 dtkey string,
 tkey string 
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
                "hive.sql.database.type" = "DERBY",
                "hive.sql.jdbc.driver" = "org.apache.derby.jdbc.EmbeddedDriver",
                "hive.sql.jdbc.url" = "jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby2;collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "user",
                "hive.sql.dbcp.password" = "passwd",
                "hive.sql.table" = "EXTERNAL_JDBC_TYPE_CONVERSION_TABLE2",
                "hive.sql.dbcp.maxActive" = "1"
);

SELECT * FROM jdbc_type_conversion_table2;

FROM src
SELECT
dboutput ('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby2;create=true','user','passwd',
'CREATE TABLE EXTERNAL_JDBC_TYPE_CONVERSION_TABLE3 ("ikey" INTEGER, "bkey" BIGINT, "fkey" REAL, "dkey" DOUBLE, "chkey" VARCHAR(20), "dekey" DECIMAL(6,4), "dtkey" DATE, "tkey" TIMESTAMP)' ),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby2','user','passwd',
'INSERT INTO EXTERNAL_JDBC_TYPE_CONVERSION_TABLE3 ("ikey","bkey","fkey","dkey","chkey","dekey","dtkey","tkey") VALUES (?,?,?,?,?,?,?,?)','1','1000','20.0','40.0','aaa','3.1415','2010-01-01','2018-01-01 12:00:00.000000000')
limit 1;

-- convert between numeric types
CREATE EXTERNAL TABLE jdbc_type_conversion_table3
(
 ikey double,
 bkey decimal(5,1),
 fkey int,
 dkey int,
 chkey double,
 dekey decimal(6,4),
 dtkey decimal(16,2),
 tkey decimal(16,2)
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
                "hive.sql.database.type" = "DERBY",
                "hive.sql.jdbc.driver" = "org.apache.derby.jdbc.EmbeddedDriver",
                "hive.sql.jdbc.url" = "jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby2;collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "user",
                "hive.sql.dbcp.password" = "passwd",
                "hive.sql.table" = "EXTERNAL_JDBC_TYPE_CONVERSION_TABLE3",
                "hive.sql.dbcp.maxActive" = "1"
);

SELECT * FROM jdbc_type_conversion_table3;
