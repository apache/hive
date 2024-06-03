--! qt:dataset:src

CREATE TEMPORARY FUNCTION dboutput AS 'org.apache.hadoop.hive.contrib.genericudf.example.GenericUDFDBOutput';

-- convert varchar to numeric/decimal/date/timestamp
FROM src
SELECT
dboutput ('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby2_300;create=true','user','passwd',
'CREATE TABLE EXTERNAL_JDBC_TYPE_CONVERSION_TABLE1 ("ikey" VARCHAR(20), "bkey" VARCHAR(20), "fkey" VARCHAR(20), "dkey" VARCHAR(20), "chkey" VARCHAR(20), "dekey" VARCHAR(20), "dtkey" VARCHAR(20), "tkey" VARCHAR(50))' ),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby2_300','user','passwd',
'INSERT INTO EXTERNAL_JDBC_TYPE_CONVERSION_TABLE1 ("ikey","bkey","fkey","dkey","chkey","dekey","dtkey","tkey") VALUES (?,?,?,?,?,?,?,?)','1','1000','20.0','40.0','aaa','3.1415','2010-01-01','2018-01-01 12:00:00.000000000'),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby2_300','user','passwd',
'INSERT INTO EXTERNAL_JDBC_TYPE_CONVERSION_TABLE1 ("ikey","bkey","fkey","dkey","chkey","dekey","dtkey","tkey") VALUES (?,?,?,?,?,?,?,?)','5','9000',null,'10.0','bbb','2.7182','2018-01-01','2010-06-01 14:00:00.000000000'),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby2_300','user','passwd',
'INSERT INTO EXTERNAL_JDBC_TYPE_CONVERSION_TABLE1 ("ikey","bkey","fkey","dkey","chkey","dekey","dtkey","tkey") VALUES (?,?,?,?,?,?,?,?)','3','4000','120.0','25.4','hello','2.7182','2017-06-05','2011-11-10 18:00:08.000000000'),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby2_300','user','passwd',
'INSERT INTO EXTERNAL_JDBC_TYPE_CONVERSION_TABLE1 ("ikey","bkey","fkey","dkey","chkey","dekey","dtkey","tkey") VALUES (?,?,?,?,?,?,?,?)','8','3000','180.0','35.8','world','3.1415','2014-03-03','2016-07-04 13:00:00.000000000'),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby2_300','user','passwd',
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
                "hive.sql.jdbc.url" = "jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby2_300;collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "user",
                "hive.sql.dbcp.password" = "passwd",
                "hive.sql.table" = "EXTERNAL_JDBC_TYPE_CONVERSION_TABLE1",
                "hive.sql.dbcp.maxActive" = "1"
);
set hive.cbo.enable=true;
SELECT * FROM jdbc_type_conversion_table1;
set hive.cbo.enable=false;
SELECT * FROM jdbc_type_conversion_table1;

-- Test type conversion matrix:
-- from Derby [INTEGER, BIGINT, REAL, DOUBLE, VARCHAR(20), DECIMAL(6,4), DATE, TIMESTAMP]
-- to Hive [STRING, INT, BIGINT, DOUBLE, DECIMAL(5,1), DECIMAL(6,2), DECIMAL(16,2), DATE, TIMESTAMP]

FROM src
SELECT
dboutput ('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby2_300;create=true','user','passwd',
'CREATE TABLE EXTERNAL_JDBC_TYPE_CONVERSION_TABLE2 ("ikey" INTEGER, "bkey" BIGINT, "fkey" REAL, "dkey" DOUBLE, "chkey" VARCHAR(20), "dekey" DECIMAL(6,4), "dtkey" DATE, "tkey" TIMESTAMP, "mixkey" VARCHAR(50))' ),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby2_300','user','passwd',
'INSERT INTO EXTERNAL_JDBC_TYPE_CONVERSION_TABLE2 ("ikey","bkey","fkey","dkey","chkey","dekey","dtkey","tkey", "mixkey") VALUES (?,?,?,?,?,?,?,?,?)','1','1000','20.0','40.0','aaa','3.1415','2010-01-01','2018-01-01 12:00:00.000000000','10'),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby2_300','user','passwd',
'INSERT INTO EXTERNAL_JDBC_TYPE_CONVERSION_TABLE2 ("ikey","bkey","fkey","dkey","chkey","dekey","dtkey","tkey", "mixkey") VALUES (?,?,?,?,?,?,?,?,?)','5','9000',null,'10.0','bbb','2.7182','2018-01-01','2010-06-01 14:00:00.000000000','100000000000'),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby2_300','user','passwd',
'INSERT INTO EXTERNAL_JDBC_TYPE_CONVERSION_TABLE2 ("ikey","bkey","fkey","dkey","chkey","dekey","dtkey","tkey", "mixkey") VALUES (?,?,?,?,?,?,?,?,?)','3','4000','120.0','25.4','hello','2.7182','2017-06-05','2011-11-10 18:00:08.000000000','10.582'),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby2_300','user','passwd',
'INSERT INTO EXTERNAL_JDBC_TYPE_CONVERSION_TABLE2 ("ikey","bkey","fkey","dkey","chkey","dekey","dtkey","tkey", "mixkey") VALUES (?,?,?,?,?,?,?,?,?)','8','3000','180.0','35.8','world','3.1415','2014-03-03','2016-07-04 13:00:00.000000000','2024-03-03'),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby2_300','user','passwd',
'INSERT INTO EXTERNAL_JDBC_TYPE_CONVERSION_TABLE2 ("ikey","bkey","fkey","dkey","chkey","dekey","dtkey","tkey", "mixkey") VALUES (?,?,?,?,?,?,?,?,?)','4','8000','120.4','31.3','ccc',null,'2014-03-04','2018-07-08 11:00:00.000000000','2018-07-08 11:00:00.000000000'),
dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby2_300','user','passwd',
'INSERT INTO EXTERNAL_JDBC_TYPE_CONVERSION_TABLE2 ("ikey","bkey","fkey","dkey","chkey","dekey","dtkey","tkey", "mixkey") VALUES (?,?,?,?,?,?,?,?,?)','6','6000','80.4','5.3','ddd',null,'2024-05-31','2024-05-31 13:22:34.000000123','ddd')
limit 1;

CREATE EXTERNAL TABLE jdbc_string_conversion_table
(
 ikey string,
 bkey string,
 fkey string,
 dkey string,
 chkey string,
 dekey string,
 dtkey string,
 tkey string,
 mixkey string 
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
                "hive.sql.database.type" = "DERBY",
                "hive.sql.jdbc.driver" = "org.apache.derby.jdbc.EmbeddedDriver",
                "hive.sql.jdbc.url" = "jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby2_300;collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "user",
                "hive.sql.dbcp.password" = "passwd",
                "hive.sql.table" = "EXTERNAL_JDBC_TYPE_CONVERSION_TABLE2",
                "hive.sql.dbcp.maxActive" = "1"
);
set hive.cbo.enable=true;
SELECT * FROM jdbc_string_conversion_table;
set hive.cbo.enable=false;
SELECT * FROM jdbc_string_conversion_table;

CREATE EXTERNAL TABLE jdbc_int_conversion_table
(
 ikey int,
 bkey int,
 fkey int,
 dkey int,
 chkey int,
 dekey int,
 dtkey int,
 tkey int,
 mixkey int
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
                "hive.sql.database.type" = "DERBY",
                "hive.sql.jdbc.driver" = "org.apache.derby.jdbc.EmbeddedDriver",
                "hive.sql.jdbc.url" = "jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby2_300;collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "user",
                "hive.sql.dbcp.password" = "passwd",
                "hive.sql.table" = "EXTERNAL_JDBC_TYPE_CONVERSION_TABLE2",
                "hive.sql.dbcp.maxActive" = "1"
);
set hive.cbo.enable=true;
SELECT * FROM jdbc_int_conversion_table;
set hive.cbo.enable=false;
SELECT * FROM jdbc_int_conversion_table;

CREATE EXTERNAL TABLE jdbc_bigint_conversion_table
(
 ikey bigint,
 bkey bigint,
 fkey bigint,
 dkey bigint,
 chkey bigint,
 dekey bigint,
 dtkey bigint,
 tkey bigint,
 mixkey bigint
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
                "hive.sql.database.type" = "DERBY",
                "hive.sql.jdbc.driver" = "org.apache.derby.jdbc.EmbeddedDriver",
                "hive.sql.jdbc.url" = "jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby2_300;collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "user",
                "hive.sql.dbcp.password" = "passwd",
                "hive.sql.table" = "EXTERNAL_JDBC_TYPE_CONVERSION_TABLE2",
                "hive.sql.dbcp.maxActive" = "1"
);
set hive.cbo.enable=true;
SELECT * FROM jdbc_bigint_conversion_table;
set hive.cbo.enable=false;
SELECT * FROM jdbc_bigint_conversion_table;

CREATE EXTERNAL TABLE jdbc_double_conversion_table
(
 ikey double,
 bkey double,
 fkey double,
 dkey double,
 chkey double,
 dekey double,
 dtkey double,
 tkey double,
 mixkey double
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
                "hive.sql.database.type" = "DERBY",
                "hive.sql.jdbc.driver" = "org.apache.derby.jdbc.EmbeddedDriver",
                "hive.sql.jdbc.url" = "jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby2_300;collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "user",
                "hive.sql.dbcp.password" = "passwd",
                "hive.sql.table" = "EXTERNAL_JDBC_TYPE_CONVERSION_TABLE2",
                "hive.sql.dbcp.maxActive" = "1"
);
set hive.cbo.enable=true;
SELECT * FROM jdbc_double_conversion_table;
set hive.cbo.enable=false;
SELECT * FROM jdbc_double_conversion_table;

CREATE EXTERNAL TABLE jdbc_decimal5_1_conversion_table
(
    ikey decimal(5,1),
    bkey decimal(5,1),
    fkey decimal(5,1),
    dkey decimal(5,1),
    chkey decimal(5,1),
    dekey decimal(5,1),
    dtkey decimal(5,1),
    tkey decimal(5,1),
    mixkey decimal(5,1)
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
                "hive.sql.database.type" = "DERBY",
                "hive.sql.jdbc.driver" = "org.apache.derby.jdbc.EmbeddedDriver",
                "hive.sql.jdbc.url" = "jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby2_300;collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "user",
                "hive.sql.dbcp.password" = "passwd",
                "hive.sql.table" = "EXTERNAL_JDBC_TYPE_CONVERSION_TABLE2",
                "hive.sql.dbcp.maxActive" = "1"
);
set hive.cbo.enable=true;
SELECT * FROM jdbc_decimal5_1_conversion_table;
set hive.cbo.enable=false;
SELECT * FROM jdbc_decimal5_1_conversion_table;

CREATE EXTERNAL TABLE jdbc_decimal6_4_conversion_table
(
    ikey decimal(6,4),
    bkey decimal(6,4),
    fkey decimal(6,4),
    dkey decimal(6,4),
    chkey decimal(6,4),
    dekey decimal(6,4),
    dtkey decimal(6,4),
    tkey decimal(6,4),
    mixkey decimal(6,4)
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
                "hive.sql.database.type" = "DERBY",
                "hive.sql.jdbc.driver" = "org.apache.derby.jdbc.EmbeddedDriver",
                "hive.sql.jdbc.url" = "jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby2_300;collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "user",
                "hive.sql.dbcp.password" = "passwd",
                "hive.sql.table" = "EXTERNAL_JDBC_TYPE_CONVERSION_TABLE2",
                "hive.sql.dbcp.maxActive" = "1"
);
set hive.cbo.enable=true;
SELECT * FROM jdbc_decimal6_4_conversion_table;
set hive.cbo.enable=false;
SELECT * FROM jdbc_decimal6_4_conversion_table;

CREATE EXTERNAL TABLE jdbc_decimal16_2_conversion_table
(
 ikey decimal(16,2),
 bkey decimal(16,2),
 fkey decimal(16,2),
 dkey decimal(16,2),
 chkey decimal(16,2),
 dekey decimal(16,2),
 dtkey decimal(16,2),
 tkey decimal(16,2),
 mixkey decimal(16,2)
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
                "hive.sql.database.type" = "DERBY",
                "hive.sql.jdbc.driver" = "org.apache.derby.jdbc.EmbeddedDriver",
                "hive.sql.jdbc.url" = "jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby2_300;collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "user",
                "hive.sql.dbcp.password" = "passwd",
                "hive.sql.table" = "EXTERNAL_JDBC_TYPE_CONVERSION_TABLE2",
                "hive.sql.dbcp.maxActive" = "1"
);
set hive.cbo.enable=true;
SELECT * FROM jdbc_decimal16_2_conversion_table;
set hive.cbo.enable=false;
SELECT * FROM jdbc_decimal16_2_conversion_table;

CREATE EXTERNAL TABLE jdbc_date_conversion_table
(
    ikey date,
    bkey date,
    fkey date,
    dkey date,
    chkey date,
    dekey date,
    dtkey date,
    tkey date,
    mixkey date
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
                "hive.sql.database.type" = "DERBY",
                "hive.sql.jdbc.driver" = "org.apache.derby.jdbc.EmbeddedDriver",
                "hive.sql.jdbc.url" = "jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby2_300;collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "user",
                "hive.sql.dbcp.password" = "passwd",
                "hive.sql.table" = "EXTERNAL_JDBC_TYPE_CONVERSION_TABLE2",
                "hive.sql.dbcp.maxActive" = "1"
);
set hive.cbo.enable=true;
SELECT * FROM jdbc_date_conversion_table;
set hive.cbo.enable=false;
SELECT * FROM jdbc_date_conversion_table;

CREATE EXTERNAL TABLE jdbc_timestamp_conversion_table
(
    ikey timestamp,
    bkey timestamp,
    fkey timestamp,
    dkey timestamp,
    chkey timestamp,
    dekey timestamp,
    dtkey timestamp,
    tkey timestamp,
    mixkey timestamp
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
                "hive.sql.database.type" = "DERBY",
                "hive.sql.jdbc.driver" = "org.apache.derby.jdbc.EmbeddedDriver",
                "hive.sql.jdbc.url" = "jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby2_300;collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "user",
                "hive.sql.dbcp.password" = "passwd",
                "hive.sql.table" = "EXTERNAL_JDBC_TYPE_CONVERSION_TABLE2",
                "hive.sql.dbcp.maxActive" = "1"
);
set hive.cbo.enable=true;
SELECT * FROM jdbc_timestamp_conversion_table;
set hive.cbo.enable=false;
SELECT * FROM jdbc_timestamp_conversion_table;