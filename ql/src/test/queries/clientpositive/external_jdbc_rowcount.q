--! qt:dataset:src

set hive.strict.checks.cartesian.product= false;

CREATE TEMPORARY FUNCTION dboutput AS 'org.apache.hadoop.hive.contrib.genericudf.example.GenericUDFDBOutput';


FROM src

SELECT

dboutput ( 'jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_as_external_table_db;create=true','','',
'CREATE TABLE SIMPLE_DERBY_TABLE1 ("ikey" INTEGER, "bkey" BIGINT, "fkey" REAL, "dkey" DOUBLE)' ),

dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_as_external_table_db;create=true','','',
'INSERT INTO SIMPLE_DERBY_TABLE1 ("ikey","bkey","fkey","dkey") VALUES (?,?,?,?)','20','20','20.0','20.0'),

dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_as_external_table_db;create=true','','',
'INSERT INTO SIMPLE_DERBY_TABLE1 ("ikey","bkey","fkey","dkey") VALUES (?,?,?,?)','-20','-20','-20.0','-20.0'),

dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_as_external_table_db;create=true','','',
'INSERT INTO SIMPLE_DERBY_TABLE1 ("ikey","bkey","fkey","dkey") VALUES (?,?,?,?)','100','-15','65.0','-74.0'),

dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_as_external_table_db;create=true','','',
'INSERT INTO SIMPLE_DERBY_TABLE1 ("ikey","bkey","fkey","dkey") VALUES (?,?,?,?)','44','53','-455.454','330.76')

limit 1;

FROM src

SELECT

dboutput ( 'jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_as_external_table_db;create=true','','',
'CREATE TABLE SIMPLE_DERBY_TABLE2 ("ikey" INTEGER, "bkey" BIGINT, "fkey" REAL, "dkey" DOUBLE, "datekey" DATE)' ),

dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_as_external_table_db;create=true','','',
'INSERT INTO SIMPLE_DERBY_TABLE2 ("ikey","bkey","fkey","dkey","datekey") VALUES (?,?,?,?,?)','20','20','20.0','20.0','1999-02-22'),

dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_as_external_table_db;create=true','','',
'INSERT INTO SIMPLE_DERBY_TABLE2 ("ikey","bkey","fkey","dkey","datekey") VALUES (?,?,?,?,?)','-20','8','9.0','11.0','2000-03-15'),

dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_as_external_table_db;create=true','','',
'INSERT INTO SIMPLE_DERBY_TABLE2 ("ikey","bkey","fkey","dkey","datekey") VALUES (?,?,?,?,?)','101','-16','66.0','-75.0','2010-04-01'),

dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_as_external_table_db;create=true','','',
'INSERT INTO SIMPLE_DERBY_TABLE2 ("ikey","bkey","fkey","dkey","datekey") VALUES (?,?,?,?,?)','40','50','-455.4543','330.767','2010-04-02')

limit 1;

FROM src

SELECT

dboutput ( 'jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_as_external_table_db;create=true','','',
'CREATE TABLE SIMPLE_DERBY_TABLE3 ("ikey2" INTEGER, "bkey2" BIGINT, "fkey2" REAL, "dkey2" DOUBLE)' ),

dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_as_external_table_db;create=true','','',
'INSERT INTO SIMPLE_DERBY_TABLE3 ("ikey2","bkey2","fkey2","dkey2") VALUES (?,?,?,?)','10','10','10.0','10.0'),

dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_as_external_table_db;create=true','','',
'INSERT INTO SIMPLE_DERBY_TABLE3 ("ikey2","bkey2","fkey2","dkey2") VALUES (?,?,?,?)','-10','-10','-10.0','-10.0'),

dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_as_external_table_db;create=true','','',
'INSERT INTO SIMPLE_DERBY_TABLE3 ("ikey2","bkey2","fkey2","dkey2") VALUES (?,?,?,?)','200','-25','55.0','-84.0'),

dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_as_external_table_db;create=true','','',
'INSERT INTO SIMPLE_DERBY_TABLE3 ("ikey2","bkey2","fkey2","dkey2") VALUES (?,?,?,?)','54','53','-455.454','330.76')

limit 1;

FROM src

SELECT

dboutput ( 'jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_as_external_table_db;create=true','','',
'CREATE TABLE SIMPLE_DERBY_TABLE4 ("ikey2" INTEGER, "bkey2" BIGINT, "fkey2" REAL, "dkey2" DOUBLE, "datekey2" DATE)' ),

dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_as_external_table_db;create=true','','',
'INSERT INTO SIMPLE_DERBY_TABLE4 ("ikey2","bkey2","fkey2","dkey2","datekey2") VALUES (?,?,?,?,?)','10','10','10.0','10.0','1999-02-22'),

dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_as_external_table_db;create=true','','',
'INSERT INTO SIMPLE_DERBY_TABLE4 ("ikey2","bkey2","fkey2","dkey2","datekey2") VALUES (?,?,?,?,?)','-10','7','9.0','12.0','2000-03-15'),

dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_as_external_table_db;create=true','','',
'INSERT INTO SIMPLE_DERBY_TABLE4 ("ikey2","bkey2","fkey2","dkey2","datekey2") VALUES (?,?,?,?,?)','102','-16','66.0','-75.0','2010-04-01'),

dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_as_external_table_db;create=true','','',
'INSERT INTO SIMPLE_DERBY_TABLE4 ("ikey2","bkey2","fkey2","dkey2","datekey2") VALUES (?,?,?,?,?)','40','50','-455.4543','330.767','2010-04-02')

limit 1;

CREATE EXTERNAL TABLE ext_simple_derby_table1
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
                "hive.sql.jdbc.url" = "jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_as_external_table_db;create=true;collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "APP",
                "hive.sql.dbcp.password" = "mine",
                "hive.sql.table" = "SIMPLE_DERBY_TABLE1",
                "hive.sql.dbcp.maxActive" = "1"
);


CREATE EXTERNAL TABLE ext_simple_derby_table2
(
 ikey int,
 bkey bigint,
 fkey float,
 dkey double,
 datekey string
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
                "hive.sql.database.type" = "DERBY",
                "hive.sql.jdbc.driver" = "org.apache.derby.jdbc.EmbeddedDriver",
                "hive.sql.jdbc.url" = "jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_as_external_table_db;create=true;collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "APP",
                "hive.sql.dbcp.password" = "mine",
                "hive.sql.table" = "SIMPLE_DERBY_TABLE2",
                "hive.sql.dbcp.maxActive" = "1"
);

CREATE EXTERNAL TABLE ext_simple_derby_table3
(
 ikey2 int,
 bkey2 bigint,
 fkey2 float,
 dkey2 double
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
                "hive.sql.database.type" = "DERBY",
                "hive.sql.jdbc.driver" = "org.apache.derby.jdbc.EmbeddedDriver",
                "hive.sql.jdbc.url" = "jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_as_external_table_db;create=true;collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "APP",
                "hive.sql.dbcp.password" = "mine",
                "hive.sql.table" = "SIMPLE_DERBY_TABLE3",
                "hive.sql.dbcp.maxActive" = "1"
);


CREATE EXTERNAL TABLE ext_simple_derby_table4
(
 ikey2 int,
 bkey2 bigint,
 fkey2 float,
 dkey2 double,
 datekey2 string
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
                "hive.sql.database.type" = "DERBY",
                "hive.sql.jdbc.driver" = "org.apache.derby.jdbc.EmbeddedDriver",
                "hive.sql.jdbc.url" = "jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_as_external_table_db;create=true;collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "APP",
                "hive.sql.dbcp.password" = "mine",
                "hive.sql.table" = "SIMPLE_DERBY_TABLE4",
                "hive.sql.dbcp.maxActive" = "1"
);

explain cbo
with t1 as (select fkey, ikey, sum(dkey) as dk_sum, sum(dkey2) as dk2_sum
            from ext_simple_derby_table1 left join ext_simple_derby_table3
            on ikey = ikey2
            where fkey2 is null
            group by fkey, ikey),
t2 as (select datekey, fkey, ikey, sum(dkey) as dk_sum2, sum(dkey2) as dk2_sum2
       from ext_simple_derby_table2 left join ext_simple_derby_table4
       on ikey = ikey2
       where fkey2 is null
       group by datekey, fkey, ikey)
select t1.fkey, t2.ikey, sum(t1.ikey)
from t1 left join t2
on t1.ikey = t2.ikey AND t1.fkey = t2.fkey
where t2.fkey is null
group by t2.datekey, t1.fkey, t2.ikey;


with t1 as (select fkey, ikey, sum(dkey) as dk_sum, sum(dkey2) as dk2_sum
            from ext_simple_derby_table1 left join ext_simple_derby_table3
            on ikey = ikey2
            where fkey2 is null
            group by fkey, ikey),
t2 as (select datekey, fkey, ikey, sum(dkey) as dk_sum2, sum(dkey2) as dk2_sum2
       from ext_simple_derby_table2 left join ext_simple_derby_table4
       on ikey = ikey2
       where fkey2 is null
       group by datekey, fkey, ikey)
select t1.fkey, t2.ikey, sum(t1.ikey)
from t1 left join t2
on t1.ikey = t2.ikey AND t1.fkey = t2.fkey
where t2.fkey is null
group by t2.datekey, t1.fkey, t2.ikey;
