--!qt:database:derby:qdb
CREATE TEMPORARY FUNCTION dboutput AS 'org.apache.hadoop.hive.contrib.genericudf.example.GenericUDFDBOutput';

SELECT
dboutput ( '${system:hive.test.database.qdb.jdbc.url}','','',
'CREATE TABLE SIMPLE_DERBY_TABLE11 ("ikey" INTEGER, "bkey" BIGINT, "fkey" REAL, "dkey" DOUBLE)' );

SELECT
dboutput ( '${system:hive.test.database.qdb.jdbc.url}','','',
'CREATE TABLE SIMPLE_DERBY_TABLE12 ("ikey" INTEGER, "bkey" BIGINT, "fkey" REAL, "dkey" DOUBLE, "datekey" DATE)' );

SELECT
dboutput ( '${system:hive.test.database.qdb.jdbc.url}','','',
'CREATE TABLE SIMPLE_DERBY_TABLE13 ("ikey2" INTEGER, "bkey2" BIGINT, "fkey2" REAL, "dkey2" DOUBLE)' );

SELECT
dboutput ( '${system:hive.test.database.qdb.jdbc.url}','','',
'CREATE TABLE SIMPLE_DERBY_TABLE14 ("ikey2" INTEGER, "bkey2" BIGINT, "fkey2" REAL, "dkey2" DOUBLE, "datekey2" DATE)' );

CREATE EXTERNAL TABLE ext_simple_derby_table11
(
 ikey int,
 bkey bigint,
 fkey float,
 dkey double
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
                "hive.sql.database.type" = "DERBY",
                "hive.sql.jdbc.driver" = "org.apache.derby.iapi.jdbc.AutoloadedDriver",
                "hive.sql.jdbc.url" = "${system:hive.test.database.qdb.jdbc.url};collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "APP",
                "hive.sql.dbcp.password" = "mine",
                "hive.sql.table" = "SIMPLE_DERBY_TABLE11",
                "hive.sql.dbcp.maxActive" = "1"
);


CREATE EXTERNAL TABLE ext_simple_derby_table12
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
                "hive.sql.jdbc.driver" = "org.apache.derby.iapi.jdbc.AutoloadedDriver",
                "hive.sql.jdbc.url" = "${system:hive.test.database.qdb.jdbc.url};collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "APP",
                "hive.sql.dbcp.password" = "mine",
                "hive.sql.table" = "SIMPLE_DERBY_TABLE12",
                "hive.sql.dbcp.maxActive" = "1"
);

CREATE EXTERNAL TABLE ext_simple_derby_table13
(
 ikey2 int,
 bkey2 bigint,
 fkey2 float,
 dkey2 double
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
                "hive.sql.database.type" = "DERBY",
                "hive.sql.jdbc.driver" = "org.apache.derby.iapi.jdbc.AutoloadedDriver",
                "hive.sql.jdbc.url" = "${system:hive.test.database.qdb.jdbc.url};collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "APP",
                "hive.sql.dbcp.password" = "mine",
                "hive.sql.table" = "SIMPLE_DERBY_TABLE13",
                "hive.sql.dbcp.maxActive" = "1"
);


CREATE EXTERNAL TABLE ext_simple_derby_table14
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
                "hive.sql.jdbc.driver" = "org.apache.derby.iapi.jdbc.AutoloadedDriver",
                "hive.sql.jdbc.url" = "${system:hive.test.database.qdb.jdbc.url};collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "APP",
                "hive.sql.dbcp.password" = "mine",
                "hive.sql.table" = "SIMPLE_DERBY_TABLE14",
                "hive.sql.dbcp.maxActive" = "1"
);

explain cbo
with t1 as (select fkey, ikey, sum(dkey) as dk_sum, sum(dkey2) as dk2_sum
            from ext_simple_derby_table11 left join ext_simple_derby_table13
            on ikey = ikey2
            where fkey2 is null
            group by fkey, ikey),
t2 as (select datekey, fkey, ikey, sum(dkey) as dk_sum2, sum(dkey2) as dk2_sum2
       from ext_simple_derby_table12 left join ext_simple_derby_table14
       on ikey = ikey2
       where fkey2 is null
       group by datekey, fkey, ikey)
select t1.fkey, t2.ikey, sum(t1.ikey)
from t1 left join t2
on t1.ikey = t2.ikey AND t1.fkey = t2.fkey
where t2.fkey is null
group by t2.datekey, t1.fkey, t2.ikey;
