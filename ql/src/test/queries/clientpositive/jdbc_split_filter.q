--!qt:database:derby:qdb

CREATE TEMPORARY FUNCTION dboutput AS 'org.apache.hadoop.hive.contrib.genericudf.example.GenericUDFDBOutput';


SELECT

dboutput ( '${system:hive.test.database.qdb.jdbc.url}','','',
'CREATE TABLE SIMPLE_DERBY_TABLE1 ("ikey" INTEGER, "bkey" BIGINT, "fkey" REAL, "dkey" DOUBLE)' );

SELECT

dboutput ( '${system:hive.test.database.qdb.jdbc.url}','','',
'CREATE TABLE SIMPLE_DERBY_TABLE2 ("ikey" INTEGER, "bkey" BIGINT, "fkey" REAL, "dkey" DOUBLE, "datekey" DATE)' );

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
                "hive.sql.jdbc.url" = "${system:hive.test.database.qdb.jdbc.url};collation=TERRITORY_BASED:PRIMARY",
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
                "hive.sql.jdbc.url" = "${system:hive.test.database.qdb.jdbc.url};collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "APP",
                "hive.sql.dbcp.password" = "mine",
                "hive.sql.table" = "SIMPLE_DERBY_TABLE2",
                "hive.sql.dbcp.maxActive" = "1"
);


explain cbo
with t1 as (select fkey, ikey, bkey, dkey from ext_simple_derby_table1),
t2 as (select fkey, ikey, datekey, dkey, bkey from ext_simple_derby_table2)
select t1.fkey, t2.dkey, sum(t1.ikey)
from t1 left join t2
on t1.ikey = t2.ikey AND t1.fkey = t2.fkey
where t2.fkey is null
group by t2.datekey, t1.fkey, t2.dkey;