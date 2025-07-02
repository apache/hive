CREATE TABLE explain_jdbc_hive_table (id INT, bigId BIGINT);

CREATE TEMPORARY FUNCTION dboutput AS 'org.apache.hadoop.hive.contrib.genericudf.example.GenericUDFDBOutput';


FROM (select 1 as hello) src

SELECT

dboutput ( 'jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_as_external_table_explain_jdbc_db;create=true','','',
'CREATE TABLE DERBY_TABLE ("id" INTEGER, "bigId" BIGINT)' ),

dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_as_external_table_explain_jdbc_db;create=true','','',
'INSERT INTO DERBY_TABLE ("id","bigId") VALUES (?,?)','20','20')
limit 1;

CREATE EXTERNAL TABLE ext_DERBY_TABLE
(
 id int,
 bigId bigint
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
                "hive.sql.database.type" = "DERBY",
                "hive.sql.jdbc.driver" = "org.apache.derby.jdbc.EmbeddedDriver",
                "hive.sql.jdbc.url" = "jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_as_external_table_explain_jdbc_db;create=true;collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "APP",
                "hive.sql.dbcp.password" = "mine",
                "hive.sql.table" = "DERBY_TABLE",
                "hive.sql.dbcp.maxActive" = "1"
);


SET hive.fetch.task.conversion=none;

select 1 from ext_DERBY_TABLE;

explain extended select 1 from ext_DERBY_TABLE;


create table if not exists ctas_dbs as select * from ext_DERBY_TABLE;

select 1
from ctas_dbs
limit 1;

explain extended
select 1
from ctas_dbs
limit 1;

create table if not exists ctlt_dbs like ext_DERBY_TABLE;

insert into ctlt_dbs
select * from ext_DERBY_TABLE;

select 1
from ctlt_dbs
limit 1;

explain extended
select 1
from ctlt_dbs
limit 1;
