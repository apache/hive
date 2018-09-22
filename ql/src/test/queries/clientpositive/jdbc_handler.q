
set hive.strict.checks.cartesian.product= false;


CREATE TEMPORARY FUNCTION dboutput AS 'org.apache.hadoop.hive.contrib.genericudf.example.GenericUDFDBOutput';


FROM src

SELECT dboutput ( 'jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_as_external_table_db;create=true','','',
'CREATE TABLE SIMPLE_DERBY_TABLE ("kkey" INTEGER NOT NULL )' ),

dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_as_external_table_db;create=true','','',
'INSERT INTO SIMPLE_DERBY_TABLE ("kkey") VALUES (?)','20'),

dboutput('jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_as_external_table_db;create=true','','',
'INSERT INTO SIMPLE_DERBY_TABLE ("kkey") VALUES (?)','200')

limit 1;

CREATE EXTERNAL TABLE ext_simple_derby_table
(
 kkey int
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
                "hive.sql.database.type" = "DERBY",
                "hive.sql.jdbc.driver" = "org.apache.derby.jdbc.EmbeddedDriver",
                "hive.sql.jdbc.url" = "jdbc:derby:;databaseName=${system:test.tmp.dir}/test_derby_as_external_table_db;create=true;collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "APP",
                "hive.sql.dbcp.password" = "mine",
                "hive.sql.table" = "SIMPLE_DERBY_TABLE",
                "hive.sql.dbcp.maxActive" = "1"
);

select * from ext_simple_derby_table;

explain select * from ext_simple_derby_table where 100 < ext_simple_derby_table.kkey;

select * from ext_simple_derby_table where 100 < ext_simple_derby_table.kkey;

CREATE EXTERNAL TABLE tables
(
id bigint,
db_id bigint,
name STRING,
type STRING,
owner STRING
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" = "SELECT TBL_ID, DB_ID, TBL_NAME, TBL_TYPE, OWNER FROM TBLS",
"hive.sql.column.mapping" = "id=TBL_ID, db_id=DB_ID, name=TBL_NAME, type=TBL_TYPE, owner=OWNER"
);

CREATE EXTERNAL TABLE dbs
(
DB_ID bigint,
NAME STRING
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
"hive.sql.database.type" = "METASTORE",
"hive.sql.query" = "SELECT DB_ID, NAME FROM DBS"
);


select tables.name as tn, dbs.NAME as dn, tables.type as t
from tables join dbs on (tables.db_id = dbs.DB_ID) order by tn, dn, t;

explain
select
  t1.name as a, t2.key as b
from
  (select 1 as db_id, tables.name from tables) t1
  join
  (select distinct key from src) t2
  on (t2.key-1) = t1.db_id
order by a,b;

select
  t1.name as a, t2.key as b
from
  (select 1 as db_id, tables.name from tables) t1
  join
  (select distinct key from src) t2
  on (t2.key-1) = t1.db_id
order by a,b;

show tables;

describe tables;
