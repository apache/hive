--! qt:dataset:src

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
from tables join dbs on (tables.db_id = dbs.DB_ID) WHERE tables.name IN ("src", "dbs", "tables") order by tn, dn, t;

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
  where t1.name IN ("src", "dbs", "tables")
order by a,b;

describe tables;

-- Tests for inserting to jdbc data source

FROM src

SELECT dboutput ( 'jdbc:derby:;databaseName=${system:test.tmp.dir}/test_insert_derby_as_external_table_db;create=true','','',
'CREATE TABLE INSERT_TO_DERBY_TABLE (a BOOLEAN, b  INTEGER, c BIGINT, d FLOAT, e DOUBLE, f DATE, g VARCHAR(27),
                                  h VARCHAR(27), i CHAR(2), j TIMESTAMP, k DECIMAL(5,4), l SMALLINT, m SMALLINT)' )

limit 1;

CREATE EXTERNAL TABLE insert_to_ext_derby_table
(
 a BOOLEAN,
 b INTEGER,
 c BIGINT,
 d FLOAT,
 e DOUBLE,
 f DATE,
 g VARCHAR(27),
 h STRING,
 i CHAR(2),
 j TIMESTAMP,
 k DECIMAL(5,4),
 l TINYINT,
 m SMALLINT
 )
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
                "hive.sql.database.type" = "DERBY",
                "hive.sql.jdbc.driver" = "org.apache.derby.jdbc.EmbeddedDriver",
                "hive.sql.jdbc.url" = "jdbc:derby:;databaseName=${system:test.tmp.dir}/test_insert_derby_as_external_table_db;create=true;collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "APP",
                "hive.sql.dbcp.password" = "mine",
                "hive.sql.table" = "INSERT_TO_DERBY_TABLE",
                "hive.sql.dbcp.maxActive" = "1"
);

CREATE TABLE test_insert_tbl
(
 a BOOLEAN,
 b INTEGER,
 c BIGINT,
 d FLOAT,
 e DOUBLE,
 f DATE,
 g VARCHAR(27),
 h STRING,
 i CHAR(2),
 j TIMESTAMP,
 k DECIMAL(5,4),
 l TINYINT,
 m SMALLINT
 );

INSERT INTO test_insert_tbl VALUES(true, 342, 8900, 9.63, 1099.9999, '2019-04-11', 'abcd', 'efgh', 'k', '2019-05-01 00:00:00', 1.8899, 1, 2);

-- Inserting single row of data

INSERT INTO insert_to_ext_derby_table VALUES(true, 10, 100, 2.63, 999.9999, '2019-01-11', 'test', 'test1', 'z', '2019-01-01 00:00:00', 1.7899, 1, 2);

INSERT INTO insert_to_ext_derby_table select * from test_insert_tbl;
select * from insert_to_ext_derby_table;

INSERT INTO insert_to_ext_derby_table VALUES(false, 324, 53465, 2.6453, 599.9999, '2019-04-11', 'fgeg', 'asda', 'k', '2019-03-01 10:00:00', 1.7899, 1, 2);

-- Inserting multiple row of data
INSERT INTO insert_to_ext_derby_table VALUES(false, 10, 100, 2.63, 999.9999, '2019-11-11', 'test', 'test1', 'a', '2019-01-01 00:00:00', 1.7899, 1, 2),
                                         (true, 100, 1000, 2.632, 9999.99999, '2019-12-11', 'test_1', 'test1_1', 'b', '2019-02-01 01:00:01', 5.7899, 3, 4),
                                         (false, 10, 999, 23.632, 99999.99999, '2019-09-11', 'test_2', 'test1_2', 'c', '2019-03-01 11:00:01', 9.7899, 5, 6);

INSERT INTO insert_to_ext_derby_table select * from test_insert_tbl;
select * from insert_to_ext_derby_table;
