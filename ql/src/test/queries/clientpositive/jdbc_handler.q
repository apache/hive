
set hive.strict.checks.cartesian.product= false;
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
