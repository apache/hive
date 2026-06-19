--!qt:database:derby:qdb
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

CREATE TEMPORARY FUNCTION dboutput AS 'org.apache.hadoop.hive.contrib.genericudf.example.GenericUDFDBOutput';

SELECT
dboutput ( '${system:hive.test.database.qdb.jdbc.url}','','',
'CREATE TABLE person ("id" INTEGER, "name" VARCHAR(25), "jid" INTEGER, "cid" INTEGER)' );

SELECT
dboutput ( '${system:hive.test.database.qdb.jdbc.url}','','',
'CREATE TABLE country ("id" INTEGER, "name" VARCHAR(25))' );

CREATE EXTERNAL TABLE person
(
 id int,
 name varchar(25),
 jid int,
 cid int
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
                "hive.sql.database.type" = "DERBY",
                "hive.sql.jdbc.driver" = "org.apache.derby.iapi.jdbc.AutoloadedDriver",
                "hive.sql.jdbc.url" = "${system:hive.test.database.qdb.jdbc.url};collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "APP",
                "hive.sql.dbcp.password" = "mine",
                "hive.sql.table" = "PERSON",
                "hive.sql.dbcp.maxActive" = "1"
);

CREATE EXTERNAL TABLE country
(
    id int,
    name varchar(25)
)
    STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
    TBLPROPERTIES (
        "hive.sql.database.type" = "DERBY",
        "hive.sql.jdbc.driver" = "org.apache.derby.iapi.jdbc.AutoloadedDriver",
        "hive.sql.jdbc.url" = "${system:hive.test.database.qdb.jdbc.url};collation=TERRITORY_BASED:PRIMARY",
        "hive.sql.dbcp.username" = "APP",
        "hive.sql.dbcp.password" = "mine",
        "hive.sql.table" = "COUNTRY",
        "hive.sql.dbcp.maxActive" = "1"
        );

CREATE TABLE job (
    id int,
    title varchar(20)
) STORED AS ORC TBLPROPERTIES ('transactional'='true');

CREATE MATERIALIZED VIEW mv1 AS SELECT id, title FROM job WHERE title = 'Software Engineer';

explain cbo 
select * 
from person 
join job on person.jid = job.id
join country on person.cid = country.id
where job.title = 'Software Engineer';

DROP MATERIALIZED VIEW mv1;
