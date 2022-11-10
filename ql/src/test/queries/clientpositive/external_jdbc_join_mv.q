set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

CREATE TEMPORARY FUNCTION dboutput AS 'org.apache.hadoop.hive.contrib.genericudf.example.GenericUDFDBOutput';

SELECT
dboutput ( 'jdbc:derby:;databaseName=${system:test.tmp.dir}/test_jdbc_join_mv;create=true','','',
'CREATE TABLE person ("id" INTEGER, "name" VARCHAR(25), "jid" INTEGER, "cid" INTEGER)' );

SELECT
dboutput ( 'jdbc:derby:;databaseName=${system:test.tmp.dir}/test_jdbc_join_mv;create=true','','',
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
                "hive.sql.jdbc.driver" = "org.apache.derby.jdbc.EmbeddedDriver",
                "hive.sql.jdbc.url" = "jdbc:derby:;databaseName=${system:test.tmp.dir}/test_jdbc_join_mv;create=true;collation=TERRITORY_BASED:PRIMARY",
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
        "hive.sql.jdbc.driver" = "org.apache.derby.jdbc.EmbeddedDriver",
        "hive.sql.jdbc.url" = "jdbc:derby:;databaseName=${system:test.tmp.dir}/test_jdbc_join_mv;create=true;collation=TERRITORY_BASED:PRIMARY",
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
