--!qt:database:derby:qdb
CREATE TEMPORARY FUNCTION dboutput AS 'org.apache.hadoop.hive.contrib.genericudf.example.GenericUDFDBOutput';

SELECT
dboutput ( '${system:hive.test.database.qdb.jdbc.url}','','',
'CREATE TABLE person ("id" INTEGER, "name" VARCHAR(25), "jid" INTEGER, "cid" INTEGER)' );

SELECT
dboutput ( '${system:hive.test.database.qdb.jdbc.url}','','',
'CREATE TABLE country ("id" INTEGER, "name" VARCHAR(25))' );

SELECT
dboutput ( '${system:hive.test.database.qdb.jdbc.url}','','',
'CREATE TABLE job ("id" INTEGER, "title" VARCHAR(20))' );


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
    "hive.sql.jdbc.url" = "${system:hive.test.database.qdb.jdbc.url};collation=TERRITORY_BASED:PRIMARY",
    "hive.sql.dbcp.username" = "APP",
    "hive.sql.dbcp.password" = "mine",
    "hive.sql.table" = "PERSON",
    "hive.sql.dbcp.maxActive" = "1"
);
INSERT INTO `person` VALUES (1, 'Laszlo', 1, 1);

CREATE EXTERNAL TABLE country
(
    id int,
    name varchar(25)
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
    "hive.sql.database.type" = "DERBY",
    "hive.sql.jdbc.driver" = "org.apache.derby.jdbc.EmbeddedDriver",
    "hive.sql.jdbc.url" = "${system:hive.test.database.qdb.jdbc.url};collation=TERRITORY_BASED:PRIMARY",
    "hive.sql.dbcp.username" = "APP",
    "hive.sql.dbcp.password" = "mine",
    "hive.sql.table" = "COUNTRY",
    "hive.sql.dbcp.maxActive" = "1",
    "hive.sql.numPartitions"="2",
    "hive.sql.partitionColumn"="id"
);
INSERT INTO `country` VALUES (1, 'Hungary'); 
INSERT INTO `country` VALUES (2, 'Poland'); 
INSERT INTO `country` VALUES (3, 'Germany');     

CREATE EXTERNAL TABLE job
(
    id int,
    title varchar(20)
)
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
    "hive.sql.database.type" = "DERBY",
    "hive.sql.jdbc.driver" = "org.apache.derby.jdbc.EmbeddedDriver",
    "hive.sql.jdbc.url" = "${system:hive.test.database.qdb.jdbc.url};collation=TERRITORY_BASED:PRIMARY",
    "hive.sql.dbcp.username" = "APP",
    "hive.sql.dbcp.password" = "mine",
    "hive.sql.schema" = "APP",
    "hive.sql.table" = "JOB",
    "hive.sql.dbcp.maxActive" = "1",
    "hive.sql.numPartitions"="2",
    "hive.sql.partitionColumn"="id"
);
INSERT INTO `job` VALUES (1, 'Software Engineer'); 
INSERT INTO `job` VALUES (2, 'Architect'); 
INSERT INTO `job` VALUES (3, 'Quality Assurance'); 

CREATE VIEW v1 AS 
SELECT id FROM job WHERE title = 'Software Engineer';

select * from v1;

CREATE VIEW v2 AS 
SELECT person.* FROM person 
    JOIN v1 job on person.jid = job.id
    JOIN country on person.cid = country.id
WHERE country.name = 'Hungary';

select * from v2;
