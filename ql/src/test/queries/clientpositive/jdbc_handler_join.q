--! qt:dataset:src

CREATE TEMPORARY FUNCTION dboutput AS 'org.apache.hadoop.hive.contrib.genericudf.example.GenericUDFDBOutput';

FROM src
SELECT dboutput ( 'jdbc:derby:;databaseName=${system:test.tmp.dir}/jdbc_as_external_table_db;create=true','','',
'CREATE TABLE JDBC_HANDLER_JOIN_TABLES (a BOOLEAN, b  INTEGER, c BIGINT, d FLOAT, e DOUBLE, f DATE, g VARCHAR(27),
                                  h VARCHAR(27), i CHAR(2), j TIMESTAMP, k DECIMAL(5,4), l SMALLINT, m SMALLINT, b1 CHAR(10))' )
limit 1;

CREATE EXTERNAL TABLE external_jdbc_table_join
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
 m SMALLINT,
 b1 BOOLEAN
 )
STORED BY 'org.apache.hive.storage.jdbc.JdbcStorageHandler'
TBLPROPERTIES (
                "hive.sql.database.type" = "DERBY",
                "hive.sql.jdbc.driver" = "org.apache.derby.jdbc.EmbeddedDriver",
                "hive.sql.jdbc.url" = "jdbc:derby:;databaseName=${system:test.tmp.dir}/jdbc_as_external_table_db;create=true;collation=TERRITORY_BASED:PRIMARY",
                "hive.sql.dbcp.username" = "APP",
                "hive.sql.dbcp.password" = "mine",
                "hive.sql.table" = "JDBC_HANDLER_JOIN_TABLES",
                "hive.sql.dbcp.maxActive" = "1"
);

CREATE TABLE hive_table
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
 m SMALLINT,
 b1 BOOLEAN
 );

-- insert 5 rows into hive table
INSERT INTO hive_table VALUES(true, 342, 8900, 9.63, 1099.9999, '2019-04-11', 'abcd', 'efgh', 'k', '2019-05-01 00:00:00', 1.8899, 1, 2, true);
INSERT INTO hive_table VALUES(true, 343, 8900, 9.63, 1099.9999, '2019-04-11', 'abcd', 'efgh', 'k', '2019-05-01 00:00:00', 1.8899, 1, 2, true);
INSERT INTO hive_table VALUES(true, 344, 8900, 9.63, 1099.9999, '2019-04-11', 'abcd', 'efgh', 'k', '2019-05-01 00:00:00', 1.8899, 1, 2, true);
INSERT INTO hive_table VALUES(true, 345, 8900, 9.63, 1099.9999, '2019-04-11', 'abcd', 'efgh', 'k', '2019-05-01 00:00:00', 1.8899, 1, 2, true);
INSERT INTO hive_table VALUES(true, 346, 8900, 9.63, 1099.9999, '2019-04-11', 'abcd', 'efgh', 'k', '2019-05-01 00:00:00', 1.8899, 1, 2, true);


-- insert 35 rows into jdbc table
INSERT INTO external_jdbc_table_join select * from hive_table;
INSERT INTO external_jdbc_table_join select * from hive_table;
INSERT INTO external_jdbc_table_join select * from hive_table;
INSERT INTO external_jdbc_table_join select * from hive_table;
INSERT INTO external_jdbc_table_join select * from hive_table;
INSERT INTO external_jdbc_table_join select * from hive_table;
INSERT INTO external_jdbc_table_join select * from hive_table;


select count(*) from hive_table;

select count(*) from external_jdbc_table_join;

-- joining external_jdbc_table and hive_table
set hive.auto.convert.join=true;
set hive.stats.collect.non.native.tables=true;
-- even though external_jdbc_table is a bigTable (35 rows) as compared to hive_table (5 rows) join will happen in the wrong side i.e on the hive_table
EXPLAIN SELECT d.* FROM external_jdbc_table_join d JOIN hive_table i ON d.b=342;

set hive.stats.collect.non.native.tables=false;
EXPLAIN SELECT d.* FROM external_jdbc_table_join d JOIN hive_table i ON d.b=342;

SELECT d.* FROM external_jdbc_table_join d JOIN hive_table i ON d.b=342;
