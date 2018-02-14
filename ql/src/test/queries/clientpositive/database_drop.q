-- create database with multiple tables, views.
-- Use both partitioned and non-partitioned tables, as well as
-- tables with specific storage locations
-- verify the drop the database with cascade works and that the directories
-- outside the database's default storage are removed as part of the drop

CREATE DATABASE db5;
SHOW DATABASES;
USE db5;

set hive.stats.dbclass=fs;
dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/dbcascade/temp;
dfs -rmr ${system:test.tmp.dir}/dbcascade;
dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/dbcascade;

-- add a table, view
CREATE TABLE temp_tbl (id INT, name STRING);
LOAD DATA LOCAL INPATH '../../data/files/kv1.txt' INTO TABLE temp_tbl;
CREATE VIEW temp_tbl_view AS SELECT * FROM temp_tbl;

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/dbcascade/temp_tbl2;
-- add a table, view with a different storage location
CREATE TABLE temp_tbl2 (id INT, name STRING) LOCATION 'file:${system:test.tmp.dir}/dbcascade/temp_tbl2';
LOAD DATA LOCAL INPATH '../../data/files/kv1.txt' into table temp_tbl2;
CREATE VIEW temp_tbl2_view AS SELECT * FROM temp_tbl2;

-- add a partitioned table, view
CREATE TABLE part_tab (id INT, name STRING)  PARTITIONED BY (ds string);
LOAD DATA LOCAL INPATH '../../data/files/kv1.txt' INTO TABLE part_tab PARTITION (ds='2008-04-09');
LOAD DATA LOCAL INPATH '../../data/files/kv1.txt' INTO TABLE part_tab PARTITION (ds='2009-04-09');

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/dbcascade/part_tab2;
-- add a partitioned table, view with a different storage location
CREATE TABLE part_tab2 (id INT, name STRING)  PARTITIONED BY (ds string)
		LOCATION 'file:${system:test.tmp.dir}/dbcascade/part_tab2';
LOAD DATA LOCAL INPATH '../../data/files/kv1.txt' INTO TABLE part_tab2 PARTITION (ds='2008-04-09');
LOAD DATA LOCAL INPATH '../../data/files/kv1.txt' INTO TABLE part_tab2 PARTITION (ds='2009-04-09');

dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/dbcascade/part_tab3;
dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/dbcascade/part_tab3_p1;
-- add a partitioned table, view with a different storage location
CREATE TABLE part_tab3 (id INT, name STRING)  PARTITIONED BY (ds string)
		LOCATION 'file:${system:test.tmp.dir}/dbcascade/part_tab3';
ALTER TABLE part_tab3 ADD PARTITION  (ds='2007-04-09') LOCATION 'file:${system:test.tmp.dir}/dbcascade/part_tab3_p1';
LOAD DATA LOCAL INPATH '../../data/files/kv1.txt' INTO TABLE part_tab3 PARTITION (ds='2008-04-09');
LOAD DATA LOCAL INPATH '../../data/files/kv1.txt' INTO TABLE part_tab3 PARTITION (ds='2009-04-09');


dfs ${system:test.dfs.mkdir} ${system:test.tmp.dir}/dbcascade/extab1;
dfs -touchz ${system:test.tmp.dir}/dbcascade/extab1/file1.txt;
-- add an external table
CREATE EXTERNAL TABLE extab1(id INT, name STRING) ROW FORMAT
              DELIMITED FIELDS TERMINATED BY ''
              LINES TERMINATED BY '\n'
              STORED AS TEXTFILE
              LOCATION 'file:${system:test.tmp.dir}/dbcascade/extab1';

-- add a table
CREATE TABLE temp_tbl3 (id INT, name STRING);
LOAD DATA LOCAL INPATH '../../data/files/kv1.txt' into table temp_tbl3;

-- drop the database with cascade
DROP DATABASE db5 CASCADE;

dfs -test -d ${system:test.tmp.dir}/dbcascade/extab1;
dfs -rmr ${system:test.tmp.dir}/dbcascade;
