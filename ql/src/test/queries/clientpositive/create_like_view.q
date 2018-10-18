--! qt:dataset:srcpart
--! qt:dataset:src
-- SORT_QUERY_RESULTS

DROP TABLE IF EXISTS table1_n14;
DROP TABLE IF EXISTS table2_n9;
DROP TABLE IF EXISTS table3_n2;
DROP VIEW IF EXISTS view1_n1;

CREATE TABLE table1_n14 (a STRING, b STRING) STORED AS TEXTFILE;
DESCRIBE table1_n14;
DESCRIBE FORMATTED table1_n14;

CREATE VIEW view1_n1 AS SELECT * FROM table1_n14;

CREATE TABLE table2_n9 LIKE view1_n1;
DESCRIBE table2_n9;
DESCRIBE FORMATTED table2_n9;

CREATE TABLE IF NOT EXISTS table2_n9 LIKE view1_n1;

CREATE EXTERNAL TABLE IF NOT EXISTS table2_n9 LIKE view1_n1;

CREATE EXTERNAL TABLE IF NOT EXISTS table3_n2 LIKE view1_n1;
DESCRIBE table3_n2;
DESCRIBE FORMATTED table3_n2;

INSERT OVERWRITE TABLE table1_n14 SELECT key, value FROM src WHERE key = 86;
INSERT OVERWRITE TABLE table2_n9 SELECT key, value FROM src WHERE key = 100;

SELECT * FROM table1_n14;
SELECT * FROM table2_n9;

DROP TABLE table1_n14;
DROP TABLE table2_n9;
DROP VIEW view1_n1;

-- check partitions
create view view1_n1 partitioned on (ds, hr) as select * from srcpart;
create table table1_n14 like view1_n1;
describe formatted table1_n14;
DROP TABLE table1_n14;
DROP VIEW view1_n1;