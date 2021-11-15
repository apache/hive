--! qt:dataset:src
SET hive.vectorized.execution.enabled=false;
set hive.mapred.mode=nonstrict;
CREATE TABLE t1_n131 (c1 BIGINT, c2 STRING);

CREATE TABLE t2_n78 (c1 INT, c2 STRING)
PARTITIONED BY (p1 STRING);

LOAD DATA LOCAL INPATH '../../data/files/dynamic_partition_insert.txt' INTO TABLE t1_n131;
LOAD DATA LOCAL INPATH '../../data/files/dynamic_partition_insert.txt' INTO TABLE t1_n131;
LOAD DATA LOCAL INPATH '../../data/files/dynamic_partition_insert.txt' INTO TABLE t1_n131;
LOAD DATA LOCAL INPATH '../../data/files/dynamic_partition_insert.txt' INTO TABLE t1_n131;
LOAD DATA LOCAL INPATH '../../data/files/dynamic_partition_insert.txt' INTO TABLE t1_n131;

INSERT OVERWRITE TABLE t2_n78 partition(p1) SELECT *,c1 AS p1 FROM t1_n131 DISTRIBUTE BY p1;
SELECT * FROM t2_n78;
-- no partition spec
TRUNCATE TABLE t2_n78;
INSERT OVERWRITE TABLE t2_n78 SELECT *,c1 AS p1 FROM t1_n131 DISTRIBUTE BY p1;
SHOW PARTITIONS t2_n78;
SELECT * FROM t2_n78;

DROP TABLE t1_n131;
DROP TABLE t2_n78;

-- Single partition with buckets
CREATE TABLE table1_n15 (id int) partitioned by (key string) clustered by (id) into 2 buckets ;
-- without partition schema
INSERT INTO TABLE table1_n15 VALUES (1, '101'), (2, '202'), (3, '303'), (4, '404'), (5, '505');
SHOW PARTITIONS table1_n15;
SELECT * FROM table1_n15;
DROP TABLE table1_n15;

-- Multiple partitions
CREATE TABLE table1_n15 (name string, age int) PARTITIONED BY (country string, state string);
INSERT INTO table1_n15 values ('John Doe', 23, 'USA', 'CA'), ('Jane Doe', 22, 'USA', 'TX');
SHOW PARTITIONS table1_n15;

CREATE TABLE table2_n10 (name string, age int) PARTITIONED BY (country string, state string);
INSERT INTO TABLE table2_n10 SELECT * FROM table1_n15;
SHOW PARTITIONS table2_n10;
SELECT * FROM table2_n10;
DROP TABLE table2_n10;
DROP TABLE table1_n15;

CREATE TABLE dest1_n143(key string) partitioned by (value string);
CREATE TABLE dest2_n37(key string) partitioned by (value string);
FROM src
INSERT OVERWRITE TABLE dest1_n143 SELECT src.* WHERE src.key < 100
INSERT OVERWRITE TABLE dest2_n37 SELECT src.key, src.value WHERE src.key >= 100 and src.key < 200;

SELECT distinct value FROM SRC WHERE src.key < 100;
SHOW PARTITIONS dest1_n143;
SELECT distinct value FROM SRC WHERE src.key >= 100 and src.key < 200;
SHOW PARTITIONS dest2_n37;
DROP TABLE dest1_n143;
DROP TABLE dest2_n37;

-- partitio spec within column spec
CREATE TABLE table1_n15 (name string, age int) PARTITIONED BY (country string, state string);
INSERT INTO table1_n15 values ('John Doe', 23, 'USA', 'CA'), ('Jane Doe', 22, 'USA', 'TX');
SHOW PARTITIONS table1_n15;

CREATE TABLE table2_n10 (name string, age int) PARTITIONED BY (country string, state string);

-- full partition schema with column schema
INSERT INTO TABLE table2_n10(age, name, country, state) SELECT age, name, country, state FROM table1_n15;
SHOW PARTITIONS table2_n10;
SELECT  * from table2_n10;

-- only partition schema
INSERT INTO TABLE table2_n10(state, country) SELECT state, country FROM table1_n15;
SHOW PARTITIONS table2_n10;
SELECT  * from table2_n10;

-- full column schema with partial partition schema
INSERT INTO TABLE table2_n10(age, name, country) SELECT age, name, country FROM table1_n15;
SHOW PARTITIONS table2_n10;
SELECT  * from table2_n10;

-- partial column schema with partial partition schema
INSERT INTO TABLE table2_n10( name, country) SELECT name, country FROM table1_n15;
SHOW PARTITIONS table2_n10;
SELECT  * from table2_n10;

DROP TABLE table2_n10;
DROP TABLE table1_n15;

CREATE TABLE pageviews (userid VARCHAR(64), link STRING, source STRING) PARTITIONED BY (datestamp STRING, i int)
    CLUSTERED BY (userid) INTO 256 BUCKETS STORED AS ORC;
INSERT INTO TABLE pageviews PARTITION (datestamp='2014-09-23',i)(userid,i,link) VALUES ('jsmith', 7, '7mail.com');
SHOW PARTITIONS pageviews;
select * from pageviews;

drop database if exists x314n cascade;
