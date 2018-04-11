set hive.mapred.mode=nonstrict;
CREATE TABLE t1 (c1 BIGINT, c2 STRING);

CREATE TABLE t2 (c1 INT, c2 STRING)
PARTITIONED BY (p1 STRING);

LOAD DATA LOCAL INPATH '../../data/files/dynamic_partition_insert.txt' INTO TABLE t1;
LOAD DATA LOCAL INPATH '../../data/files/dynamic_partition_insert.txt' INTO TABLE t1;
LOAD DATA LOCAL INPATH '../../data/files/dynamic_partition_insert.txt' INTO TABLE t1;
LOAD DATA LOCAL INPATH '../../data/files/dynamic_partition_insert.txt' INTO TABLE t1;
LOAD DATA LOCAL INPATH '../../data/files/dynamic_partition_insert.txt' INTO TABLE t1;

SET hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE t2 partition(p1) SELECT *,c1 AS p1 FROM t1 DISTRIBUTE BY p1;
SELECT * FROM t2;
-- no partition spec
TRUNCATE TABLE t2;
INSERT OVERWRITE TABLE t2 SELECT *,c1 AS p1 FROM t1 DISTRIBUTE BY p1;
SHOW PARTITIONS t2;
SELECT * FROM t2;

DROP TABLE t1;
DROP TABLE t2;

-- Single partition with buckets
CREATE TABLE table1 (id int) partitioned by (key string) clustered by (id) into 2 buckets ;
-- without partition schema
INSERT INTO TABLE table1 VALUES (1, '101'), (2, '202'), (3, '303'), (4, '404'), (5, '505');
SHOW PARTITIONS table1;
SELECT * FROM table1;
DROP TABLE table1;

-- Multiple partitions
CREATE TABLE table1 (name string, age int) PARTITIONED BY (country string, state string);
INSERT INTO table1 values ('John Doe', 23, 'USA', 'CA'), ('Jane Doe', 22, 'USA', 'TX');
SHOW PARTITIONS table1;

CREATE TABLE table2 (name string, age int) PARTITIONED BY (country string, state string);
INSERT INTO TABLE table2 SELECT * FROM table1;
SHOW PARTITIONS table2;
SELECT * FROM table2;
DROP TABLE table2;
DROP TABLE table1;

CREATE TABLE dest1(key string) partitioned by (value string);
CREATE TABLE dest2(key string) partitioned by (value string);
FROM src
INSERT OVERWRITE TABLE dest1 SELECT src.* WHERE src.key < 100
INSERT OVERWRITE TABLE dest2 SELECT src.key, src.value WHERE src.key >= 100 and src.key < 200;

SELECT distinct value FROM SRC WHERE src.key < 100;
SHOW PARTITIONS dest1;
SELECT distinct value FROM SRC WHERE src.key >= 100 and src.key < 200;
SHOW PARTITIONS dest2;
DROP TABLE dest1;
DROP TABLE dest2;
