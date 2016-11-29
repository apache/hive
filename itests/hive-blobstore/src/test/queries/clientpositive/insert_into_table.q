SET hive.blobstore.optimizations.enabled=true;
SET hive.blobstore.use.blobstore.as.scratchdir=true;
set hive.exec.dynamic.partition.mode=nonstrict;

-- Insert unpartitioned table;
DROP TABLE table1;
CREATE TABLE table1 (id int) LOCATION '${hiveconf:test.blobstore.path.unique}/table1/';
INSERT INTO TABLE table1 VALUES (1);
INSERT INTO TABLE table1 VALUES (2);
SELECT * FROM table1;
EXPLAIN EXTENDED INSERT INTO TABLE table1 VALUES (1);
DROP TABLE table1;

-- Insert dynamic partitions;
CREATE TABLE table1 (id int) partitioned by (key string) clustered by (id) into 2 buckets LOCATION '${hiveconf:test.blobstore.path.unique}/table1/';
INSERT INTO TABLE table1 PARTITION (key) VALUES (1, '101'), (2, '202'), (3, '303'), (4, '404'), (5, '505');
INSERT INTO TABLE table1 PARTITION (key) VALUES (1, '101'), (2, '202'), (3, '303'), (4, '404'), (5, '505');
SELECT * FROM table1;
EXPLAIN EXTENDED INSERT INTO TABLE table1 PARTITION (key) VALUES (1, '101'), (2, '202'), (3, '303'), (4, '404'), (5, '505');
DROP TABLE table1;