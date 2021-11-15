SET hive.blobstore.optimizations.enabled=true;
-- SORT_QUERY_RESULTS

-- Single partition with buckets
DROP TABLE table1;
CREATE TABLE table1 (id int) partitioned by (key string) clustered by (id) into 2 buckets LOCATION '${hiveconf:test.blobstore.path.unique}/table1/';
INSERT OVERWRITE TABLE table1 PARTITION (key) VALUES (1, '101'), (2, '202'), (3, '303'), (4, '404'), (5, '505');
SELECT * FROM table1;
INSERT OVERWRITE TABLE table1 PARTITION (key) VALUES (1, '101'), (2, '202'), (3, '303'), (4, '404'), (5, '505');
SELECT * FROM table1;
EXPLAIN EXTENDED INSERT OVERWRITE TABLE table1 PARTITION (key) VALUES (1, '101'), (2, '202'), (3, '303'), (4, '404'), (5, '505');
DROP TABLE table1;

-- Multiple partitions
CREATE TABLE table1 (name string, age int) PARTITIONED BY (country string, state string);
INSERT INTO table1 PARTITION (country='USA', state='CA') values ('John Doe', 23), ('Jane Doe', 22);
INSERT INTO table1 PARTITION (country='USA', state='CA') values ('Mark Cage', 38), ('Mirna Cage', 37);
INSERT INTO table1 PARTITION (country='USA', state='TX') values ('Bill Rose', 52), ('Maria Full', 50);
CREATE TABLE table2 (name string, age int) PARTITIONED BY (country string, state string) LOCATION '${hiveconf:test.blobstore.path.unique}/table2/';
INSERT OVERWRITE TABLE table2 PARTITION (country, state) SELECT * FROM table1;
SHOW PARTITIONS table2;
SELECT * FROM table2;
INSERT OVERWRITE TABLE table2 PARTITION (country, state) SELECT * FROM table1 WHERE age < 30;
SHOW PARTITIONS table2;
SELECT * FROM table2;
INSERT OVERWRITE TABLE table2 PARTITION (country='MEX', state) VALUES ('Peter Mo', 87, 'SON');
SHOW PARTITIONS table2;
SELECT * FROM table2;
DROP TABLE table2;
DROP TABLE table1;
