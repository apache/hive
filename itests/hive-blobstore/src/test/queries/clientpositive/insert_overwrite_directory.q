SET hive.blobstore.optimizations.enabled=true;
SET hive.blobstore.use.blobstore.as.scratchdir=true;

-- Create a simple source table;
DROP TABLE table1;
CREATE TABLE table1 (id int, key string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
INSERT INTO TABLE table1 VALUES (1, 'k1');
INSERT INTO TABLE table1 VALUES (2, 'k2');

-- Write and verify data on the directory;
INSERT OVERWRITE DIRECTORY '${hiveconf:test.blobstore.path.unique}/table1.dir/' SELECT * FROM table1;
dfs -cat ${hiveconf:test.blobstore.path.unique}/table1.dir/000000_0;

-- Write and verify data using FROM ... INSERT OVERWRITE DIRECTORY;
FROM table1
INSERT OVERWRITE DIRECTORY '${hiveconf:test.blobstore.path.unique}/table1.dir/' SELECT id
INSERT OVERWRITE DIRECTORY '${hiveconf:test.blobstore.path.unique}/table2.dir/' SELECT key;

dfs -cat ${hiveconf:test.blobstore.path.unique}/table1.dir/000000_0;
dfs -cat ${hiveconf:test.blobstore.path.unique}/table2.dir/000000_0;

-- Verify plan is optimizedl
EXPLAIN EXTENDED INSERT OVERWRITE DIRECTORY '${hiveconf:test.blobstore.path.unique}/table1.dir/' SELECT * FROM table1;

EXPLAIN EXTENDED FROM table1
                 INSERT OVERWRITE DIRECTORY '${hiveconf:test.blobstore.path.unique}/table1.dir/' SELECT id
                 INSERT OVERWRITE DIRECTORY '${hiveconf:test.blobstore.path.unique}/table2.dir/' SELECT key;