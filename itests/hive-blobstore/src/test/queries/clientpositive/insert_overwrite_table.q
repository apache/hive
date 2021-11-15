SET hive.blobstore.optimizations.enabled=true;
SET hive.blobstore.use.blobstore.as.scratchdir=true;

-- Insert unpartitioned table;
DROP TABLE table1;
CREATE TABLE table1 (id int) LOCATION '${hiveconf:test.blobstore.path.unique}/table1/';
INSERT OVERWRITE TABLE table1 VALUES (1);
SELECT * FROM table1;
INSERT OVERWRITE TABLE table1 VALUES (2);
SELECT * FROM table1;
EXPLAIN EXTENDED INSERT OVERWRITE TABLE table1 VALUES (1);
DROP TABLE table1;