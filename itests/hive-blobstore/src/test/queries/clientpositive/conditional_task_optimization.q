-- This test case verifies that the CONDITIONAL TASK added to the query plan is optimized;
-- for blobstorage systems. The CONDITIONAL TASK usually has two concatenated renames (or MoveTask) on;
-- HDFS systems, but it must have only one rename on Blobstorage;

SET hive.blobstore.use.blobstore.as.scratchdir=true;

DROP TABLE conditional;
CREATE TABLE conditional (id int) LOCATION '${hiveconf:test.blobstore.path.unique}/conditional/';

-- Disable optimization;
SET hive.blobstore.optimizations.enabled=false;
EXPLAIN INSERT INTO TABLE conditional VALUES (1);
INSERT INTO TABLE conditional VALUES (1);
SELECT * FROM conditional;
EXPLAIN INSERT OVERWRITE TABLE conditional VALUES (11);
INSERT OVERWRITE TABLE conditional VALUES (11);
SELECT * FROM conditional;

-- Enable optimization;
SET hive.blobstore.optimizations.enabled=true;
EXPLAIN INSERT INTO TABLE conditional VALUES (2);
INSERT INTO TABLE conditional VALUES (2);
SELECT * FROM conditional;
EXPLAIN INSERT OVERWRITE TABLE conditional VALUES (22);
INSERT OVERWRITE TABLE conditional VALUES (22);
SELECT * FROM conditional;

DROP TABLE conditional;