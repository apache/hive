set hive.blobstore.use.blobstore.as.scratchdir=true;

DROP TABLE qtest;
CREATE TABLE qtest (value int) LOCATION '${hiveconf:test.blobstore.path.unique}/qtest/';
INSERT INTO qtest VALUES (1), (10), (100), (1000);
EXPLAIN EXTENDED INSERT INTO qtest VALUES (1), (10), (100), (1000);
SELECT * FROM qtest;
