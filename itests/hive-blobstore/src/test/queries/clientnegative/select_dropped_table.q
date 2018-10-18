CREATE TABLE qtest (key STRING, value STRING)
LOCATION '${hiveconf:test.blobstore.path.unique}/qtest';
DROP TABLE qtest;
SELECT * FROM qtest;
