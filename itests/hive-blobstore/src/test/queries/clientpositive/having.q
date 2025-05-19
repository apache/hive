-- Test HAVING clause

DROP TABLE having_blobstore_test;
CREATE TABLE having_blobstore_test(a int, b int, c string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' '
LOCATION '${hiveconf:test.blobstore.path.unique}/having_blobstore/having_blobstore_test';
LOAD DATA LOCAL INPATH '../../data/files/having_data.txt' INTO TABLE having_blobstore_test;

SELECT a, COUNT(b) AS ct FROM having_blobstore_test GROUP BY a HAVING ct > 1;
SELECT a, MAX(b) FROM having_blobstore_test GROUP BY a HAVING MAX(b) > 9;
SELECT a, MAX(b), MAX(c) FROM having_blobstore_test GROUP BY a HAVING MIN(c) < 'b';
