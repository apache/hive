-- This test does not test encrypted data, but it makes sure that external tables out of HDFS can
-- be queried due to internal encryption functions;

DROP TABLE mydata;

CREATE EXTERNAL TABLE mydata (key STRING, value STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '
LOCATION 'pfile://${system:test.tmp.dir}/external_mydata';

LOAD DATA LOCAL INPATH '../../data/files/kv1.txt' OVERWRITE INTO TABLE mydata;

SELECT * from mydata;

DROP TABLE mydata;