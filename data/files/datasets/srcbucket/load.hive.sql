CREATE TABLE srcbucket_tmp (key INT, value STRING) STORED AS TEXTFILE;
CREATE TABLE srcbucket (key INT, value STRING)
CLUSTERED BY (key) INTO 2 BUCKETS
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/bucketed_files/000000_0" INTO TABLE srcbucket_tmp;
LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/bucketed_files/000001_0" INTO TABLE srcbucket_tmp;
INSERT INTO srcbucket SELECT * FROM srcbucket_tmp;
DROP TABLE srcbucket_tmp;

ANALYZE TABLE srcbucket COMPUTE STATISTICS;

ANALYZE TABLE srcbucket COMPUTE STATISTICS FOR COLUMNS key,value;
