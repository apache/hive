CREATE TABLE srcbucket_tmp (key INT, value STRING);
CREATE TABLE srcbucket2 (key INT, value STRING)
CLUSTERED BY (key) INTO 4 BUCKETS
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/bmj/000000_0" INTO TABLE srcbucket_tmp;
LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/bmj/000001_0" INTO TABLE srcbucket_tmp;
LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/bmj/000002_0" INTO TABLE srcbucket_tmp;
LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/bmj/000003_0" INTO TABLE srcbucket_tmp;
INSERT INTO srcbucket2 SELECT * FROM srcbucket_tmp;
DROP TABLE srcbucket_tmp;

ANALYZE TABLE srcbucket2 COMPUTE STATISTICS;

ANALYZE TABLE srcbucket2 COMPUTE STATISTICS FOR COLUMNS key,value;
