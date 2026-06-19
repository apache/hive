SET mapreduce.job.ubertask.enable=true;
SET mapreduce.job.ubertask.maxreduces=1;
SET mapred.reduce.tasks=1;

-- Uberized mode is a YARN option, ignore this test for non-YARN Hadoop versions

CREATE TABLE T1_n136(key STRING, val STRING);
LOAD DATA LOCAL INPATH '../../data/files/T1.txt' INTO TABLE T1_n136;

SELECT count(*) FROM T1_n136;
