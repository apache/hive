--
-- Table testdataset
--
CREATE TABLE testdataset (key STRING COMMENT 'default', value STRING COMMENT 'default') STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/kv1.txt" INTO TABLE testdataset;

ANALYZE TABLE testdataset COMPUTE STATISTICS;

ANALYZE TABLE testdataset COMPUTE STATISTICS FOR COLUMNS key,value;
