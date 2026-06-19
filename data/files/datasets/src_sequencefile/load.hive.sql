CREATE TABLE src_sequencefile (key STRING COMMENT 'default', value STRING COMMENT 'default') STORED AS SEQUENCEFILE;

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/kv1.seq" INTO TABLE src_sequencefile;

ANALYZE TABLE src_sequencefile COMPUTE STATISTICS;

ANALYZE TABLE src_sequencefile COMPUTE STATISTICS FOR COLUMNS key,value;
