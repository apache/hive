--
-- Table src
--
DROP TABLE IF EXISTS src;

CREATE TABLE src (key STRING COMMENT 'default', value STRING COMMENT 'default') STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH "${hiveconf:test.data.dir}/kv1.txt" INTO TABLE src;

ANALYZE TABLE src COMPUTE STATISTICS;

ANALYZE TABLE src COMPUTE STATISTICS FOR COLUMNS key,value;

--
-- Table dest1
--
DROP TABLE IF EXISTS dest1;

CREATE TABLE dest1 (key STRING COMMENT 'default', value STRING COMMENT 'default')
STORED AS
INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

