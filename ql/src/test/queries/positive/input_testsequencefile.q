--! qt:dataset:src

--
-- Table dest4_sequencefile
--
DROP TABLE IF EXISTS dest4_sequencefile;

CREATE TABLE dest4_sequencefile (key STRING COMMENT 'default', value STRING COMMENT 'default')
STORED AS
INPUTFORMAT 'org.apache.hadoop.mapred.SequenceFileInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.mapred.SequenceFileOutputFormat';

FROM src
INSERT OVERWRITE TABLE dest4_sequencefile SELECT src.key, src.value
