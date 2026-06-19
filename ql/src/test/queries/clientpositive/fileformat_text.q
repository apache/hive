--! qt:dataset:src
EXPLAIN
CREATE TABLE dest1_n107(key INT, value STRING) STORED AS
  INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat';

CREATE TABLE dest1_n107(key INT, value STRING) STORED AS
  INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat';

DESCRIBE EXTENDED dest1_n107;

FROM src
INSERT OVERWRITE TABLE dest1_n107 SELECT src.key, src.value WHERE src.key < 10;

SELECT dest1_n107.* FROM dest1_n107;


