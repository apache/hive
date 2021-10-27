
CREATE TABLE TEST(
  col1 varchar(100) NOT NULL COMMENT "comment for column 1",
  col2 timestamp DEFAULT CURRENT_TIMESTAMP() COMMENT "comment for column 2",
  col3 decimal,
  col4 varchar(512) NOT NULL,
  col5 varchar(100),
  primary key(col1, col2) disable novalidate)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat';

SHOW CREATE TABLE TEST;
