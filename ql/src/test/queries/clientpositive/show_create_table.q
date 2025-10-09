CREATE TABLE TEST(
  col1 varchar(100) NOT NULL COMMENT "comment for column 1",
  col2 timestamp DEFAULT CURRENT_TIMESTAMP() COMMENT "comment for column 2",
  `col 3` decimal CHECK (`col 3` + col4 > 1) enable novalidate rely,
  col4 decimal NOT NULL,
  col5 varchar(100),
  primary key(col1, col2) disable novalidate rely,
  constraint c3_c4_check CHECK((`col 3` + col4)/(`col 3` - col4) > 3) enable novalidate norely,
  constraint c4_unique UNIQUE(col4) disable novalidate rely)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat';

CREATE TABLE TEST2(
 col varchar(100),
 primary key(col) disable novalidate rely)
ROW FORMAT SERDE
'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat';

CREATE TABLE TEST3(
  col1 varchar(100) COMMENT "comment",
  col2 timestamp,
  col3 varchar(100),
  foreign key(col1, col2) references TEST(col1, col2) disable novalidate rely,
  foreign key(col3) references TEST2(col) disable novalidate norely)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat';

SHOW CREATE TABLE TEST;

SHOW CREATE TABLE TEST2;

SHOW CREATE TABLE TEST3;

CREATE TABLE TEST_RESERVED (
`member_nr` varchar(8),
`plan_nr` varchar(11),
`timestamp` timestamp,
`shared_ind` varchar(1))
CLUSTERED BY (
member_nr,
plan_nr,
`timestamp`)
INTO 4 BUCKETS;

SHOW CREATE TABLE TEST_RESERVED;
