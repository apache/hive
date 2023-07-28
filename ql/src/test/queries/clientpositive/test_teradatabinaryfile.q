DROP TABLE if exists teradata_binary_table_64kb;
DROP TABLE if exists teradata_binary_table_1mb;
DROP TABLE if exists teradata_binary_table_64kb_insert;
DROP TABLE if exists teradata_binary_table_1mb_insert;


CREATE TABLE `teradata_binary_table_64kb`(
  `test_tinyint` tinyint,
  `test_smallint` smallint,
  `test_int` int,
  `test_bigint` bigint,
  `test_double` double,
  `test_decimal` decimal(15,2),
  `test_date` date,
  `test_timestamp` timestamp,
  `test_char` char(1),
  `test_varchar` varchar(40),
  `test_binary` binary
 )
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.teradata.TeradataBinarySerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.TeradataBinaryFileInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.TeradataBinaryFileOutputFormat'
TBLPROPERTIES (
  'teradata.timestamp.precision'='0',
  'teradata.char.charset'='LATIN',
  'teradata.row.length'='64KB'
);

CREATE TABLE `teradata_binary_table_1mb`(
  `test_tinyint` tinyint,
  `test_smallint` smallint,
  `test_int` int,
  `test_bigint` bigint,
  `test_double` double,
  `test_decimal` decimal(15,2),
  `test_date` date,
  `test_timestamp` timestamp,
  `test_char` char(1),
  `test_varchar` varchar(40),
  `test_binary` binary
 )
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.teradata.TeradataBinarySerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.TeradataBinaryFileInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.TeradataBinaryFileOutputFormat'
TBLPROPERTIES (
  'teradata.timestamp.precision'='6',
  'teradata.char.charset'='UNICODE',
  'teradata.row.length'='1MB'
);

CREATE TABLE `teradata_binary_table_64kb_insert`(
  `test_tinyint` tinyint,
  `test_decimal` decimal(15,2),
  `test_date` date,
  `test_timestamp` timestamp
 )
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.teradata.TeradataBinarySerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.TeradataBinaryFileInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.TeradataBinaryFileOutputFormat'
TBLPROPERTIES (
  'teradata.timestamp.precision'='0',
  'teradata.char.charset'='LATIN',
  'teradata.row.length'='64KB'
);

CREATE TABLE `teradata_binary_table_1mb_insert`(
  `test_tinyint` tinyint,
  `test_int` int
 )
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.teradata.TeradataBinarySerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.TeradataBinaryFileInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.TeradataBinaryFileOutputFormat'
TBLPROPERTIES (
  'teradata.timestamp.precision'='6',
  'teradata.char.charset'='UNICODE',
  'teradata.row.length'='1MB'
);

LOAD DATA LOCAL INPATH '../../data/files/teradata_binary_file/teradata_binary_table.deflate' OVERWRITE INTO TABLE teradata_binary_table_64kb;
LOAD DATA LOCAL INPATH '../../data/files/teradata_binary_file/td_data_with_1mb_rowsize.teradata.gz' OVERWRITE INTO TABLE teradata_binary_table_1mb;

SELECT * from teradata_binary_table_64kb;
SELECT * from teradata_binary_table_1mb;

SELECT COUNT(*) FROM teradata_binary_table_64kb;
SELECT COUNT(*) FROM teradata_binary_table_1mb;

SELECT max(date_format(test_timestamp, 'y')) FROM teradata_binary_table_64kb;
SELECT max(date_format(test_date, 'y')) FROM teradata_binary_table_64kb;
SELECT max(Floor(test_decimal)) FROM teradata_binary_table_64kb;

SELECT max(date_format(test_timestamp, 'y')) FROM teradata_binary_table_1mb;
SELECT max(date_format(test_date, 'y')) FROM teradata_binary_table_1mb;
SELECT max(Floor(test_decimal)) FROM teradata_binary_table_1mb;

SELECT test_tinyint, MAX(test_decimal) FROM teradata_binary_table_64kb GROUP BY test_tinyint;
SELECT test_tinyint, MAX(test_decimal) FROM teradata_binary_table_1mb GROUP BY test_tinyint;

INSERT OVERWRITE TABLE teradata_binary_table_64kb_insert
SELECT test_tinyint, test_decimal, test_date, test_timestamp FROM teradata_binary_table_64kb;

INSERT OVERWRITE TABLE teradata_binary_table_1mb_insert
SELECT 1, 15;

DESC FORMATTED teradata_binary_table_64kb_insert;
DESC FORMATTED teradata_binary_table_1mb_insert;

DROP TABLE if exists teradata_binary_table_64kb;
DROP TABLE if exists teradata_binary_table_1mb;
DROP TABLE if exists teradata_binary_table_64kb_insert;
DROP TABLE if exists teradata_binary_table_1mb_insert;
