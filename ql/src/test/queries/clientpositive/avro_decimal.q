DROP TABLE IF EXISTS `dec_n0`;

CREATE TABLE `dec_n0`(name string, value decimal(8,4));

LOAD DATA LOCAL INPATH '../../data/files/dec.txt' into TABLE `dec_n0`;

ANALYZE TABLE `dec_n0` COMPUTE STATISTICS FOR COLUMNS value;
DESC FORMATTED `dec_n0` value;

DROP TABLE IF EXISTS avro_dec_n0;

CREATE TABLE `avro_dec_n0`(
  `name` string COMMENT 'from deserializer',
  `value` decimal(5,2) COMMENT 'from deserializer')
COMMENT 'just drop the schema right into the HQL'
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
TBLPROPERTIES (
  'numFiles'='1',
  'avro.schema.literal'='{\"namespace\":\"com.howdy\",\"name\":\"some_schema\",\"type\":\"record\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"value\",\"type\":{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":5,\"scale\":2}}]}'
);

DESC avro_dec_n0;

INSERT OVERWRITE TABLE avro_dec_n0 select name, value from `dec_n0`;

SELECT * FROM avro_dec_n0;

DROP TABLE IF EXISTS avro_dec1_n0;

CREATE TABLE `avro_dec1_n0`(
  `name` string COMMENT 'from deserializer',
  `value` decimal(4,1) COMMENT 'from deserializer')
COMMENT 'just drop the schema right into the HQL'
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
TBLPROPERTIES (
  'numFiles'='1',
  'avro.schema.literal'='{\"namespace\":\"com.howdy\",\"name\":\"some_schema\",\"type\":\"record\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"value\",\"type\":{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":4,\"scale\":1}}]}'
);

DESC avro_dec1_n0;

LOAD DATA LOCAL INPATH '../../data/files/dec.avro' into TABLE avro_dec1_n0;

select value from avro_dec1_n0;

DROP TABLE `dec_n0`;
DROP TABLE avro_dec_n0;
DROP TABLE avro_dec1_n0;

CREATE TABLE test_quoted_scale_precision
STORED AS AVRO
TBLPROPERTIES ('avro.schema.literal'='{"type":"record","name":"DecimalTest","namespace":"com.example.test","fields":[{"name":"Decimal24_6","type":["null",{"type":"bytes","logicalType":"decimal","precision":"24","scale":"6"}]}]}');
show create table test_quoted_scale_precision;
desc test_quoted_scale_precision;
DROP TABLE test_quoted_scale_precision;
