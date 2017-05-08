DROP TABLE IF EXISTS dec;

CREATE TABLE dec(name string, value decimal(8,4));

LOAD DATA LOCAL INPATH '../../data/files/dec.txt' into TABLE dec;

ANALYZE TABLE dec COMPUTE STATISTICS FOR COLUMNS value;
DESC FORMATTED dec value;

DROP TABLE IF EXISTS avro_dec;

CREATE TABLE `avro_dec`(
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

DESC avro_dec;

INSERT OVERWRITE TABLE avro_dec select name, value from dec;

SELECT * FROM avro_dec;

DROP TABLE IF EXISTS avro_dec1;

CREATE TABLE `avro_dec1`(
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

DESC avro_dec1;

LOAD DATA LOCAL INPATH '../../data/files/dec.avro' into TABLE avro_dec1;

select value from avro_dec1;

DROP TABLE dec;
DROP TABLE avro_dec;
DROP TABLE avro_dec1;
