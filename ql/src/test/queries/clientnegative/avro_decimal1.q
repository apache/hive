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
  'avro.schema.literal'='{\"namespace\":\"com.howdy\",\"name\":\"some_schema\",\"type\":\"record\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"value\",\"type\":{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":"-1",\"scale\":"6"}}]}'
);
