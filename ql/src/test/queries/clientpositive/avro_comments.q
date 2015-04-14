-- verify Avro columns comments
DROP TABLE IF EXISTS testAvroComments1;

CREATE TABLE testAvroComments1
ROW FORMAT
SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
TBLPROPERTIES ('avro.schema.literal'='{
  "namespace": "testing.hive.avro.serde",
  "name": "doctors",
  "type": "record",
  "fields": [
    {
      "name":"number",
      "type":"int",
      "doc":"Order of playing the role"
    },
    {
      "name":"first_name",
      "type":"string",
      "doc":"first name of actor playing role"
    },
    {
      "name":"last_name",
      "type":"string",
      "doc":"last name of actor playing role"
    },
    {
      "name":"extra_field",
      "type":"string",
      "doc":"an extra field not in the original file",
      "default":"fishfingers and custard"
    }
  ]
}');

DESCRIBE testAvroComments1;
DROP TABLE testAvroComments1;

DROP TABLE IF EXISTS testAvroComments2;
CREATE TABLE testAvroComments2
ROW FORMAT
SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
TBLPROPERTIES ('avro.schema.literal'='{
  "namespace": "testing.hive.avro.serde",
  "name": "doctors",
  "type": "record",
  "fields": [
    {
      "name":"number",
      "type":"int",
      "doc":"Order of playing the role"
    },
    {
      "name":"first_name",
      "type":"string"
    },
    {
      "name":"last_name",
      "type":"string",
      "doc":"last name of actor playing role"
    },
    {
      "name":"extra_field",
      "type":"string",
      "default":"fishfingers and custard"
    }
  ]
}');

DESCRIBE testAvroComments2;
DROP TABLE testAvroComments2;

DROP TABLE IF EXISTS testAvroComments3;
CREATE TABLE testAvroComments3
ROW FORMAT
SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
TBLPROPERTIES ('avro.schema.literal'='{
  "namespace": "testing.hive.avro.serde",
  "name": "doctors",
  "type": "record",
  "fields": [
    {
      "name":"number",
      "type":"int"
    },
    {
      "name":"first_name",
      "type":"string"
    },
    {
      "name":"last_name",
      "type":"string"
    },
    {
      "name":"extra_field",
      "type":"string",
      "default":"fishfingers and custard"
    }
  ]
}');

DESCRIBE testAvroComments3;
DROP TABLE testAvroComments3;

DROP TABLE IF EXISTS testAvroComments4;

CREATE TABLE testAvroComments4 (
  number int COMMENT "Order of playing the role",
  first_name string COMMENT "first name of actor playing role",
  last_name string COMMENT "last name of actor playing role",
  extra_field string COMMENT "an extra field not in the original file")
STORED AS AVRO;

DESCRIBE testAvroComments4;
DROP TABLE testAvroComments4;

DROP TABLE IF EXISTS testAvroComments5;

CREATE TABLE testAvroComments5 (
  number int COMMENT "Order of playing the role",
  first_name string,
  last_name string COMMENT "last name of actor playing role",
  extra_field string)
STORED AS AVRO;

DESCRIBE testAvroComments5;
DROP TABLE testAvroComments5;

DROP TABLE IF EXISTS testAvroComments6;

CREATE TABLE testAvroComments6 (
  number int,
  first_name string,
  last_name string,
  extra_field string)
STORED AS AVRO;

DESCRIBE testAvroComments6;
DROP TABLE testAvroComments6;


