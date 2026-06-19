-- Check the queries work fine with the following property set to true
SET hive.optimize.update.table.properties.from.serde=true;

dfs -cp ${system:hive.root}data/files/table1.avsc ${system:test.tmp.dir}/avro_tableproperty_optimize.avsc;

CREATE TABLE avro_extschema_literal_n0
STORED AS AVRO
TBLPROPERTIES ('avro.schema.literal'='{
  "namespace": "org.apache.hive",
  "name": "ext_schema",
  "type": "record",
  "fields": [
    { "name":"col1", "type":"string" },
    { "name":"col2", "type":"long" },
    { "name":"col3", "type":"string" }
  ] }');
INSERT INTO TABLE avro_extschema_literal_n0 VALUES('s1', 1, 's2');

DESCRIBE EXTENDED avro_extschema_literal_n0;
SELECT * FROM avro_extschema_literal_n0;

CREATE TABLE avro_extschema_url_n0
STORED AS AVRO
TBLPROPERTIES ('avro.schema.url'='${system:test.tmp.dir}/avro_tableproperty_optimize.avsc');
INSERT INTO TABLE avro_extschema_url_n0 VALUES('s1', 1, 's2');

DESCRIBE EXTENDED avro_extschema_url_n0;
SELECT * FROM avro_extschema_url_n0;

CREATE TABLE avro_extschema_literal1
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
WITH SERDEPROPERTIES (
'avro.schema.literal'='{
  "namespace": "org.apache.hive",
  "name": "ext_schema",
  "type": "record",
  "fields": [
    { "name":"col1", "type":"string" },
    { "name":"col2", "type":"long" },
    { "name":"col3", "type":"string" }
  ] }')
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat';
INSERT INTO TABLE avro_extschema_literal1 VALUES('s1', 1, 's2');

DESCRIBE EXTENDED avro_extschema_literal1;
SELECT * FROM avro_extschema_literal1;

CREATE TABLE avro_extschema_url1
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
WITH SERDEPROPERTIES ('avro.schema.url'='${system:test.tmp.dir}/avro_tableproperty_optimize.avsc')
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat';
INSERT INTO TABLE avro_extschema_url1 VALUES('s1', 1, 's2');

DESCRIBE EXTENDED avro_extschema_url1;
SELECT * FROM avro_extschema_url1;

dfs -rm ${system:test.tmp.dir}/avro_tableproperty_optimize.avsc;
