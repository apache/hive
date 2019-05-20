-- verify that we can modify avro tables created by externalschemas

CREATE TABLE avro_extschema
STORED AS AVRO
TBLPROPERTIES ('avro.schema.literal'='{
  "namespace": "org.apache.hive",
  "name": "ext_schema",
  "type": "record",
  "fields": [
    { "name":"number", "type":"int" },
    { "name":"first_name", "type":"string" },
    { "name":"last_name", "type":"string" }
  ] }');

DESCRIBE avro_extschema;

ALTER TABLE avro_extschema
CHANGE COLUMN number number bigint;