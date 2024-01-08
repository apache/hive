-- verify AVRO-3820:Don't allow invalid field names, field name should match [A-Za-z_][A-Za-z0-9_]*

CREATE TABLE avroExternalInvalidField
STORED AS AVRO
TBLPROPERTIES ('avro.schema.literal'='{
  "namespace": "org.apache.hive",
  "name": "my_record",
  "type": "record",
  "fields": [
    {
      "name": "f1.x",
      "type": {
        "name": "my_enum",
        "type": "enum",
        "symbols": ["a"]
      }
    }
  ] }');
