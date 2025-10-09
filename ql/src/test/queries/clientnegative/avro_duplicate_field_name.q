-- verify AVRO-3827: Disallow duplicate field names

CREATE TABLE avroExternalDupField
STORED AS AVRO
TBLPROPERTIES ('avro.schema.literal'='{
  "namespace": "org.apache.hive",
  "name": "my_schema",
  "type": "record",
  "fields": [
    {
      "name": "f1",
      "type": {
        "name": "a",
        "type": "record",
        "fields": []
      }
    },  {
      "name": "f1",
      "type": {
        "name": "b",
        "type": "record",
        "fields": []
      }
    }
  ] }');
