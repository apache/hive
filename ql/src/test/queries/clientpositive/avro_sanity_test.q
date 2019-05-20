-- SORT_QUERY_RESULTS

-- verify that we can actually read avro files
CREATE TABLE doctors_n1 
ROW FORMAT
SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
TBLPROPERTIES ('avro.schema.literal'='{
  "namespace": "testing.hive.avro.serde",
  "name": "doctors_n1",
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
    }
  ]
}');

DESCRIBE doctors_n1;

LOAD DATA LOCAL INPATH '../../data/files/doctors.avro' INTO TABLE doctors_n1;

SELECT * FROM doctors_n1;

