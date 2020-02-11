set hive.mapred.mode=nonstrict;
-- SORT_QUERY_RESULTS
-- Verify that table scans work with partitioned Avro tables
CREATE TABLE episodes_n2
ROW FORMAT
SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
TBLPROPERTIES ('avro.schema.literal'='{
  "namespace": "testing.hive.avro.serde",
  "name": "episodes_n2",
  "type": "record",
  "fields": [
    {
      "name":"title",
      "type":"string",
      "doc":"episode title"
    },
    {
      "name":"air_date",
      "type":"string",
      "doc":"initial date"
    },
    {
      "name":"doctor",
      "type":"int",
      "doc":"main actor playing the Doctor in episode"
    }
  ]
}');

LOAD DATA LOCAL INPATH '../../data/files/episodes.avro' INTO TABLE episodes_n2;

CREATE TABLE episodes_partitioned_n1
PARTITIONED BY (doctor_pt INT)
ROW FORMAT
SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
TBLPROPERTIES ('avro.schema.literal'='{
  "namespace": "testing.hive.avro.serde",
  "name": "episodes_n2",
  "type": "record",
  "fields": [
    {
      "name":"title",
      "type":"string",
      "doc":"episode title"
    },
    {
      "name":"air_date",
      "type":"string",
      "doc":"initial date"
    },
    {
      "name":"doctor",
      "type":"int",
      "doc":"main actor playing the Doctor in episode"
    }
  ]
}');

INSERT OVERWRITE TABLE episodes_partitioned_n1 PARTITION (doctor_pt) SELECT title, air_date, doctor, doctor as doctor_pt FROM episodes_n2;

SELECT * FROM episodes_partitioned_n1 WHERE doctor_pt > 6;

-- Verify that Fetch works in addition to Map
SELECT * FROM episodes_partitioned_n1 ORDER BY air_date LIMIT 5;
-- Fetch w/filter to specific partition
SELECT * FROM episodes_partitioned_n1 WHERE doctor_pt = 6;
-- Fetch w/non-existent partition
SELECT * FROM episodes_partitioned_n1 WHERE doctor_pt = 7 LIMIT 5;
-- Alter table add an empty partition
ALTER TABLE episodes_partitioned_n1 ADD PARTITION (doctor_pt=7);
SELECT COUNT(*) FROM episodes_partitioned_n1;

-- Verify that reading from an Avro partition works
-- even if it has an old schema relative to the current table level schema

-- Create table and store schema in SERDEPROPERTIES
CREATE TABLE episodes_partitioned_serdeproperties
PARTITIONED BY (doctor_pt INT)
ROW FORMAT
SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
WITH SERDEPROPERTIES ('avro.schema.literal'='{
  "namespace": "testing.hive.avro.serde",
  "name": "episodes_n2",
  "type": "record",
  "fields": [
    {
      "name":"title",
      "type":"string",
      "doc":"episode title"
    },
    {
      "name":"air_date",
      "type":"string",
      "doc":"initial date"
    },
    {
      "name":"doctor",
      "type":"int",
      "doc":"main actor playing the Doctor in episode"
    }
  ]
}')
STORED AS
INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat';

-- Insert data into a partition
INSERT INTO TABLE episodes_partitioned_serdeproperties PARTITION (doctor_pt) SELECT title, air_date, doctor, doctor as doctor_pt FROM episodes_n2;
set hive.metastore.disallow.incompatible.col.type.changes=false;
-- Evolve the table schema by adding new array field "cast_and_crew"
ALTER TABLE episodes_partitioned_serdeproperties
SET SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
WITH SERDEPROPERTIES ('avro.schema.literal'='{
  "namespace": "testing.hive.avro.serde",
  "name": "episodes_n2",
  "type": "record",
  "fields": [
    {
      "name":"cast_and_crew",
      "type":{"type":"array","items":"string"},
      "default":[]
    },
    {
      "name":"title",
      "type":"string",
      "doc":"episode title"
    },
    {
      "name":"air_date",
      "type":"string",
      "doc":"initial date"
    },
    {
      "name":"doctor",
      "type":"int",
      "doc":"main actor playing the Doctor in episode"
    }
  ]
}');

-- Try selecting from the evolved table
SELECT * FROM episodes_partitioned_serdeproperties;
reset hive.metastore.disallow.incompatible.col.type.changes;
