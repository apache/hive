set hive.cli.print.header=true;
set hive.mapred.mode=nonstrict;
-- SORT_QUERY_RESULTS
-- Verify that table scans work with partitioned Avro tables
CREATE TABLE episodes_n0 (
  title string COMMENT "episode title",
  air_date string COMMENT "initial date",
  doctor int COMMENT "main actor playing the Doctor in episode")
STORED AS AVRO;

LOAD DATA LOCAL INPATH '../../data/files/episodes.avro' INTO TABLE episodes_n0;

CREATE TABLE episodes_partitioned_n0 (
  title string COMMENT "episode title",
  air_date string COMMENT "initial date",
  doctor int COMMENT "main actor playing the Doctor in episode")
PARTITIONED BY (doctor_pt INT)
STORED AS AVRO;

INSERT OVERWRITE TABLE episodes_partitioned_n0 PARTITION (doctor_pt)
SELECT title, air_date, doctor, doctor as doctor_pt FROM episodes_n0;
DESCRIBE FORMATTED episodes_partitioned_n0;

ALTER TABLE episodes_partitioned_n0
SET SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
WITH
SERDEPROPERTIES ('avro.schema.literal'='{
  "namespace": "testing.hive.avro.serde",
  "name": "episodes_n0",
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
    },
     {
       "name":"value",
       "type":"int",
       "default":0,
       "doc":"default value"
     }
  ]
}');
DESCRIBE FORMATTED episodes_partitioned_n0;

set hive.fetch.task.conversion=more;

EXPLAIN
SELECT * FROM episodes_partitioned_n0 WHERE doctor_pt > 6;

SELECT * FROM episodes_partitioned_n0 WHERE doctor_pt > 6;

-- Verify that Fetch works in addition to Map
SELECT * FROM episodes_partitioned_n0 ORDER BY air_date LIMIT 5;
-- Fetch w/filter to specific partition
SELECT * FROM episodes_partitioned_n0 WHERE doctor_pt = 6;
-- Fetch w/non-existent partition
SELECT * FROM episodes_partitioned_n0 WHERE doctor_pt = 7 LIMIT 5;

set hive.fetch.task.conversion=none;

EXPLAIN
SELECT * FROM episodes_partitioned_n0 WHERE doctor_pt > 6;

SELECT * FROM episodes_partitioned_n0 WHERE doctor_pt > 6;

SELECT * FROM episodes_partitioned_n0 ORDER BY air_date LIMIT 5;
SELECT * FROM episodes_partitioned_n0 WHERE doctor_pt = 6;
SELECT * FROM episodes_partitioned_n0 WHERE doctor_pt = 7 LIMIT 5;