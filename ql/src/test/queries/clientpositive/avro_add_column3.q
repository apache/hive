--! qt:dataset:part
set hive.mapred.mode=nonstrict;
-- SORT_QUERY_RESULTS

-- verify that we can actually read avro files
CREATE TABLE doctors_n3 (
  number int,
  first_name string,
  last_name string)
STORED AS AVRO;

LOAD DATA LOCAL INPATH '../../data/files/doctors.avro' INTO TABLE doctors_n3;

CREATE TABLE doctors_copy_n0 (
  number int,
  first_name string)
PARTITIONED BY (part int)
STORED AS AVRO;

INSERT INTO TABLE doctors_copy_n0 PARTITION(part=1) SELECT number, first_name FROM doctors_n3;

ALTER TABLE doctors_copy_n0 ADD COLUMNS (last_name string);

DESCRIBE doctors_copy_n0;

SELECT * FROM doctors_copy_n0;