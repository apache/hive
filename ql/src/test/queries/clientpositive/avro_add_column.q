-- SORT_QUERY_RESULTS

-- verify that we can actually read avro files
CREATE TABLE doctors_n0 (
  number int,
  first_name string)
STORED AS AVRO;

DESCRIBE doctors_n0;

ALTER TABLE doctors_n0 ADD COLUMNS (last_name string);

DESCRIBE doctors_n0;

LOAD DATA LOCAL INPATH '../../data/files/doctors.avro' INTO TABLE doctors_n0;

SELECT * FROM doctors_n0;