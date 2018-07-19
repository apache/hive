-- SORT_QUERY_RESULTS

-- verify that we can actually read avro files
CREATE TABLE doctors_n4 (
  number int,
  first_name string,
  last_name string)
STORED AS AVRO;

DESCRIBE doctors_n4;

LOAD DATA LOCAL INPATH '../../data/files/doctors.avro' INTO TABLE doctors_n4;

SELECT * FROM doctors_n4;