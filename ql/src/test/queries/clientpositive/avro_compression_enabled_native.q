--! qt:dataset:src
-- verify that new joins bring in correct schemas (including evolved schemas)

CREATE TABLE doctors4_n1 (
  number int,
  first_name string,
  last_name string,
  extra_field string)
STORED AS AVRO;

LOAD DATA LOCAL INPATH '../../data/files/doctors.avro' INTO TABLE doctors4_n1;

set hive.exec.compress.output=true;

SELECT count(*) FROM src;