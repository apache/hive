-- SORT_QUERY_RESULTS

-- verify that new joins bring in correct schemas (including evolved schemas)

CREATE TABLE doctors4 (
  number int COMMENT "Order of playing the role",
  first_name string COMMENT "first name of actor playing role",
  last_name string COMMENT "last name of actor playing role")
STORED AS AVRO;

DESCRIBE doctors4;

LOAD DATA LOCAL INPATH '../../data/files/doctors.avro' INTO TABLE doctors4;

CREATE TABLE episodes (
  title string COMMENT "episode title",
  air_date string COMMENT "initial date",
  doctor int COMMENT "main actor playing the Doctor in episode")
STORED AS AVRO;

DESCRIBE episodes;

LOAD DATA LOCAL INPATH '../../data/files/episodes.avro' INTO TABLE episodes;

SELECT e.title, e.air_date, d.first_name, d.last_name, e.air_date
FROM doctors4 d JOIN episodes e ON (d.number=e.doctor);