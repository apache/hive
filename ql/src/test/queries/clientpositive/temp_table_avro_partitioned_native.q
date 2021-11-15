set hive.mapred.mode=nonstrict;
set metastore.integral.jdo.pushdown=true;
-- SORT_QUERY_RESULTS
-- Verify that table scans work with partitioned Avro tables
CREATE TABLE episodes (
  title string COMMENT "episode title",
  air_date string COMMENT "initial date",
  doctor int COMMENT "main actor playing the Doctor in episode")
STORED AS AVRO;

LOAD DATA LOCAL INPATH '../../data/files/episodes.avro' INTO TABLE episodes;

CREATE TABLE episodes_partitioned (
  title string COMMENT "episode title",
  air_date string COMMENT "initial date",
  doctor int COMMENT "main actor playing the Doctor in episode")
PARTITIONED BY (doctor_pt INT)
STORED AS AVRO;

SET hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE episodes_partitioned PARTITION (doctor_pt)
  SELECT title, air_date, doctor, doctor as doctor_pt FROM episodes;


-- Verify that Fetch works in addition to Map
SELECT * FROM episodes_partitioned ORDER BY air_date LIMIT 5;
-- Fetch w/filter to specific partition
SELECT * FROM episodes_partitioned WHERE doctor_pt = 6;
-- Fetch w/non-existent partition
SELECT * FROM episodes_partitioned WHERE doctor_pt = 7 LIMIT 5;