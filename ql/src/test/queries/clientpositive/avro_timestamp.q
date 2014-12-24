-- JAVA_VERSION_SPECIFIC_OUTPUT

DROP TABLE avro_timestamp_staging;
DROP TABLE avro_timestamp;
DROP TABLE avro_timestamp_casts;

CREATE TABLE avro_timestamp_staging (d timestamp, m1 map<string, timestamp>, l1 array<timestamp>)
   ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
   COLLECTION ITEMS TERMINATED BY ',' MAP KEYS TERMINATED BY ':'
   STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/avro_timestamp.txt' OVERWRITE INTO TABLE avro_timestamp_staging;

CREATE TABLE avro_timestamp (d timestamp, m1 map<string, timestamp>, l1 array<timestamp>)
  PARTITIONED BY (p1 int, p2 timestamp)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  COLLECTION ITEMS TERMINATED BY ',' MAP KEYS TERMINATED BY ':'
  STORED AS AVRO;

INSERT OVERWRITE TABLE avro_timestamp PARTITION(p1=2, p2='2014-09-26 07:08:09.123') SELECT * FROM avro_timestamp_staging;

SELECT * FROM avro_timestamp;
SELECT d, COUNT(d) FROM avro_timestamp GROUP BY d;
SELECT * FROM avro_timestamp WHERE d!='1947-02-11 07:08:09.123';
SELECT * FROM avro_timestamp WHERE d<'2014-12-21 07:08:09.123';
SELECT * FROM avro_timestamp WHERE d>'8000-12-01 07:08:09.123';
