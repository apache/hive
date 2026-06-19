set hive.mapred.mode=nonstrict;

DROP TABLE avro_date_staging;
DROP TABLE avro_date;
DROP TABLE avro_date_casts;

CREATE TABLE avro_date_staging (d date, m1 map<string, date>, l1 array<date>)
   ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
   COLLECTION ITEMS TERMINATED BY ',' MAP KEYS TERMINATED BY ':'
   STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '../../data/files/avro_date.txt' OVERWRITE INTO TABLE avro_date_staging;

CREATE TABLE avro_date (d date, m1 map<string, date>, l1 array<date>) 
  PARTITIONED BY (p1 int, p2 date) 
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
  COLLECTION ITEMS TERMINATED BY ',' MAP KEYS TERMINATED BY ':' 
  STORED AS AVRO;

INSERT OVERWRITE TABLE avro_date PARTITION(p1=2, p2='2014-09-26') SELECT * FROM avro_date_staging;

SELECT * FROM avro_date;
SELECT d, COUNT(d) FROM avro_date GROUP BY d;
SELECT * FROM avro_date WHERE d!='1947-02-11';
SELECT * FROM avro_date WHERE d<'2014-12-21';
SELECT * FROM avro_date WHERE d>'8000-12-01';
