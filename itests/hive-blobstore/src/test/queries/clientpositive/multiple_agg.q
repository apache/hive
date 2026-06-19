-- Test multiple aggregate udfs work in the same query

DROP TABLE birthdays;
CREATE EXTERNAL TABLE birthdays (id INT, name STRING, birthday TIMESTAMP)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '${hiveconf:test.blobstore.path.unique}/multiple_agg/birthdays';
LOAD DATA LOCAL INPATH '../../data/files/birthdays' INTO TABLE birthdays;

SELECT MIN(Birthday) AS MinBirthday, MAX(Birthday) AS MaxBirthday FROM birthdays;