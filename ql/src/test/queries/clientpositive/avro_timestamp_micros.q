CREATE EXTERNAL TABLE hive_test(`dt` timestamp) STORED AS AVRO;
INSERT INTO hive_test VALUES (cast('2024-08-09 14:08:26.326107' as timestamp));
SELECT * FROM hive_test;