CREATE EXTERNAL TABLE micros_table(`dt` timestamp)
STORED AS AVRO;

INSERT INTO micros_table VALUES
(cast('2024-08-09 14:08:26.326107' as timestamp)),
('2012-02-21 07:08:09.123'),
('1014-02-11 07:15:11.12345');

SELECT * FROM micros_table;