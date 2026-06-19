-- SORT_QUERY_RESULTS
CREATE TABLE drop_partition_days (
sensor_id STRING,
location_id STRING,
reading_time TIMESTAMP,
temperature DOUBLE,
humidity DOUBLE
)
PARTITIONED BY SPEC (location_id, days(reading_time))
STORED BY ICEBERG
TBLPROPERTIES (
'write.format.default'='parquet',
'format-version'='2',
'write.parquet.compression-codec'='gzip'
);

INSERT INTO drop_partition_days VALUES
('sensor_001', 'loc_001', '2024-06-01 10:00:00', 22.5, 60.0),
('sensor_002', 'loc_002', '2024-06-01 10:15:00', 23.0, 58.0),
('sensor_001', 'loc_001', '2024-06-02 11:00:00', 22.8, 61.0);

ALTER TABLE drop_partition_days DROP PARTITION (location_id = 'loc_002',  days(reading_time) = '2024-06-01');

SELECT * FROM drop_partition_days;

CREATE TABLE drop_partition_years (
sensor_id STRING,
location_id STRING,
reading_time TIMESTAMP,
temperature DOUBLE,
humidity DOUBLE
)
PARTITIONED BY SPEC (location_id, years(reading_time))
STORED BY ICEBERG
TBLPROPERTIES (
'write.format.default'='parquet',
'format-version'='2',
'write.parquet.compression-codec'='gzip'
);

INSERT INTO drop_partition_years VALUES
('sensor_001', 'loc_001', '2024-06-01 10:00:00', 22.5, 60.0),
('sensor_002', 'loc_002', '2024-06-01 10:15:00', 23.0, 58.0),
('sensor_001', 'loc_001', '2024-06-02 11:00:00', 22.8, 61.0);

ALTER TABLE drop_partition_years DROP PARTITION (location_id = 'loc_002',  years(reading_time) = '2024');

SELECT * FROM drop_partition_years;

INSERT INTO drop_partition_years VALUES
('sensor_001', 'loc_003', '2021-06-01 10:00:00', 20.5, 40.0),
('sensor_002', 'loc_003', '2022-06-01 10:15:00', 33.0, 58.0),
('sensor_001', 'loc_003', '2024-06-02 11:00:00', 12.8, 43.0);

ALTER TABLE drop_partition_years DROP PARTITION (location_id = 'loc_003',  years(reading_time) <= '2022');

SELECT * FROM drop_partition_years;

CREATE TABLE drop_partition_months (
sensor_id STRING,
location_id STRING,
reading_time TIMESTAMP,
temperature DOUBLE,
humidity DOUBLE
)
PARTITIONED BY SPEC (location_id, months(reading_time))
STORED BY ICEBERG
TBLPROPERTIES (
'write.format.default'='parquet',
'format-version'='2',
'write.parquet.compression-codec'='gzip'
);

INSERT INTO drop_partition_months VALUES
('sensor_001', 'loc_001', '2024-06-01 10:00:00', 22.5, 60.0),
('sensor_002', 'loc_002', '2024-06-01 10:15:00', 23.0, 58.0),
('sensor_001', 'loc_001', '2024-08-02 11:00:00', 22.8, 61.0);

ALTER TABLE drop_partition_months DROP PARTITION (location_id = 'loc_002',  months(reading_time) >= '2024-06');

SELECT * FROM drop_partition_months;

CREATE TABLE drop_partition_hours (
sensor_id STRING,
location_id STRING,
reading_time TIMESTAMP,
temperature DOUBLE,
humidity DOUBLE
)
PARTITIONED BY SPEC (location_id, hours(reading_time))
STORED BY ICEBERG
TBLPROPERTIES (
'write.format.default'='parquet',
'format-version'='2',
'write.parquet.compression-codec'='gzip'
);

INSERT INTO drop_partition_hours VALUES
('sensor_001', 'loc_001', '2024-06-01 10:00:00', 22.5, 60.0),
('sensor_002', 'loc_002', '2024-06-01 10:15:00', 23.0, 58.0),
('sensor_001', 'loc_001', '2024-06-02 11:00:00', 22.8, 61.0);

ALTER TABLE drop_partition_hours DROP PARTITION (location_id = 'loc_002',  hours(reading_time) = '2024-06-01-10');

SELECT * FROM drop_partition_hours;

create external table drop_partition_truncate(a int, b string, c int) partitioned by spec (truncate(3, b)) stored by iceberg;
insert into drop_partition_truncate values (1, 'one', 50), (2, 'two', 51), (3, 'three', 52), (4, 'four', 53), (5, 'five', 54), (111, 'one', 55), (333, 'two', 56);

ALTER TABLE drop_partition_truncate DROP PARTITION (truncate(3, b) = 'one');

SELECT * FROM drop_partition_truncate;

create external table drop_partition_bucket(a int, b string, c int) partitioned by spec (bucket(3,b)) stored by iceberg;
insert into drop_partition_bucket values (1, 'one', 50), (2, 'two', 51), (3, 'three', 52), (4, 'four', 53), (5, 'five', 54), (111, 'one', 55), (333, 'two', 56);

ALTER TABLE drop_partition_bucket DROP PARTITION (bucket(3, b) = '0');

SELECT * FROM drop_partition_bucket;