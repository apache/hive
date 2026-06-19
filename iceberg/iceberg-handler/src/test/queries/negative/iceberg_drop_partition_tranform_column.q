CREATE TABLE drop_partition (
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

INSERT INTO drop_partition VALUES
('sensor_001', 'loc_001', '2024-06-01 10:00:00', 22.5, 60.0),
('sensor_002', 'loc_002', '2024-06-01 10:15:00', 23.0, 58.0),
('sensor_001', 'loc_001', '2024-06-02 11:00:00', 22.8, 61.0);


ALTER TABLE drop_partition DROP PARTITION (location_id = 'loc_002', reading_time = '2024-06-01 10:15:00');

SELECT * FROM drop_partition;