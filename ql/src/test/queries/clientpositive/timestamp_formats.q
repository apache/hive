
CREATE TABLE timestamp_formats (
  c1 string,
  c1_ts timestamp,
  c2 string,
  c2_ts timestamp,
  c3 string,
  c3_ts timestamp
);

LOAD DATA LOCAL INPATH '../../data/files/ts_formats.txt' overwrite into table timestamp_formats;

SELECT * FROM timestamp_formats;

-- Add single timestamp format. This should allow c3_ts to parse
ALTER TABLE timestamp_formats SET SERDEPROPERTIES ("timestamp.formats"="yyyy-MM-dd'T'HH:mm:ss");
SELECT * FROM timestamp_formats;

-- Add another format, to allow c2_ts to parse
ALTER TABLE timestamp_formats SET SERDEPROPERTIES ("timestamp.formats"="yyyy-MM-dd'T'HH:mm:ss,yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS");
SELECT * FROM timestamp_formats;

DROP TABLE timestamp_formats;
