CREATE TABLE timestampltz_formats (
  formatid string,
  tsval timestamp with local time zone
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe';

LOAD DATA LOCAL INPATH '../../data/files/timestamps_mixed_formats.txt' overwrite into table timestampltz_formats;

SELECT formatid, tsval FROM timestampltz_formats;

ALTER TABLE timestampltz_formats SET SERDEPROPERTIES ("timestamp.formats"="yyyy-MM-dd'T'HH:mm:ssVV");

SELECT formatid, tsval FROM timestampltz_formats;

ALTER TABLE timestampltz_formats SET SERDEPROPERTIES ("timestamp.formats"="MMM d yyyy HH:mm:ss");

SELECT formatid, tsval FROM timestampltz_formats;

ALTER TABLE timestampltz_formats SET SERDEPROPERTIES ("timestamp.formats"="yyyy-MM-dd'T'HH:mm:ssVV,MMM d yyyy HH:mm:ss");

SELECT formatid, tsval FROM timestampltz_formats;