CREATE TABLE timestampltz_formats (
  formatid string,
  tsval timestamp with local time zone
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe';

LOAD DATA LOCAL INPATH '../../data/files/timestamps_mixed_formats.txt' overwrite into table timestampltz_formats;

SELECT * FROM timestampltz_formats;

CREATE TABLE timestampltz_orc_format (
  formatid string,
  tsval timestamp with local time zone
)
stored as orc;

insert into timestampltz_orc_format select * from timestampltz_formats;

SELECT * FROM timestampltz_orc_format;