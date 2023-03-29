
CREATE TABLE timestamptz_formats (
  c1 timestamp with local time zone
);

LOAD DATA LOCAL INPATH '../../data/files/tzl_formats.txt' overwrite into table timestamptz_formats;

SELECT * FROM timestamptz_formats;
