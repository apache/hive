
CREATE TABLE timestamptz_formats (
  c1 string,
  c2 timestamp with local time zone,
  c3 timestamp
);

LOAD DATA LOCAL INPATH '../../data/files/tzl_formats.txt' overwrite into table timestamptz_formats;

SELECT c1, '|', c2, '|', c3
FROM timestamptz_formats;
