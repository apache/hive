CREATE EXTERNAL TABLE geometry_invalid_wkb (id INT, geom GEOMETRY) STORED BY ICEBERG tblproperties('format-version'='3');

INSERT INTO geometry_invalid_wkb SELECT 1, unhex('0101000000000000000000F03F');
