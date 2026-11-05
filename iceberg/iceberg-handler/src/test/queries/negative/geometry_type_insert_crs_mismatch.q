CREATE EXTERNAL TABLE geometry_insert_crs_mismatch (id INT, geom GEOMETRY('EPSG:3857')) STORED BY ICEBERG tblproperties('format-version'='3');

INSERT INTO geometry_insert_crs_mismatch SELECT 1, CAST(unhex('0101000000000000000000F03F0000000000000040') AS GEOMETRY('EPSG:4326'));
