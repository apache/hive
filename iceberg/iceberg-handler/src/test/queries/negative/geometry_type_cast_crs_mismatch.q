CREATE EXTERNAL TABLE geometry_cast_crs_mismatch (id INT, geom GEOMETRY('EPSG:4326')) STORED BY ICEBERG tblproperties('format-version'='3');

SELECT CAST(geom AS GEOMETRY('EPSG:3857')) FROM geometry_cast_crs_mismatch;
