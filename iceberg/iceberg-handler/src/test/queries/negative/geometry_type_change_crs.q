CREATE EXTERNAL TABLE geometry_change_crs (id INT, geom GEOMETRY) STORED BY ICEBERG tblproperties('format-version'='3');

ALTER TABLE geometry_change_crs CHANGE COLUMN geom geom GEOMETRY('EPSG:3857');
