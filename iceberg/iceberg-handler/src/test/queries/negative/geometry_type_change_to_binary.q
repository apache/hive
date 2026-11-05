CREATE EXTERNAL TABLE geometry_change_to_binary (id INT, geom GEOMETRY) STORED BY ICEBERG tblproperties('format-version'='3');

ALTER TABLE geometry_change_to_binary CHANGE COLUMN geom geom BINARY;
