CREATE EXTERNAL TABLE geometry_insert_string (id INT, geom GEOMETRY) STORED BY ICEBERG tblproperties('format-version'='3');

INSERT INTO geometry_insert_string SELECT 1, 'POINT (1 2)';
