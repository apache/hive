CREATE EXTERNAL TABLE geometry_orc (id INT, geom GEOMETRY)
STORED BY ICEBERG STORED AS ORC tblproperties('format-version'='3');
