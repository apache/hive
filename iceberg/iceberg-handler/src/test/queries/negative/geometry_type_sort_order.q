CREATE EXTERNAL TABLE geometry_sort_order (id INT, geom GEOMETRY)
WRITE LOCALLY ORDERED BY geom
STORED BY ICEBERG tblproperties('format-version'='3');
