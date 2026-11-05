CREATE EXTERNAL TABLE geometry_partition_column (id INT)
PARTITIONED BY (geom GEOMETRY)
STORED BY ICEBERG tblproperties('format-version'='3');
