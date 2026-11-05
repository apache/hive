CREATE EXTERNAL TABLE geometry_partition_identity (id INT, geom GEOMETRY)
PARTITIONED BY SPEC (geom)
STORED BY ICEBERG tblproperties('format-version'='3');
