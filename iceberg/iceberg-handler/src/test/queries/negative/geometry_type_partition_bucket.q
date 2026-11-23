CREATE EXTERNAL TABLE geometry_partition_bucket (id INT, geom GEOMETRY)
PARTITIONED BY SPEC (bucket(4, geom))
STORED BY ICEBERG tblproperties('format-version'='3');
