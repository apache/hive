CREATE EXTERNAL TABLE geometry_add_column_orc (id INT)
STORED BY ICEBERG STORED AS ORC tblproperties('format-version'='3');

ALTER TABLE geometry_add_column_orc ADD COLUMNS (geom GEOMETRY);
