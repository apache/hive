CREATE EXTERNAL TABLE geometry_alter_orc (id INT, geom GEOMETRY) STORED BY ICEBERG tblproperties('format-version'='3');

ALTER TABLE geometry_alter_orc SET TBLPROPERTIES ('write.format.default'='orc');
