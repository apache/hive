CREATE EXTERNAL TABLE geometry_union_crs_mismatch (geom GEOMETRY, geom_3857 GEOMETRY('EPSG:3857')) STORED BY ICEBERG tblproperties('format-version'='3');

SELECT geom FROM geometry_union_crs_mismatch
UNION ALL
SELECT geom_3857 FROM geometry_union_crs_mismatch;
