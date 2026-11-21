-- Mask random uuid
--! qt:replace:/(\s+'uuid'=')\S+('\s*)/$1#Masked#$2/
-- Mask random snapshot id
--! qt:replace:/('current-snapshot-id'=')\d+/$1#SnapshotId#/
-- Mask current-snapshot-timestamp-ms
--! qt:replace:/('current-snapshot-timestamp-ms'=')\d+/$1#Masked#/
-- SORT_QUERY_RESULTS

CREATE EXTERNAL TABLE geometry_dml (id INT, geom GEOMETRY, geom_3857 GEOMETRY('EPSG:3857'))
STORED BY ICEBERG tblproperties('format-version'='3');

INSERT INTO geometry_dml VALUES
(1, unhex('0101000000000000000000F03F0000000000000040'), unhex('010100000000000000000008400000000000001040')),
(2, unhex('010100000000000000000008400000000000001040'), NULL),
(3, NULL, NULL);

ALTER TABLE geometry_dml CREATE TAG before_dml;

-- Row level operations rewrite geometry values
UPDATE geometry_dml SET geom = unhex('010100000000000000000008400000000000001040') WHERE id = 1;

UPDATE geometry_dml SET geom_3857 = CAST(unhex('0101000000000000000000F03F0000000000000040') AS GEOMETRY('EPSG:3857'))
WHERE geom_3857 IS NULL;

SELECT id, hex(CAST(geom AS BINARY)), hex(CAST(geom_3857 AS BINARY)) FROM geometry_dml;

DELETE FROM geometry_dml WHERE geom IS NULL;

DELETE FROM geometry_dml WHERE geom_3857 = CAST(unhex('010100000000000000000008400000000000001040') AS GEOMETRY('EPSG:3857'));

SELECT id, hex(CAST(geom AS BINARY)), hex(CAST(geom_3857 AS BINARY)) FROM geometry_dml;

MERGE INTO geometry_dml t
USING (SELECT 2 AS id, CAST(unhex('0101000000000000000000F03F0000000000000040') AS GEOMETRY) AS geom
       UNION ALL
       SELECT 4 AS id, CAST(unhex('010100000000000000000008400000000000001040') AS GEOMETRY) AS geom) s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET geom = s.geom
WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.geom, NULL);

SELECT id, hex(CAST(geom AS BINARY)), hex(CAST(geom_3857 AS BINARY)) FROM geometry_dml;

-- The tag still returns the geometry values before the changes
SELECT id, hex(CAST(geom AS BINARY)), hex(CAST(geom_3857 AS BINARY)) FROM default.geometry_dml.tag_before_dml;

-- Insert overwrite from a query over geometry values
INSERT OVERWRITE TABLE geometry_dml
SELECT id, geom, geom_3857 FROM default.geometry_dml.tag_before_dml WHERE geom IS NOT NULL;

SELECT id, hex(CAST(geom AS BINARY)), hex(CAST(geom_3857 AS BINARY)) FROM geometry_dml;

-- Insert from a table with the same geometry types
CREATE EXTERNAL TABLE geometry_dml_target (id INT, geom GEOMETRY, geom_3857 GEOMETRY('EPSG:3857'))
STORED BY ICEBERG tblproperties('format-version'='3');

INSERT INTO geometry_dml_target SELECT * FROM geometry_dml;

INSERT INTO geometry_dml_target (id, geom_3857) SELECT id + 10, geom_3857 FROM geometry_dml;

SELECT id, hex(CAST(geom AS BINARY)), hex(CAST(geom_3857 AS BINARY)) FROM geometry_dml_target;

-- Column statistics are not collected for geometry columns
ANALYZE TABLE geometry_dml_target COMPUTE STATISTICS FOR COLUMNS;

DESCRIBE FORMATTED geometry_dml_target id;

DESCRIBE FORMATTED geometry_dml_target geom;

-- Metadata tables report the geometry bounds as WKB points
SELECT content, record_count, hex(CAST(readable_metrics.geom_3857.lower_bound AS BINARY)),
    hex(CAST(readable_metrics.geom_3857.upper_bound AS BINARY)), readable_metrics.geom_3857.null_value_count
FROM default.geometry_dml_target.files;

SELECT ST_AsText(ST_GeomFromWKB(CAST(readable_metrics.geom.lower_bound AS BINARY))),
    ST_AsText(ST_GeomFromWKB(CAST(readable_metrics.geom.upper_bound AS BINARY)))
FROM default.geometry_dml_target.files WHERE readable_metrics.geom.lower_bound IS NOT NULL;

-- Materialized views over geometry columns
CREATE MATERIALIZED VIEW geometry_dml_mv STORED BY ICEBERG tblproperties('format-version'='3') AS
SELECT id, geom_3857 FROM geometry_dml_target WHERE geom_3857 IS NOT NULL;

DESCRIBE geometry_dml_mv;

SELECT id, hex(CAST(geom_3857 AS BINARY)) FROM geometry_dml_mv;

DROP MATERIALIZED VIEW geometry_dml_mv;
