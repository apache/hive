-- Mask random uuid
--! qt:replace:/(\s+'uuid'=')\S+('\s*)/$1#Masked#$2/
-- Mask random snapshot id
--! qt:replace:/('current-snapshot-id'=')\d+/$1#SnapshotId#/
-- Mask current-snapshot-timestamp-ms
--! qt:replace:/('current-snapshot-timestamp-ms'=')\d+/$1#Masked#/
-- SORT_QUERY_RESULTS

-- Geometry columns with the default CRS (OGC:CRS84), an explicit CRS and a column comment
CREATE EXTERNAL TABLE geometry_test_basic (
    id INT,
    geom GEOMETRY,
    geom_3857 GEOMETRY('EPSG:3857') COMMENT 'web mercator'
) STORED BY ICEBERG tblproperties('format-version'='3');

DESCRIBE geometry_test_basic;

SHOW CREATE TABLE geometry_test_basic;

-- The default CRS written explicitly is normalized, CRS names are case-insensitive, and GEOMETRY is a
-- non-reserved keyword that can be used as a column name
CREATE EXTERNAL TABLE geometry_test_crs (
    geometry GEOMETRY,
    geom_crs84 GEOMETRY('OGC:CRS84'),
    geom_lower GEOMETRY('epsg:32633')
) STORED BY ICEBERG tblproperties('format-version'='3');

DESCRIBE geometry_test_crs;

-- Geometry values are WKB; binary values are implicitly converted
INSERT INTO geometry_test_basic VALUES
(1, CAST(unhex('0101000000000000000000F03F0000000000000040') AS GEOMETRY),
    unhex('010100000000000000000008400000000000001040')),
(2, unhex('010100000000000000000008400000000000001040'), NULL),
(3, NULL, NULL),
(4, unhex('0101000000000000000000F03F0000000000000040'),
    CAST(unhex('0101000000000000000000F03F0000000000000040') AS GEOMETRY('EPSG:3857')));

SELECT id, hex(CAST(geom AS BINARY)), hex(CAST(geom_3857 AS BINARY)) FROM geometry_test_basic;

-- WKB variety: big endian, Z coordinate, empty point, line string, polygon with a hole, multi polygon and
-- geometry collection. The ESRI functions do not support geometry collections
CREATE EXTERNAL TABLE geometry_test_shapes (id INT, shape STRING, geom GEOMETRY)
STORED BY ICEBERG tblproperties('format-version'='3');

INSERT INTO geometry_test_shapes VALUES
(1, 'big endian point', unhex('00000000013FF00000000000004000000000000000')),
(2, 'point z', unhex('01E9030000000000000000F03F00000000000000400000000000000840')),
(3, 'empty point', unhex('0101000000000000000000F87F000000000000F87F')),
(4, 'line string', unhex('01020000000200000000000000000000000000000000000000000000000000F03F000000000000F03F'));

INSERT INTO geometry_test_shapes
SELECT 5, 'polygon with hole',
    ST_AsBinary(ST_GeomFromText('POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 3 2, 3 3, 2 3, 2 2))'))
UNION ALL
SELECT 6, 'multi polygon',
    ST_AsBinary(ST_GeomFromText('MULTIPOLYGON (((0 0, 1 0, 1 1, 0 0)), ((5 5, 6 5, 6 6, 5 5)))'));

-- GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (0 0, 1 1))
INSERT INTO geometry_test_shapes VALUES
(7, 'geometry collection', unhex('0107000000020000000101000000000000000000F03F000000000000004001020000000200000000000000000000000000000000000000000000000000F03F000000000000F03F'));

SELECT id, shape, hex(CAST(geom AS BINARY)), length(CAST(geom AS BINARY)) FROM geometry_test_shapes;

SELECT id, shape, ST_AsText(ST_GeomFromWKB(CAST(geom AS BINARY))) FROM geometry_test_shapes WHERE id <> 7;

-- Filters
SELECT id FROM geometry_test_basic WHERE geom IS NOT NULL AND geom_3857 IS NULL;

SELECT id FROM geometry_test_basic WHERE geom IS NULL;

SELECT id FROM geometry_test_basic WHERE geom = CAST(unhex('0101000000000000000000F03F0000000000000040') AS GEOMETRY);

SELECT id FROM geometry_test_basic WHERE geom = unhex('0101000000000000000000F03F0000000000000040');

SELECT id FROM geometry_test_basic WHERE geom <> CAST(unhex('0101000000000000000000F03F0000000000000040') AS GEOMETRY);

SELECT id FROM geometry_test_basic
WHERE geom IN (CAST(unhex('0101000000000000000000F03F0000000000000040') AS GEOMETRY),
               CAST(unhex('010100000000000000000008400000000000001040') AS GEOMETRY));

-- Geometry values compare by their WKB bytes, so they can be grouped, sorted, deduplicated and joined
SELECT hex(CAST(geom AS BINARY)), count(*) FROM geometry_test_basic GROUP BY geom;

SELECT DISTINCT hex(CAST(geom AS BINARY)) FROM geometry_test_basic;

SELECT count(DISTINCT geom), count(geom), count(*) FROM geometry_test_basic;

SELECT id, hex(CAST(geom AS BINARY)) FROM geometry_test_basic ORDER BY geom, id;

SELECT a.id, b.id FROM geometry_test_basic a JOIN geometry_test_basic b ON a.geom = b.geom;

SELECT hex(CAST(min(geom) AS BINARY)), hex(CAST(max(geom) AS BINARY)) FROM geometry_test_basic;

-- Geometry values can be processed by the ESRI spatial functions through WKB
SELECT id, ST_AsText(ST_GeomFromWKB(CAST(geom AS BINARY))) FROM geometry_test_basic WHERE geom IS NOT NULL;

SELECT id, ST_Intersects(ST_GeomFromWKB(CAST(geom AS BINARY)), ST_GeomFromText('POLYGON ((0 0, 2 0, 2 3, 0 3, 0 0))'))
FROM geometry_test_basic WHERE geom IS NOT NULL;

-- Schema evolution: add, rename with a comment, replace and drop geometry columns
ALTER TABLE geometry_test_basic ADD COLUMNS (geom_4326 GEOMETRY('EPSG:4326'));

DESCRIBE geometry_test_basic;

INSERT INTO geometry_test_basic VALUES
(5, NULL, NULL, CAST(ST_AsBinary(ST_Point(5, 6)) AS GEOMETRY('EPSG:4326')));

SELECT id, ST_AsText(ST_GeomFromWKB(CAST(geom_4326 AS BINARY))) FROM geometry_test_basic;

ALTER TABLE geometry_test_basic CHANGE COLUMN geom_4326 geom_wgs84 GEOMETRY('EPSG:4326') COMMENT 'renamed';

DESCRIBE geometry_test_basic;

SELECT id, hex(CAST(geom_wgs84 AS BINARY)) FROM geometry_test_basic WHERE geom_wgs84 IS NOT NULL;

-- CTAS preserves geometry column types from the query schema
CREATE TABLE geometry_test_ctas STORED BY ICEBERG tblproperties('format-version'='3') AS
SELECT * FROM geometry_test_basic;

DESCRIBE geometry_test_ctas;

SELECT id, hex(CAST(geom AS BINARY)), hex(CAST(geom_3857 AS BINARY)), hex(CAST(geom_wgs84 AS BINARY))
FROM geometry_test_ctas;

-- CTLT preserves geometry column types from the source table
CREATE TABLE geometry_test_ctlt LIKE geometry_test_basic STORED BY ICEBERG tblproperties('format-version'='3');

DESCRIBE geometry_test_ctlt;

INSERT INTO geometry_test_ctlt SELECT * FROM geometry_test_basic;

SELECT id, hex(CAST(geom AS BINARY)), hex(CAST(geom_3857 AS BINARY)), hex(CAST(geom_wgs84 AS BINARY))
FROM geometry_test_ctlt;

ALTER TABLE geometry_test_ctlt REPLACE COLUMNS (id INT, geom_3857 GEOMETRY('EPSG:3857') COMMENT 'web mercator');

DESCRIBE geometry_test_ctlt;

ALTER TABLE geometry_test_ctas DROP COLUMN geom_wgs84;

DESCRIBE geometry_test_ctas;

-- Views keep the geometry type and CRS of their columns
CREATE VIEW geometry_test_view AS SELECT id, geom_3857 FROM geometry_test_basic WHERE geom_3857 IS NOT NULL;

DESCRIBE geometry_test_view;

SELECT id, hex(CAST(geom_3857 AS BINARY)) FROM geometry_test_view;

-- Avro data files store geometry values as binary
CREATE EXTERNAL TABLE geometry_test_avro (id INT, geom GEOMETRY('EPSG:3857'))
STORED BY ICEBERG STORED AS AVRO tblproperties('format-version'='3');

INSERT INTO geometry_test_avro SELECT id, geom_3857 FROM geometry_test_basic;

DESCRIBE geometry_test_avro;

SELECT id, hex(CAST(geom AS BINARY)) FROM geometry_test_avro;
