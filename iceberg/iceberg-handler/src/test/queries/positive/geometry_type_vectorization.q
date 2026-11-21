-- SORT_QUERY_RESULTS

set hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

CREATE EXTERNAL TABLE geometry_vec (id INT, geom GEOMETRY, geom_3857 GEOMETRY('EPSG:3857'))
STORED BY ICEBERG tblproperties('format-version'='3');

INSERT INTO geometry_vec VALUES
(1, unhex('0101000000000000000000F03F0000000000000040'), unhex('010100000000000000000008400000000000001040')),
(2, unhex('010100000000000000000008400000000000001040'), NULL),
(3, NULL, unhex('0101000000000000000000F03F0000000000000040')),
(4, unhex('0101000000000000000000F03F0000000000000040'), unhex('0101000000000000000000F03F0000000000000040'));

-- Geometry columns are read by the vectorized Parquet reader
EXPLAIN VECTORIZATION DETAIL
SELECT id, geom, geom_3857 FROM geometry_vec WHERE geom IS NOT NULL;

SELECT id, hex(CAST(geom AS BINARY)), hex(CAST(geom_3857 AS BINARY)) FROM geometry_vec WHERE geom IS NOT NULL;

EXPLAIN VECTORIZATION DETAIL
SELECT hex(CAST(geom AS BINARY)), count(*) FROM geometry_vec GROUP BY geom;

SELECT hex(CAST(geom AS BINARY)), count(*) FROM geometry_vec GROUP BY geom;

EXPLAIN VECTORIZATION DETAIL
SELECT id FROM geometry_vec
WHERE geom_3857 = CAST(unhex('0101000000000000000000F03F0000000000000040') AS GEOMETRY('EPSG:3857'));

SELECT id FROM geometry_vec
WHERE geom_3857 = CAST(unhex('0101000000000000000000F03F0000000000000040') AS GEOMETRY('EPSG:3857'));

SELECT a.id, b.id FROM geometry_vec a JOIN geometry_vec b ON CAST(a.geom AS BINARY) = CAST(b.geom_3857 AS BINARY);

SELECT id, hex(CAST(geom AS BINARY)) FROM geometry_vec ORDER BY geom, id;

-- The same queries without vectorization
set hive.vectorized.execution.enabled=false;

SELECT id, hex(CAST(geom AS BINARY)), hex(CAST(geom_3857 AS BINARY)) FROM geometry_vec WHERE geom IS NOT NULL;

SELECT hex(CAST(geom AS BINARY)), count(*) FROM geometry_vec GROUP BY geom;

SELECT id FROM geometry_vec
WHERE geom_3857 = CAST(unhex('0101000000000000000000F03F0000000000000040') AS GEOMETRY('EPSG:3857'));

SELECT a.id, b.id FROM geometry_vec a JOIN geometry_vec b ON CAST(a.geom AS BINARY) = CAST(b.geom_3857 AS BINARY);

SELECT id, hex(CAST(geom AS BINARY)) FROM geometry_vec ORDER BY geom, id;
