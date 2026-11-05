-- Mask random uuid
--! qt:replace:/(\s+'uuid'=')\S+('\s*)/$1#Masked#$2/
-- SORT_QUERY_RESULTS

CREATE EXTERNAL TABLE geometry_expr (id INT, geom GEOMETRY, geom_3857 GEOMETRY('EPSG:3857'))
STORED BY ICEBERG tblproperties('format-version'='3');

INSERT INTO geometry_expr VALUES
(1, unhex('0101000000000000000000F03F0000000000000040'), unhex('010100000000000000000008400000000000001040')),
(2, NULL, unhex('0101000000000000000000F03F0000000000000040')),
(3, unhex('010100000000000000000008400000000000001040'), NULL);

-- Casts between binary and geometry, and between geometry types with the same CRS
SELECT hex(CAST(CAST(unhex('0101000000000000000000F03F0000000000000040') AS GEOMETRY('EPSG:3857')) AS BINARY));

SELECT hex(CAST(CAST(geom_3857 AS GEOMETRY('EPSG:3857')) AS BINARY)) FROM geometry_expr;

SELECT hex(CAST(CAST(CAST(unhex('0101000000000000000000F03F0000000000000040') AS GEOMETRY('EPSG:32632')) AS GEOMETRY('epsg:32632')) AS BINARY));

SELECT hex(CAST(CAST(geom AS GEOMETRY('OGC:CRS84')) AS BINARY)) FROM geometry_expr;

-- CRS spellings that differ only in case are kept as written, but values can be used across them
CREATE EXTERNAL TABLE geometry_expr_case (id INT, g_upper GEOMETRY('EPSG:32632'), g_lower GEOMETRY('epsg:32632'))
STORED BY ICEBERG tblproperties('format-version'='3');

DESCRIBE geometry_expr_case;

INSERT INTO geometry_expr_case SELECT 1,
    CAST(unhex('0101000000000000000000F03F0000000000000040') AS GEOMETRY('epsg:32632')),
    CAST(unhex('010100000000000000000008400000000000001040') AS GEOMETRY('EPSG:32632'));

INSERT INTO geometry_expr_case SELECT id + 1, g_lower, g_upper FROM geometry_expr_case;

INSERT INTO geometry_expr_case SELECT 3, g_upper, g_upper FROM geometry_expr_case WHERE id = 1;

SELECT id, hex(CAST(g_upper AS BINARY)), hex(CAST(g_lower AS BINARY)) FROM geometry_expr_case;

SELECT id FROM geometry_expr_case WHERE g_upper = g_lower;

CREATE TABLE geometry_expr_case_ctas STORED BY ICEBERG tblproperties('format-version'='3') AS
SELECT COALESCE(g_lower, g_upper) AS c_coalesce, CASE WHEN id = 1 THEN g_upper ELSE g_lower END AS c_case
FROM geometry_expr_case;

DESCRIBE geometry_expr_case_ctas;

CREATE TABLE geometry_expr_case_union STORED BY ICEBERG tblproperties('format-version'='3') AS
SELECT g_upper AS u FROM geometry_expr_case
UNION ALL
SELECT g_lower AS u FROM geometry_expr_case;

DESCRIBE geometry_expr_case_union;

SELECT hex(CAST(u AS BINARY)), count(*) FROM geometry_expr_case_union GROUP BY u;

-- Casting does not transform coordinates: the CRS is relabeled by casting through binary
SELECT id, hex(CAST(CAST(CAST(geom AS BINARY) AS GEOMETRY('EPSG:3857')) AS BINARY)) FROM geometry_expr;

CREATE TABLE geometry_expr_relabel STORED BY ICEBERG tblproperties('format-version'='3') AS
SELECT id, CAST(CAST(geom AS BINARY) AS GEOMETRY('EPSG:4326')) AS geom_4326 FROM geometry_expr;

DESCRIBE geometry_expr_relabel;

SELECT id, hex(CAST(geom_4326 AS BINARY)) FROM geometry_expr_relabel;

-- Typed NULL values
SELECT CAST(NULL AS GEOMETRY), CAST(NULL AS GEOMETRY('EPSG:3857')) IS NULL;

SELECT hex(CAST(CAST(CAST(NULL AS BINARY) AS GEOMETRY) AS BINARY));

-- Conditional expressions keep the geometry type when both sides have the same CRS
CREATE TABLE geometry_expr_conditional STORED BY ICEBERG tblproperties('format-version'='3') AS
SELECT id,
    COALESCE(geom_3857, CAST(unhex('0101000000000000000000F03F0000000000000040') AS GEOMETRY('EPSG:3857'))) AS c_coalesce,
    CASE WHEN id = 1 THEN geom_3857 ELSE NULL END AS c_case,
    IF(id = 2, geom_3857, NULL) AS c_if,
    NVL(geom_3857, geom_3857) AS c_nvl,
    COALESCE(geom, geom) AS c_default_crs
FROM geometry_expr;

DESCRIBE geometry_expr_conditional;

SELECT id, hex(CAST(c_coalesce AS BINARY)), hex(CAST(c_case AS BINARY)), hex(CAST(c_if AS BINARY)),
    hex(CAST(c_nvl AS BINARY)), hex(CAST(c_default_crs AS BINARY))
FROM geometry_expr_conditional;

-- Set operations over geometry columns with the same CRS, including a constant
CREATE TABLE geometry_expr_union STORED BY ICEBERG tblproperties('format-version'='3') AS
SELECT geom_3857 AS u FROM geometry_expr
UNION ALL
SELECT CAST(unhex('010100000000000000000008400000000000001040') AS GEOMETRY('EPSG:3857')) AS u;

DESCRIBE geometry_expr_union;

SELECT hex(CAST(u AS BINARY)) FROM geometry_expr_union;

SELECT hex(CAST(u AS BINARY)) FROM (
    SELECT geom AS u FROM geometry_expr
    UNION
    SELECT geom AS u FROM geometry_expr) t;

-- Constant geometry values keep their type after constant folding
EXPLAIN CBO
SELECT CAST(unhex('0101000000000000000000F03F0000000000000040') AS GEOMETRY('EPSG:3857')) FROM geometry_expr;

EXPLAIN
SELECT CAST(unhex('0101000000000000000000F03F0000000000000040') AS GEOMETRY('EPSG:3857')) FROM geometry_expr;

EXPLAIN
SELECT id FROM geometry_expr
WHERE geom_3857 = CAST(unhex('0101000000000000000000F03F0000000000000040') AS GEOMETRY('EPSG:3857'));

CREATE TABLE geometry_expr_constant STORED BY ICEBERG tblproperties('format-version'='3') AS
SELECT id, CAST(unhex('0101000000000000000000F03F0000000000000040') AS GEOMETRY('EPSG:3857')) AS c,
    CAST(NULL AS GEOMETRY('EPSG:3857')) AS n
FROM geometry_expr;

DESCRIBE geometry_expr_constant;

SELECT id, hex(CAST(c AS BINARY)), n FROM geometry_expr_constant;

-- WKT is converted to geometry through the spatial functions
INSERT INTO geometry_expr
SELECT 4, ST_AsBinary(ST_GeomFromText('POINT (7 8)')), CAST(ST_AsBinary(ST_GeomFromText('POINT (9 10)')) AS GEOMETRY('EPSG:3857'));

SELECT id, ST_AsText(ST_GeomFromWKB(CAST(geom AS BINARY))), ST_AsText(ST_GeomFromWKB(CAST(geom_3857 AS BINARY)))
FROM geometry_expr WHERE id = 4;
