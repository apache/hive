-- Mask random uuid
--! qt:replace:/(\s+'uuid'=')\S+('\s*)/$1#Masked#$2/
-- SORT_QUERY_RESULTS

-- Geometry values nested in arrays, structs and maps
CREATE EXTERNAL TABLE geometry_nested (
    id INT,
    a ARRAY<GEOMETRY>,
    s STRUCT<g: GEOMETRY('EPSG:3857'), n: INT>,
    m MAP<STRING, GEOMETRY>
) STORED BY ICEBERG tblproperties('format-version'='3');

DESCRIBE geometry_nested;

SHOW CREATE TABLE geometry_nested;

-- Complex values are not implicitly converted, so the geometry elements are cast explicitly
INSERT INTO geometry_nested SELECT 1,
    array(CAST(unhex('0101000000000000000000F03F0000000000000040') AS GEOMETRY),
          CAST(unhex('010100000000000000000008400000000000001040') AS GEOMETRY)),
    named_struct('g', CAST(unhex('0101000000000000000000F03F0000000000000040') AS GEOMETRY('EPSG:3857')), 'n', 1),
    map('k1', CAST(unhex('010100000000000000000008400000000000001040') AS GEOMETRY),
        'k2', CAST(unhex('0101000000000000000000F03F0000000000000040') AS GEOMETRY));

INSERT INTO geometry_nested SELECT 2,
    CAST(NULL AS ARRAY<GEOMETRY>),
    CAST(NULL AS STRUCT<g: GEOMETRY('EPSG:3857'), n: INT>),
    CAST(NULL AS MAP<STRING, GEOMETRY>);

INSERT INTO geometry_nested SELECT 3,
    array(CAST(NULL AS GEOMETRY)),
    named_struct('g', CAST(NULL AS GEOMETRY('EPSG:3857')), 'n', 3),
    map('k', CAST(NULL AS GEOMETRY));

SELECT id, size(a), hex(CAST(a[0] AS BINARY)), hex(CAST(a[1] AS BINARY)), hex(CAST(s.g AS BINARY)), s.n,
    hex(CAST(m['k1'] AS BINARY)), hex(CAST(m['k2'] AS BINARY)), size(m)
FROM geometry_nested;

SELECT id, s.g IS NULL, a IS NULL, m IS NULL FROM geometry_nested;

SELECT id, pos, hex(CAST(e AS BINARY)) FROM geometry_nested LATERAL VIEW posexplode(a) t AS pos, e;

SELECT id, k, hex(CAST(v AS BINARY)) FROM geometry_nested LATERAL VIEW explode(m) t AS k, v;

SELECT id, ST_AsText(ST_GeomFromWKB(CAST(s.g AS BINARY))) FROM geometry_nested WHERE s.g IS NOT NULL;

-- Filter and project on nested geometry values
SELECT id FROM geometry_nested WHERE s.g = CAST(unhex('0101000000000000000000F03F0000000000000040') AS GEOMETRY('EPSG:3857'));

SELECT id FROM geometry_nested WHERE array_contains(a, CAST(unhex('010100000000000000000008400000000000001040') AS GEOMETRY));

-- Nested geometry types are kept by CTAS
CREATE TABLE geometry_nested_ctas STORED BY ICEBERG tblproperties('format-version'='3') AS
SELECT id, a, s, m FROM geometry_nested;

DESCRIBE geometry_nested_ctas;

SELECT id, size(a), hex(CAST(s.g AS BINARY)), size(m) FROM geometry_nested_ctas;

-- Vectorized and non-vectorized reads return the same values
set hive.vectorized.execution.enabled=false;

SELECT id, hex(CAST(a[0] AS BINARY)), hex(CAST(s.g AS BINARY)), hex(CAST(m['k1'] AS BINARY)) FROM geometry_nested;

set hive.vectorized.execution.enabled=true;

SELECT id, hex(CAST(a[0] AS BINARY)), hex(CAST(s.g AS BINARY)), hex(CAST(m['k1'] AS BINARY)) FROM geometry_nested;
