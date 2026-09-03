set hive.explain.user=false;

drop table if exists variant_cast;

CREATE EXTERNAL TABLE variant_cast (
  id INT,
  v VARIANT
) STORED BY ICEBERG
TBLPROPERTIES (
  'format-version'='3'
);

-- typed variant construction: values JSON cannot express keep their real type
INSERT INTO variant_cast VALUES
(1, CAST(true AS VARIANT)),
(2, CAST(42 AS VARIANT)),
(3, CAST(1.5BD AS VARIANT)),
(4, CAST(unhex('DEADBEEF') AS VARIANT)),
(5, CAST(DATE '2026-07-28' AS VARIANT)),
(6, CAST(TIMESTAMP '2026-07-28 10:00:00' AS VARIANT)),
(7, CAST('plain' AS VARIANT)),
(8, CAST(array(1, 2, 3) AS VARIANT)),
(9, CAST(NULL AS VARIANT));

SELECT id, v FROM variant_cast ORDER BY id;

-- binary node is rendered as base64, matching Impala/Trino
SELECT variant_get(v, '$', 'string') FROM variant_cast WHERE id = 4;

-- cast over a column expression, not a constant
drop table if exists variant_src;

CREATE TABLE variant_src (id INT, s STRING);

INSERT INTO variant_src VALUES (11, 'from_col');

EXPLAIN INSERT INTO variant_cast SELECT id, CAST(s AS VARIANT) FROM variant_src;

INSERT INTO variant_cast SELECT id, CAST(s AS VARIANT) FROM variant_src;

SELECT id, v FROM variant_cast WHERE id > 10;

-- identity cast over a reader-produced variant column
SELECT id, CAST(v AS VARIANT) FROM variant_cast WHERE id > 10;

drop table variant_cast;
drop table variant_src;