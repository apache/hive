USE default;

-- this query tests all the udfs provided to work with binary types

SELECT
  key,
  value,
  LENGTH(CAST(src.key AS BINARY)),
  LENGTH(CAST(src.value AS BINARY)),
  CONCAT(CAST(src.key AS BINARY), CAST(src.value AS BINARY)),
  SUBSTR(CAST(src.value AS BINARY), 1, 4),
  SUBSTR(CAST(src.value AS BINARY), 3),
  SUBSTR(CAST(src.value AS BINARY), -4, 3)
FROM src
ORDER BY value
LIMIT 100;
