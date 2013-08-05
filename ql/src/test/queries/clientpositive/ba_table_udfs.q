USE default;

CREATE TABLE dest1(bytes1 BINARY,
                   bytes2 BINARY);

FROM src INSERT OVERWRITE TABLE dest1
SELECT
  CAST(key AS BINARY),
  CAST(value AS BINARY)
ORDER BY value
LIMIT 100;

--Add in a null row for good measure
INSERT INTO TABLE dest1 SELECT NULL, NULL FROM dest1 LIMIT 1;

-- this query tests all the udfs provided to work with binary types

SELECT
  bytes1,
  bytes2,
  LENGTH(bytes1),
  CONCAT(bytes1, bytes2),
  SUBSTR(bytes2, 1, 4),
  SUBSTR(bytes2, 3),
  SUBSTR(bytes2, -4, 3)
FROM dest1
ORDER BY bytes2;
