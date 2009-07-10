CREATE TABLE dest1(len INT);

EXPLAIN
FROM src INSERT OVERWRITE TABLE dest1 SELECT length(src.value);

FROM src INSERT OVERWRITE TABLE dest1 SELECT length(src.value);

SELECT dest1.* FROM dest1;

DROP TABLE dest1;
