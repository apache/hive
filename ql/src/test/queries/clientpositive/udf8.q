--! qt:dataset:src
CREATE TABLE dest1_n54(c1 STRING) STORED AS TEXTFILE;

FROM src INSERT OVERWRITE TABLE dest1_n54 SELECT '' WHERE src.key = 86;

FROM src INSERT OVERWRITE TABLE dest1_n54 SELECT '1' WHERE src.key = 86;

EXPLAIN
SELECT avg(c1), sum(c1), count(c1) FROM dest1_n54;

SELECT avg(c1), sum(c1), count(c1) FROM dest1_n54;
