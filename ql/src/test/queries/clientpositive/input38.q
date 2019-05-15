--! qt:dataset:src

CREATE TABLE dest1_n91(key STRING, value STRING) STORED AS TEXTFILE;

EXPLAIN
FROM (
  FROM src
  SELECT TRANSFORM(src.key, src.value, 1+2, 3+4)
         USING 'cat'
) tmap
INSERT OVERWRITE TABLE dest1_n91 SELECT tmap.key, tmap.value;

FROM (
  FROM src
  SELECT TRANSFORM(src.key, src.value, 1+2, 3+4)
         USING 'cat'
) tmap
INSERT OVERWRITE TABLE dest1_n91 SELECT tmap.key, tmap.value;


SELECT dest1_n91.* FROM dest1_n91;


