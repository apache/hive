CREATE TABLE dest_g2(key STRING, c1 INT, c2 STRING) STORED AS TEXTFILE;

EXPLAIN
FROM src
INSERT OVERWRITE TABLE dest_g2 SELECT substr(src.key,0,1), count(DISTINCT substr(src.value,4)), concat(substr(src.key,0,1),sum(substr(src.value,4))) GROUP BY substr(src.key,0,1);

FROM src
INSERT OVERWRITE TABLE dest_g2 SELECT substr(src.key,0,1), count(DISTINCT substr(src.value,4)), concat(substr(src.key,0,1),sum(substr(src.value,4))) GROUP BY substr(src.key,0,1);

SELECT dest_g2.* FROM dest_g2;
