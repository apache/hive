--! qt:dataset:src
set hive.multigroupby.singlereducer=true;

CREATE TABLE dest_g2_n4(key STRING, c1 INT) STORED AS TEXTFILE;
CREATE TABLE dest_g3_n0(key STRING, c1 INT, c2 INT) STORED AS TEXTFILE;

-- SORT_QUERY_RESULTS

EXPLAIN
FROM src
INSERT OVERWRITE TABLE dest_g2_n4 SELECT substr(src.key,1,1), count(DISTINCT src.key) WHERE substr(src.key,1,1) >= 5 GROUP BY substr(src.key,1,1)
INSERT OVERWRITE TABLE dest_g3_n0 SELECT substr(src.key,1,1), count(DISTINCT src.key), count(src.value) WHERE substr(src.key,1,1) < 5 GROUP BY substr(src.key,1,1);

FROM src
INSERT OVERWRITE TABLE dest_g2_n4 SELECT substr(src.key,1,1), count(DISTINCT src.key) WHERE substr(src.key,1,1) >= 5 GROUP BY substr(src.key,1,1)
INSERT OVERWRITE TABLE dest_g3_n0 SELECT substr(src.key,1,1), count(DISTINCT src.key), count(src.value) WHERE substr(src.key,1,1) < 5 GROUP BY substr(src.key,1,1);

SELECT * FROM dest_g2_n4;
SELECT * FROM dest_g3_n0;

DROP TABLE dest_g2_n4;
DROP TABLE dest_g3_n0;
