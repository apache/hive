set hive.stats.column.autogather=false;
-- due to L137 in LimitPushDownOptimization Not safe to continue for RS-GBY-GBY-LIM kind of pipelines. See HIVE-10607 for more.

set hive.multigroupby.singlereducer=true;

-- SORT_QUERY_RESULTS

CREATE TABLE dest_g2(key STRING, c1 INT, c2 STRING, c3 INT, c4 INT) STORED AS TEXTFILE;
CREATE TABLE dest_g3(key STRING, c1 INT, c2 STRING, c3 INT, c4 INT) STORED AS TEXTFILE;
CREATE TABLE dest_g4(key STRING, c1 INT, c2 STRING, c3 INT, c4 INT) STORED AS TEXTFILE;
CREATE TABLE dest_h2(key STRING, c1 INT, c2 STRING, c3 INT, c4 INT) STORED AS TEXTFILE;
CREATE TABLE dest_h3(key STRING, c1 INT, c2 STRING, c3 INT, c4 INT) STORED AS TEXTFILE;

EXPLAIN
FROM src
INSERT OVERWRITE TABLE dest_g2 SELECT substr(src.key,1,1), count(DISTINCT substr(src.value,5)), concat(substr(src.key,1,1),sum(substr(src.value,5))), sum(DISTINCT substr(src.value, 5)), count(src.value) WHERE substr(src.key,1,1) >= 5 GROUP BY substr(src.key,1,1)
INSERT OVERWRITE TABLE dest_g3 SELECT substr(src.key,1,1), count(DISTINCT substr(src.value,5)), concat(substr(src.key,1,1),sum(substr(src.value,5))), sum(DISTINCT substr(src.value, 5)), count(src.value) WHERE substr(src.key,1,1) < 5 GROUP BY substr(src.key,1,1)
INSERT OVERWRITE TABLE dest_g4 SELECT substr(src.key,1,1), count(DISTINCT substr(src.value,5)), concat(substr(src.key,1,1),sum(substr(src.value,5))), sum(DISTINCT substr(src.value, 5)), count(src.value) GROUP BY substr(src.key,1,1);

FROM src
INSERT OVERWRITE TABLE dest_g2 SELECT substr(src.key,1,1), count(DISTINCT substr(src.value,5)), concat(substr(src.key,1,1),sum(substr(src.value,5))), sum(DISTINCT substr(src.value, 5)), count(src.value) WHERE substr(src.key,1,1) >= 5 GROUP BY substr(src.key,1,1)
INSERT OVERWRITE TABLE dest_g3 SELECT substr(src.key,1,1), count(DISTINCT substr(src.value,5)), concat(substr(src.key,1,1),sum(substr(src.value,5))), sum(DISTINCT substr(src.value, 5)), count(src.value) WHERE substr(src.key,1,1) < 5 GROUP BY substr(src.key,1,1)
INSERT OVERWRITE TABLE dest_g4 SELECT substr(src.key,1,1), count(DISTINCT substr(src.value,5)), concat(substr(src.key,1,1),sum(substr(src.value,5))), sum(DISTINCT substr(src.value, 5)), count(src.value) GROUP BY substr(src.key,1,1);

SELECT * FROM dest_g2;
SELECT * FROM dest_g3;
SELECT * FROM dest_g4;

EXPLAIN
FROM src
INSERT OVERWRITE TABLE dest_g2 SELECT substr(src.key,1,1), count(DISTINCT substr(src.value,5)), concat(substr(src.key,1,1),sum(substr(src.value,5))), sum(DISTINCT substr(src.value, 5)), count(src.value) WHERE substr(src.key,1,1) >= 5 GROUP BY substr(src.key,1,1)
INSERT OVERWRITE TABLE dest_g3 SELECT substr(src.key,1,1), count(DISTINCT substr(src.value,5)), concat(substr(src.key,1,1),sum(substr(src.value,5))), sum(DISTINCT substr(src.value, 5)), count(src.value) WHERE substr(src.key,1,1) < 5 GROUP BY substr(src.key,1,1)
INSERT OVERWRITE TABLE dest_g4 SELECT substr(src.key,1,1), count(DISTINCT substr(src.value,5)), concat(substr(src.key,1,1),sum(substr(src.value,5))), sum(DISTINCT substr(src.value, 5)), count(src.value) GROUP BY substr(src.key,1,1)
INSERT OVERWRITE TABLE dest_h2 SELECT substr(src.key,1,1) as c1, count(DISTINCT substr(src.value,5)) as c2, concat(substr(src.key,1,1),sum(substr(src.value,5))) as c3, sum(substr(src.value, 5)) as c4, count(src.value) as c6 GROUP BY substr(src.key,1,1), substr(src.key,2,1) ORDER BY c1, c2  LIMIT 10
INSERT OVERWRITE TABLE dest_h3 SELECT substr(src.key,1,1), count(DISTINCT substr(src.value,5)), concat(substr(src.key,1,1),sum(substr(src.value,5))), sum(substr(src.value, 5)), count(src.value) WHERE substr(src.key,1,1) >= 5 GROUP BY substr(src.key,1,1), substr(src.key,2,1);

FROM src
INSERT OVERWRITE TABLE dest_g2 SELECT substr(src.key,1,1), count(DISTINCT substr(src.value,5)), concat(substr(src.key,1,1),sum(substr(src.value,5))), sum(DISTINCT substr(src.value, 5)), count(src.value) WHERE substr(src.key,1,1) >= 5 GROUP BY substr(src.key,1,1)
INSERT OVERWRITE TABLE dest_g3 SELECT substr(src.key,1,1), count(DISTINCT substr(src.value,5)), concat(substr(src.key,1,1),sum(substr(src.value,5))), sum(DISTINCT substr(src.value, 5)), count(src.value) WHERE substr(src.key,1,1) < 5 GROUP BY substr(src.key,1,1)
INSERT OVERWRITE TABLE dest_g4 SELECT substr(src.key,1,1), count(DISTINCT substr(src.value,5)), concat(substr(src.key,1,1),sum(substr(src.value,5))), sum(DISTINCT substr(src.value, 5)), count(src.value) GROUP BY substr(src.key,1,1)
INSERT OVERWRITE TABLE dest_h2 SELECT substr(src.key,1,1) as c1, count(DISTINCT substr(src.value,5)) as c2, concat(substr(src.key,1,1),sum(substr(src.value,5))) as c3, sum(substr(src.value, 5)) as c4, count(src.value) as c6 GROUP BY substr(src.key,1,1), substr(src.key,2,1) ORDER BY c1, c2  LIMIT 10
INSERT OVERWRITE TABLE dest_h3 SELECT substr(src.key,1,1), count(DISTINCT substr(src.value,5)), concat(substr(src.key,1,1),sum(substr(src.value,5))), sum(substr(src.value, 5)), count(src.value) WHERE substr(src.key,1,1) >= 5 GROUP BY substr(src.key,1,1), substr(src.key,2,1);

SELECT * FROM dest_g2;
SELECT * FROM dest_g3;
SELECT * FROM dest_g4;
SELECT * FROM dest_h2;
SELECT * FROM dest_h3;

DROP TABLE dest_g2;
DROP TABLE dest_g3;
DROP TABLE dest_g4;
DROP TABLE dest_h2;
DROP TABLE dest_h3;
