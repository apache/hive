CREATE TABLE T1(key INT, val STRING);
LOAD DATA LOCAL INPATH '../data/files/T1.txt' INTO TABLE T1;
CREATE TABLE T2(key INT, val STRING);
LOAD DATA LOCAL INPATH '../data/files/T2.txt' INTO TABLE T2;
CREATE TABLE T3(key INT, val STRING);
LOAD DATA LOCAL INPATH '../data/files/T1.txt' INTO TABLE T3;

CREATE TABLE dest_co1(key INT, cnt INT);
CREATE TABLE dest_co2(key INT, cnt INT);

set hive.optimize.correlation=false;
EXPLAIN
INSERT OVERWRITE TABLE dest_co1
SELECT y.key, count(1) FROM T2 x JOIN T1 y ON (x.key = y.key) JOIN T3 z ON (y.key = z.key) GROUP BY y.key;
INSERT OVERWRITE TABLE dest_co1
SELECT y.key, count(1) FROM T2 x JOIN T1 y ON (x.key = y.key) JOIN T3 z ON (y.key = z.key) GROUP BY y.key;
set hive.optimize.correlation=true;
EXPLAIN
INSERT OVERWRITE TABLE dest_co2
SELECT y.key, count(1) FROM T2 x JOIN T1 y ON (x.key = y.key) JOIN T3 z ON (y.key = z.key) GROUP BY y.key;
INSERT OVERWRITE TABLE dest_co2
SELECT y.key, count(1) FROM T2 x JOIN T1 y ON (x.key = y.key) JOIN T3 z ON (y.key = z.key) GROUP BY y.key;
-- dest_co1 and dest_co2 should be same
SELECT * FROM dest_co1 x ORDER BY x.key, x.cnt;
SELECT * FROM dest_co2 x ORDER BY x.key, x.cnt;

set hive.optimize.correlation=false;
EXPLAIN
INSERT OVERWRITE TABLE dest_co1
SELECT x.key, count(1) FROM T2 x LEFT OUTER JOIN T1 y ON (x.key = y.key) LEFT OUTER JOIN T3 z ON (y.key = z.key) GROUP BY x.key;
INSERT OVERWRITE TABLE dest_co1
SELECT x.key, count(1) FROM T2 x LEFT OUTER JOIN T1 y ON (x.key = y.key) LEFT OUTER JOIN T3 z ON (y.key = z.key) GROUP BY x.key;
set hive.optimize.correlation=true;
EXPLAIN
INSERT OVERWRITE TABLE dest_co2
SELECT x.key, count(1) FROM T2 x LEFT OUTER JOIN T1 y ON (x.key = y.key) LEFT OUTER JOIN T3 z ON (y.key = z.key) GROUP BY x.key;
INSERT OVERWRITE TABLE dest_co2
SELECT x.key, count(1) FROM T2 x LEFT OUTER JOIN T1 y ON (x.key = y.key) LEFT OUTER JOIN T3 z ON (y.key = z.key) GROUP BY x.key;
-- dest_co1 and dest_co2 should be same
SELECT * FROM dest_co1 x ORDER BY x.key, x.cnt;
SELECT * FROM dest_co2 x ORDER BY x.key, x.cnt;

set hive.optimize.correlation=false;
-- FULL OUTER JOIN will not be optimized
EXPLAIN
INSERT OVERWRITE TABLE dest_co1
SELECT z.key, count(1) FROM T2 x RIGHT OUTER JOIN T1 y ON (x.key = y.key) RIGHT OUTER JOIN T3 z ON (y.key = z.key) GROUP BY z.key;
INSERT OVERWRITE TABLE dest_co1
SELECT z.key, count(1) FROM T2 x RIGHT OUTER JOIN T1 y ON (x.key = y.key) RIGHT OUTER JOIN T3 z ON (y.key = z.key) GROUP BY z.key;
set hive.optimize.correlation=true;
EXPLAIN
INSERT OVERWRITE TABLE dest_co2
SELECT z.key, count(1) FROM T2 x RIGHT OUTER JOIN T1 y ON (x.key = y.key) RIGHT OUTER JOIN T3 z ON (y.key = z.key) GROUP BY z.key;
INSERT OVERWRITE TABLE dest_co2
SELECT z.key, count(1) FROM T2 x RIGHT OUTER JOIN T1 y ON (x.key = y.key) RIGHT OUTER JOIN T3 z ON (y.key = z.key) GROUP BY z.key;
-- dest_co1 and dest_co2 should be same
SELECT * FROM dest_co1 x ORDER BY x.key, x.cnt;
SELECT * FROM dest_co2 x ORDER BY x.key, x.cnt;

set hive.optimize.correlation=false;
EXPLAIN
INSERT OVERWRITE TABLE dest_co1
SELECT y.key, count(1) FROM T2 x JOIN T1 y ON (x.key = y.key) FULL OUTER JOIN T3 z ON (y.key = z.key) GROUP BY y.key;
INSERT OVERWRITE TABLE dest_co1
SELECT y.key, count(1) FROM T2 x JOIN T1 y ON (x.key = y.key) FULL OUTER JOIN T3 z ON (y.key = z.key) GROUP BY y.key;
set hive.optimize.correlation=true;
EXPLAIN
INSERT OVERWRITE TABLE dest_co2
SELECT y.key, count(1) FROM T2 x JOIN T1 y ON (x.key = y.key) FULL OUTER JOIN T3 z ON (y.key = z.key) GROUP BY y.key;
INSERT OVERWRITE TABLE dest_co2
SELECT y.key, count(1) FROM T2 x JOIN T1 y ON (x.key = y.key) FULL OUTER JOIN T3 z ON (y.key = z.key) GROUP BY y.key;
-- dest_co1 and dest_co2 should be same
SELECT * FROM dest_co1 x ORDER BY x.key, x.cnt;
SELECT * FROM dest_co2 x ORDER BY x.key, x.cnt;
