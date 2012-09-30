-- the query is from auto_join26.q

CREATE TABLE dest_co1(key INT, cnt INT);
CREATE TABLE dest_co2(key INT, cnt INT);

set hive.optimize.correlation=false;
EXPLAIN
INSERT OVERWRITE TABLE dest_co1
SELECT x.key, count(1) FROM src1 x JOIN src y ON (x.key = y.key) group by x.key;

INSERT OVERWRITE TABLE dest_co1
SELECT  x.key, count(1) FROM src1 x JOIN src y ON (x.key = y.key) group by x.key;

set hive.optimize.correlation=true;
EXPLAIN
INSERT OVERWRITE TABLE dest_co2
SELECT x.key, count(1) FROM src1 x JOIN src y ON (x.key = y.key) group by x.key;

INSERT OVERWRITE TABLE dest_co2
SELECT  x.key, count(1) FROM src1 x JOIN src y ON (x.key = y.key) group by x.key;

-- dest_co1 and dest_co2 should be same
SELECT * FROM dest_co1 x ORDER BY x.key;
SELECT * FROM dest_co2 x ORDER BY x.key;