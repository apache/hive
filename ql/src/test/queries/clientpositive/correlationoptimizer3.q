CREATE TABLE dest_co1(key INT, cnt INT, value STRING);
CREATE TABLE dest_co2(key INT, cnt INT, value STRING);


set hive.optimize.correlation=false;
EXPLAIN
INSERT OVERWRITE TABLE dest_co1
SELECT b.key, b.cnt, d.value
FROM
(SELECT x.key, count(1) AS cnt FROM src1 x JOIN src y ON (x.key = y.key) group by x.key) b
JOIN
(SELECT x.key, x.value FROM src1 x JOIN src y ON (x.key = y.key)) d
ON b.key = d.key;

INSERT OVERWRITE TABLE dest_co1
SELECT b.key, b.cnt, d.value
FROM
(SELECT x.key, count(1) AS cnt FROM src1 x JOIN src y ON (x.key = y.key) group by x.key) b
JOIN
(SELECT x.key, x.value FROM src1 x JOIN src y ON (x.key = y.key)) d
ON b.key = d.key;

set hive.optimize.correlation=true;
EXPLAIN
INSERT OVERWRITE TABLE dest_co2
SELECT b.key, b.cnt, d.value
FROM
(SELECT x.key, count(1) AS cnt FROM src1 x JOIN src y ON (x.key = y.key) group by x.key) b
JOIN
(SELECT x.key, x.value FROM src1 x JOIN src y ON (x.key = y.key)) d
ON b.key = d.key;

INSERT OVERWRITE TABLE dest_co2
SELECT b.key, b.cnt, d.value
FROM
(SELECT x.key, count(1) AS cnt FROM src1 x JOIN src y ON (x.key = y.key) group by x.key) b
JOIN
(SELECT x.key, x.value FROM src1 x JOIN src y ON (x.key = y.key)) d
ON b.key = d.key;

-- dest_co1 and dest_co2 should be same
SELECT * FROM dest_co1 x ORDER BY x.key, x.cnt, x.value;
SELECT * FROM dest_co2 x ORDER BY x.key, x.cnt, x.value;