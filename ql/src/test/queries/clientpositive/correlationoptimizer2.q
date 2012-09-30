-- the query is modified from join18.q

CREATE TABLE dest_co1(key1 INT, cnt1 INT, key2 INT, cnt2 INT);
CREATE TABLE dest_co2(key1 INT, cnt1 INT, key2 INT, cnt2 INT);

set hive.optimize.correlation=false;
EXPLAIN
INSERT OVERWRITE TABLE dest_co1
SELECT a.key, a.cnt, b.key, b.cnt
FROM
(SELECT x.key as key, count(x.value) AS cnt FROM src x group by x.key) a
JOIN
(SELECT y.key as key, count(y.value) AS cnt FROM src1 y group by y.key) b
ON (a.key = b.key);

INSERT OVERWRITE TABLE dest_co1
SELECT a.key, a.cnt, b.key, b.cnt
FROM
(SELECT x.key as key, count(x.value) AS cnt FROM src x group by x.key) a
JOIN
(SELECT y.key as key, count(y.value) AS cnt FROM src1 y group by y.key) b
ON (a.key = b.key);

set hive.optimize.correlation=true;
EXPLAIN
INSERT OVERWRITE TABLE dest_co2
SELECT a.key, a.cnt, b.key, b.cnt
FROM
(SELECT x.key as key, count(x.value) AS cnt FROM src x group by x.key) a
JOIN
(SELECT y.key as key, count(y.value) AS cnt FROM src1 y group by y.key) b
ON (a.key = b.key);

INSERT OVERWRITE TABLE dest_co2
SELECT a.key, a.cnt, b.key, b.cnt
FROM
(SELECT x.key as key, count(x.value) AS cnt FROM src x group by x.key) a
JOIN
(SELECT y.key as key, count(y.value) AS cnt FROM src1 y group by y.key) b
ON (a.key = b.key);

-- dest_co1 and dest_co2 should be same
SELECT * FROM dest_co1 x ORDER BY x.key1, x.key2, x.cnt1, x.cnt2;
SELECT * FROM dest_co2 x ORDER BY x.key1, x.key2, x.cnt1, x.cnt2;