set hive.explain.user=false;
set hive.auto.convert.join=false;
set hive.merge.join.skew.threshold=2;
set hive.merge.join.skew.abort=false;
set hive.merge.join.skew.check.interval=1;

-- SORT_QUERY_RESULTS

CREATE TABLE merge_skew_warn_a (key int, value string);
CREATE TABLE merge_skew_warn_b (key int, value string);

INSERT INTO TABLE merge_skew_warn_a VALUES (1, 'a1'), (1, 'a2'), (1, 'a3'), (1, 'a4'),
(2, 'b1'), (3, 'c1');
INSERT INTO TABLE merge_skew_warn_b VALUES (1, 'x1'), (2, 'y1'), (3, 'z1');

EXPLAIN
SELECT a.key, a.value, b.value
FROM merge_skew_warn_a a JOIN merge_skew_warn_b b ON a.key = b.key;

SELECT a.key, a.value, b.value
FROM merge_skew_warn_a a JOIN merge_skew_warn_b b ON a.key = b.key;

SELECT count(*) FROM merge_skew_warn_a a JOIN merge_skew_warn_b b ON a.key = b.key;

-- no warning run
set hive.merge.join.skew.threshold=-1;

SELECT count(*) FROM merge_skew_warn_a a JOIN merge_skew_warn_b b ON a.key = b.key;

-- interval test: threshold=2, interval=3 -- skew key (key=1 has 4 rows) must still be detected
-- even though it may not be evaluated on every row
set hive.merge.join.skew.threshold=2;
set hive.merge.join.skew.check.interval=3;

SELECT count(*) FROM merge_skew_warn_a a JOIN merge_skew_warn_b b ON a.key = b.key;

-- unique-mapping test: 1-to-1 join between tables with unique keys should never trip threshold
-- even with a low threshold of 2. Each key appears only once, so no skew.
set hive.merge.join.skew.abort=true;

CREATE TABLE merge_skew_warn_unique_a (key int, value string);
CREATE TABLE merge_skew_warn_unique_b (key int, value string);

INSERT INTO TABLE merge_skew_warn_unique_a VALUES (1, 'u1'), (2, 'u2'), (3, 'u3');
INSERT INTO TABLE merge_skew_warn_unique_b VALUES (1, 'v1'), (2, 'v2'), (3, 'v3');

set hive.merge.join.skew.threshold=2;
set hive.merge.join.skew.check.interval=1;

-- must complete without abort: every key appears exactly once on both sides
SELECT count(*) FROM merge_skew_warn_unique_a a JOIN merge_skew_warn_unique_b b ON a.key = b.key;

DROP TABLE merge_skew_warn_unique_a;
DROP TABLE merge_skew_warn_unique_b;

DROP TABLE merge_skew_warn_a;
DROP TABLE merge_skew_warn_b;

-- three table join

set hive.explain.user=false;
set hive.auto.convert.join=false;
set hive.merge.join.skew.threshold=2;
set hive.merge.join.skew.abort=false;
set hive.merge.join.skew.check.interval=1;

CREATE TABLE merge_skew_warn_3w_a (key int, val string);
CREATE TABLE merge_skew_warn_3w_b (key int, val string);
CREATE TABLE merge_skew_warn_3w_c (key int, val string);

-- key=1 has 4 rows in table a -> triggers skew warning
INSERT INTO TABLE merge_skew_warn_3w_a VALUES (1, 'a1'), (1, 'a2'), (1, 'a3'), (1, 'a4'), (2, 'a5'), (3, 'a6');
INSERT INTO TABLE merge_skew_warn_3w_b VALUES (1, 'b1'), (2, 'b2'), (3, 'b3');
INSERT INTO TABLE merge_skew_warn_3w_c VALUES (1, 'c1'), (2, 'c2'), (3, 'c3');

EXPLAIN
SELECT a.key, a.val, b.val, c.val
FROM merge_skew_warn_3w_a a
  JOIN merge_skew_warn_3w_b b ON a.key = b.key
  JOIN merge_skew_warn_3w_c c ON b.key = c.key;

-- Should complete with a skew warning for key=1
SELECT a.key, a.val, b.val, c.val
FROM merge_skew_warn_3w_a a
  JOIN merge_skew_warn_3w_b b ON a.key = b.key
  JOIN merge_skew_warn_3w_c c ON b.key = c.key;

SELECT count(*)
FROM merge_skew_warn_3w_a a
  JOIN merge_skew_warn_3w_b b ON a.key = b.key
  JOIN merge_skew_warn_3w_c c ON b.key = c.key;

-- unique-keys run: no skew expected even with abort=true
set hive.merge.join.skew.abort=true;

CREATE TABLE merge_skew_warn_3w_uniq_a (key int, val string);
CREATE TABLE merge_skew_warn_3w_uniq_b (key int, val string);
CREATE TABLE merge_skew_warn_3w_uniq_c (key int, val string);

INSERT INTO TABLE merge_skew_warn_3w_uniq_a VALUES (1, 'u1'), (2, 'u2'), (3, 'u3');
INSERT INTO TABLE merge_skew_warn_3w_uniq_b VALUES (1, 'v1'), (2, 'v2'), (3, 'v3');
INSERT INTO TABLE merge_skew_warn_3w_uniq_c VALUES (1, 'w1'), (2, 'w2'), (3, 'w3');

-- must complete without abort: every key appears exactly once in all three tables
SELECT count(*)
FROM merge_skew_warn_3w_uniq_a a
  JOIN merge_skew_warn_3w_uniq_b b ON a.key = b.key
  JOIN merge_skew_warn_3w_uniq_c c ON b.key = c.key;

DROP TABLE merge_skew_warn_3w_uniq_a;
DROP TABLE merge_skew_warn_3w_uniq_b;
DROP TABLE merge_skew_warn_3w_uniq_c;

DROP TABLE merge_skew_warn_3w_a;
DROP TABLE merge_skew_warn_3w_b;
DROP TABLE merge_skew_warn_3w_c;

