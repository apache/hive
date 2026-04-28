SET hive.vectorized.execution.enabled=false;
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.cbo.enable=false;
set hive.auto.convert.join=false;
set hive.optimize.ppd=false;
set hive.merge.join.skew.threshold=2;
set hive.merge.join.skew.abort=false;

-- SORT_QUERY_RESULTS

CREATE TABLE merge_skew_warn_a (key int, value string) STORED AS TEXTFILE;
CREATE TABLE merge_skew_warn_b (key int, value string) STORED AS TEXTFILE;

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

DROP TABLE merge_skew_warn_a;
DROP TABLE merge_skew_warn_b;

