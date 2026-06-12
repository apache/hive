-- compound key join with skewed data, merge join should abort when skew threshold is exceeded
set hive.explain.user=false;
set hive.auto.convert.join=false;
set hive.merge.join.skew.threshold=2;
set hive.merge.join.skew.abort=true;
set hive.merge.join.skew.check.interval=1;

CREATE TABLE merge_skew_compound_a (k1 int, k2 string, val string);
CREATE TABLE merge_skew_compound_b (k1 int, k2 string, val string);

INSERT INTO TABLE merge_skew_compound_a VALUES
  (1, 'x', 'a1'), (1, 'x', 'a2'), (1, 'x', 'a3'), (1, 'x', 'a4'),
  (2, 'y', 'b1');
INSERT INTO TABLE merge_skew_compound_b VALUES
  (1, 'x', 'r1'), (2, 'y', 'r2');

SELECT a.k1, a.k2, a.val, b.val
FROM merge_skew_compound_a a JOIN merge_skew_compound_b b
  ON a.k1 = b.k1 AND a.k2 = b.k2;

