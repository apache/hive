set hive.auto.convert.join=false;
set hive.merge.join.skew.threshold=2;
set hive.merge.join.skew.abort=true;
set hive.merge.join.skew.check.interval=1;

CREATE TABLE merge_skew_abort_3w_a (key int, val string);
CREATE TABLE merge_skew_abort_3w_b (key int, val string);
CREATE TABLE merge_skew_abort_3w_c (key int, val string);

-- key=10 has 4 rows in table a -> abort threshold exceeded
INSERT INTO TABLE merge_skew_abort_3w_a VALUES (10, 's1'), (20, 's5');
INSERT INTO TABLE merge_skew_abort_3w_b VALUES (10, 't1'), (20, 't2');
INSERT INTO TABLE merge_skew_abort_3w_c VALUES (10, 'u1'), (20, 'u2'), (10, 's2'), (10, 's3'), (10, 's4');

SELECT count(*)
FROM merge_skew_abort_3w_a a
  JOIN merge_skew_abort_3w_b b ON a.key = b.key
  JOIN merge_skew_abort_3w_c c ON b.key = c.key;

DROP TABLE merge_skew_abort_3w_a;
DROP TABLE merge_skew_abort_3w_b;
DROP TABLE merge_skew_abort_3w_c;

