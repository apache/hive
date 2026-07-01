-- 2 joins in single query: first join has unique keys (no skew), second join has skew
set hive.merge.join.skew.threshold=2;
set hive.merge.join.skew.abort=true;
set hive.merge.join.skew.check.interval=1;

CREATE TABLE merge_skew_warn_2j_unique_a (key int, val string);
CREATE TABLE merge_skew_warn_2j_unique_b (key int, val string);
CREATE TABLE merge_skew_warn_2j_skew_a  (key int, val string);
CREATE TABLE merge_skew_warn_2j_skew_b  (key int, val string);

-- unique side: 1-to-1
INSERT INTO TABLE merge_skew_warn_2j_unique_a VALUES (10, 'u1'), (20, 'u2'), (30, 'u3');
INSERT INTO TABLE merge_skew_warn_2j_unique_b VALUES (10, 'v1'), (20, 'v2'), (30, 'v3');
-- skewed side: key=10 has 4 rows in skew_a -> abort the task
INSERT INTO TABLE merge_skew_warn_2j_skew_a  VALUES (10, 's1'), (10, 's2'), (10, 's3'), (10, 's4'), (20, 's5');
INSERT INTO TABLE merge_skew_warn_2j_skew_b  VALUES (10, 't1'), (20, 't2');

SELECT count(*)
FROM merge_skew_warn_2j_unique_a ua JOIN merge_skew_warn_2j_unique_b ub ON ua.key = ub.key
UNION ALL
SELECT count(*)
FROM merge_skew_warn_2j_skew_a  sa JOIN merge_skew_warn_2j_skew_b  sb ON sa.key = sb.key;

DROP TABLE merge_skew_warn_2j_unique_a;
DROP TABLE merge_skew_warn_2j_unique_b;
DROP TABLE merge_skew_warn_2j_skew_a;
DROP TABLE merge_skew_warn_2j_skew_b;
