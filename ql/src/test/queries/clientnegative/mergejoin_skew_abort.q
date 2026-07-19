set hive.explain.user=false;
set hive.auto.convert.join=false;
-- merge join observability config, with true should throw exception after skew
-- join detected beyond the threshold
set hive.merge.join.skew.threshold=2;
set hive.merge.join.skew.abort=true;
-- interval=1 means check every row (most aggressive, catches skew at first boundary crossing)
set hive.merge.join.skew.check.interval=1;

CREATE TABLE merge_skew_abort_a (testColKey_A int, testColValue_A string);
CREATE TABLE merge_skew_abort_b (testColKey_B int, testColValue_B string);

INSERT INTO TABLE merge_skew_abort_a VALUES (1, 'a1'), (1, 'a2'), (1, 'a3'), (1, 'a4'),(2, 'b1');
INSERT INTO TABLE merge_skew_abort_b VALUES (1, 'x1'), (2, 'y1');

SELECT a.testColKey_A, a.testColValue_A, b.testColValue_B
FROM merge_skew_abort_a a JOIN merge_skew_abort_b b ON a.testColKey_A = b.testColKey_B;

