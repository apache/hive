SET hive.vectorized.execution.enabled=false;
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.cbo.enable=false;
set hive.auto.convert.join=false;
set hive.optimize.ppd=false;
-- merge join observability config, with true should throw exception after skew
-- join detected beyond the threshold
set hive.merge.join.skew.threshold=2;
set hive.merge.join.skew.abort=true;

CREATE TABLE merge_skew_abort_a (key int, value string) STORED AS TEXTFILE;
CREATE TABLE merge_skew_abort_b (key int, value string) STORED AS TEXTFILE;

INSERT INTO TABLE merge_skew_abort_a VALUES (1, 'a1'), (1, 'a2'), (1, 'a3'), (1, 'a4'),(2, 'b1');
INSERT INTO TABLE merge_skew_abort_b VALUES (1, 'x1'), (2, 'y1');

SELECT a.key, a.value, b.value
FROM merge_skew_abort_a a JOIN merge_skew_abort_b b ON a.key = b.key;

