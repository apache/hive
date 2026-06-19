set hive.vectorized.execution.reduce.enabled=true;

CREATE EXTERNAL TABLE  orderdatatest_ext (col1 int, col2 int);
INSERT INTO orderdatatest_ext VALUES
  (1,1),
  (1,2),
  (1,3),
  (1,4),
  (1,5),
  (1,6),
  (1,7),
  (1,8),
  (1,9),
  (1,10);

select * from orderdatatest_ext order by col1 limit 5,100
