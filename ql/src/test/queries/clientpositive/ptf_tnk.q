set hive.vectorized.execution.enabled=false;

CREATE EXTERNAL TABLE t1(
   a int,
   b int,
   c int,
   d int);

insert into t1(a, b, c, d) values
(1,2,3,4),
(2,3,4,5),
(1,2,3,4),
(7,8,9,10),
(7,7,2,4),
(1,2,3,4),
(2,3,4,5),
(1,2,3,4),
(7,8,9,10),
(7,7,2,4),
(1,2,3,4);

explain
SELECT * FROM (
  SELECT a,
         b,
         c,
         d,
         DENSE_RANK() OVER (PARTITION BY a,c ORDER BY d DESC) AS drank
  FROM (
     select distinct(a),
            b,
            c,
            d
     FROM t1
     ) subA
 ) subB
 WHERE subB.drank <= 13 limit 1;

SELECT * FROM (
  SELECT a,
         b,
         c,
         d,
         DENSE_RANK() OVER (PARTITION BY a,c ORDER BY d DESC) AS drank
  FROM (
     select distinct(a),
            b,
            c,
            d
     FROM t1
     ) subA
 ) subB
 WHERE subB.drank <= 13 limit 1;
