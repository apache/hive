CREATE EXTERNAL TABLE t1(
   a int,
   b int,
   c int,
   d int);

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
