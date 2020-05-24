--! qt:dataset:cbo_t3
--! qt:dataset:cbo_t2
--! qt:dataset:cbo_t1
set hive.mapred.mode=nonstrict;
set hive.cbo.enable=true;
set hive.exec.check.crossproducts=false;

set hive.stats.fetch.column.stats=true;
set hive.auto.convert.join=false;

-- 7. Test Select + TS + Join + Fil + GB + GB Having + Limit
  SELECT key, (c_int+1)+2 AS x, sum(c_int)
    FROM cbo_t1
GROUP BY c_float, cbo_t1.c_int, key
ORDER BY x, key
   LIMIT 1;

  SELECT x, y, count(*)
    FROM (SELECT key, (c_int+c_float+1+2) AS x, sum(c_int) AS y
            FROM cbo_t1
        GROUP BY c_float, cbo_t1.c_int, key
         ) R
GROUP BY y, x
ORDER BY x, y
   LIMIT 1;

SELECT key
  FROM (SELECT key
          FROM (SELECT key
                  FROM cbo_t1
              ORDER BY key
                 LIMIT 5
               ) cbo_t2
         LIMIT 5
       ) cbo_t3
 LIMIT 5;

  SELECT key, c_int
    FROM (SELECT key, c_int
            FROM (SELECT key, c_int
                    FROM cbo_t1
                ORDER BY c_int, key
                   LIMIT 5
                 ) cbo_t1
        ORDER BY c_int
           LIMIT 5
         ) cbo_t2
ORDER BY c_int
   LIMIT 5;

  SELECT cbo_t3.c_int, c, count(*)
    FROM (SELECT key AS a, c_int+1 AS b, sum(c_int) AS c
            FROM cbo_t1
           WHERE (cbo_t1.c_int + 1 >= 0)
             AND (cbo_t1.c_int > 0 OR cbo_t1.c_float >= 0)
        GROUP BY c_float, cbo_t1.c_int, key
        ORDER BY a, b
           LIMIT 5
         ) cbo_t1
    JOIN (SELECT key AS p, c_int+1 AS q, sum(c_int) AS r
            FROM cbo_t2
           WHERE (cbo_t2.c_int + 1 >= 0)
             AND (cbo_t2.c_int > 0 OR cbo_t2.c_float >= 0)
        GROUP BY c_float, cbo_t2.c_int, key
        ORDER BY q/10 DESC, r ASC, p ASC
           LIMIT 5
         ) cbo_t2 ON cbo_t1.a = p
    JOIN cbo_t3 ON cbo_t1.a = key
   WHERE (b + cbo_t2.q >= 0)
     AND (b > 0 OR c_int >= 0)
GROUP BY cbo_t3.c_int, c
ORDER BY cbo_t3.c_int + c DESC, c ASC
   LIMIT 5;

   SELECT cbo_t3.c_int, c, count(*)
     FROM (SELECT key AS a, c_int+1 AS b, sum(c_int) AS c
             FROM cbo_t1
            WHERE (cbo_t1.c_int + 1 >= 0)
              AND (cbo_t1.c_int > 0 OR cbo_t1.c_float >= 0)
         GROUP BY c_float, cbo_t1.c_int, key
           HAVING cbo_t1.c_float > 0
              AND (c_int >=1 OR c_float >= 1)
              AND (c_int + c_float) >= 0
         ORDER BY b % c ASC, b DESC, a ASC
            LIMIT 5
          ) cbo_t1
LEFT JOIN (SELECT key AS p, c_int+1 AS q, sum(c_int) AS r
             FROM cbo_t2
            WHERE (cbo_t2.c_int + 1 >= 0)
              AND (cbo_t2.c_int > 0 OR cbo_t2.c_float >= 0)
         GROUP BY c_float, cbo_t2.c_int, key
           HAVING cbo_t2.c_float > 0
              AND (c_int >=1 OR c_float >= 1)
              AND (c_int + c_float) >= 0
         ORDER BY p, q
            LIMIT 5
          ) cbo_t2 ON cbo_t1.a = p
LEFT JOIN cbo_t3 ON cbo_t1.a = key
    WHERE (b + cbo_t2.q >= 0)
      AND (b > 0 OR c_int >= 0)
 GROUP BY cbo_t3.c_int, c
   HAVING cbo_t3.c_int > 0
      AND (c_int >=1 OR c >= 1)
      AND (c_int + c) >= 0
 ORDER BY cbo_t3.c_int % c ASC, cbo_t3.c_int, c DESC
    LIMIT 5;

-- order by and limit
explain cbo select count(*) cs from cbo_t1 where c_int > 1 order by cs limit 100;
select count(*) cs from cbo_t1 where c_int > 1 order by cs limit 100;

-- only order by
explain cbo select count(*) cs from cbo_t1 where c_int > 1 order by cs ;
select count(*) cs from cbo_t1 where c_int > 1 order by cs ;

-- only LIMIT
explain cbo select count(*) cs from cbo_t1 where c_int > 1 LIMIT 100;
select count(*) cs from cbo_t1 where c_int > 1 LIMIT 100;

-- LIMIT 1
explain cbo select c_int from (select c_int from cbo_t1 where c_float > 1.0 limit 1) subq  where c_int > 1 order by c_int;
select c_int from (select c_int from cbo_t1 where c_float > 1.0 limit 1) subq  where c_int > 1 order by c_int;

explain cbo select count(*) from cbo_t1 where c_float > 1.0 group by true limit 0;
select count(*) from cbo_t1 where c_float > 1.0 group by true limit 0;

-- prune un-necessary aggregates
explain cbo select count(*) from cbo_t1 order by sum(c_int), count(*);
select count(*) from cbo_t1 order by sum(c_int), count(*);
