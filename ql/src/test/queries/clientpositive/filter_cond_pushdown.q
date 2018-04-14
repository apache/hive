--! qt:dataset:src
--! qt:dataset:cbo_t3
--! qt:dataset:cbo_t2
--! qt:dataset:cbo_t1
set hive.mapred.mode=nonstrict;
EXPLAIN
SELECT f.key, g.value
FROM src f JOIN src m JOIN src g ON(g.value = m.value AND m.value is not null AND m.value !='')
WHERE (f.key = m.key AND f.value='2008-04-08' AND m.value='2008-04-08') OR (f.key = m.key AND f.value='2008-04-09');

EXPLAIN
SELECT f.key, g.value
FROM src f JOIN src m JOIN src g ON(g.value = m.value AND m.value is not null AND m.value !='')
WHERE (f.key = m.key AND f.value IN ('2008-04-08','2008-04-10') AND m.value='2008-04-08') OR (f.key = m.key AND f.value='2008-04-09');

EXPLAIN
SELECT t1.key 
FROM cbo_t1 t1
JOIN (
  SELECT t2.key
  FROM cbo_t2 t2
  JOIN (SELECT * FROM cbo_t3 t3 WHERE c_int=1) t3 ON t2.key=t3.c_int
  WHERE ((t2.key=t3.key) AND (t2.c_float + t3.c_float > 2)) OR
      ((t2.key=t3.key) AND (t2.c_int + t3.c_int > 2))) t4 ON t1.key=t4.key;

EXPLAIN
SELECT f.key, f.value, m.value
FROM src f JOIN src m ON(f.key = m.key AND m.value is not null AND m.value !='')
WHERE (f.value IN ('2008-04-08','2008-04-10') AND f.value IN ('2008-04-08','2008-04-09') AND m.value='2008-04-10') OR (m.value='2008-04-08');
