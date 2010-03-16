EXPLAIN
SELECT /*+ MAPJOIN(z) */ subq.key1, z.value
FROM
(SELECT /*+ MAPJOIN(x) */ x.key as key1, x.value as value1, y.key as key2, y.value as value2 
 FROM src1 x JOIN src y ON (x.key = y.key)) subq
 JOIN srcpart z ON (subq.key1 = z.key and z.ds='2008-04-08' and z.hr=11);
 
SELECT /*+ MAPJOIN(z) */ subq.key1, z.value
FROM
(SELECT /*+ MAPJOIN(x) */ x.key as key1, x.value as value1, y.key as key2, y.value as value2 
 FROM src1 x JOIN src y ON (x.key = y.key)) subq
 JOIN srcpart z ON (subq.key1 = z.key and z.ds='2008-04-08' and z.hr=11);
 
EXPLAIN 
SELECT /*+ MAPJOIN(z) */ subq.key1, z.value
FROM
(SELECT /*+ MAPJOIN(x) */ x.key as key1, x.value as value1, y.key as key2, y.value as value2 
 FROM src1 x JOIN src y ON (x.key = y.key)) subq
 JOIN srcpart z ON (subq.key1 = z.key and z.ds='2008-04-08' and z.hr=11) 
 order by subq.key1;


SELECT /*+ MAPJOIN(z) */ subq.key1, z.value
FROM
(SELECT /*+ MAPJOIN(x) */ x.key as key1, x.value as value1, y.key as key2, y.value as value2  
 FROM src1 x JOIN src y ON (x.key = y.key)) subq 
 JOIN srcpart z ON (subq.key1 = z.key and z.ds='2008-04-08' and z.hr=11)  
 order by subq.key1;
