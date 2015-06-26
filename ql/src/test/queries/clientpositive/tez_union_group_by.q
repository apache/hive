set hive.explain.user=false;
CREATE TABLE x
(
u bigint,
t string,
st string
)
PARTITIONED BY (`date` string)
STORED AS ORC 
TBLPROPERTIES ("orc.compress"="ZLIB");

CREATE TABLE y
(
u bigint
)
PARTITIONED BY (`date` string)
STORED AS ORC 
TBLPROPERTIES ("orc.compress"="ZLIB");

CREATE TABLE z
(
u bigint
)
PARTITIONED BY (`date` string)
STORED AS ORC 
TBLPROPERTIES ("orc.compress"="ZLIB");

CREATE TABLE v
(
t string, 
st string,
id int
)
STORED AS ORC 
TBLPROPERTIES ("orc.compress"="ZLIB");

EXPLAIN 
SELECT o.u, n.u
FROM 
(
SELECT m.u, Min(`date`) as ft
FROM 
(
SELECT u, `date` FROM x WHERE `date` < '2014-09-02' 
UNION ALL
SELECT u, `date` FROM y WHERE `date` < '2014-09-02' 
UNION ALL
SELECT u, `date` FROM z WHERE `date` < '2014-09-02' 
) m
GROUP BY m.u
) n 
LEFT OUTER JOIN
(
SELECT x.u
FROM x
JOIN v 
ON (x.t = v.t AND x.st <=> v.st)
WHERE x.`date` >= '2014-03-04' AND x.`date` < '2014-09-03'
GROUP BY x.u
) o
ON n.u = o.u 
WHERE n.u <> 0 AND n.ft <= '2014-09-02';

SELECT o.u, n.u
FROM 
(
SELECT m.u, Min(`date`) as ft
FROM 
(
SELECT u, `date` FROM x WHERE `date` < '2014-09-02' 
UNION ALL
SELECT u, `date` FROM y WHERE `date` < '2014-09-02' 
UNION ALL
SELECT u, `date` FROM z WHERE `date` < '2014-09-02' 
) m
GROUP BY m.u
) n 
LEFT OUTER JOIN
(
SELECT x.u
FROM x
JOIN v 
ON (x.t = v.t AND x.st <=> v.st)
WHERE x.`date` >= '2014-03-04' AND x.`date` < '2014-09-03'
GROUP BY x.u
) o
ON n.u = o.u 
WHERE n.u <> 0 AND n.ft <= '2014-09-02';
