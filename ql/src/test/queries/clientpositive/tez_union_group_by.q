set hive.explain.user=false;
CREATE TABLE x_n3
(
u bigint,
t string,
st string
)
PARTITIONED BY (`date` string)
STORED AS ORC 
TBLPROPERTIES ("orc.compress"="ZLIB");

CREATE TABLE y_n1
(
u bigint
)
PARTITIONED BY (`date` string)
STORED AS ORC 
TBLPROPERTIES ("orc.compress"="ZLIB");

CREATE TABLE z_n0
(
u bigint
)
PARTITIONED BY (`date` string)
STORED AS ORC 
TBLPROPERTIES ("orc.compress"="ZLIB");

CREATE TABLE v_n15
(
t string, 
st string,
id int
)
STORED AS ORC 
TBLPROPERTIES ("orc.compress"="ZLIB");

EXPLAIN CBO
SELECT o.u, n.u
FROM 
(
SELECT m.u, Min(`date`) as ft
FROM 
(
SELECT u, `date` FROM x_n3 WHERE `date` < '2014-09-02' 
UNION ALL
SELECT u, `date` FROM y_n1 WHERE `date` < '2014-09-02' 
UNION ALL
SELECT u, `date` FROM z_n0 WHERE `date` < '2014-09-02' 
) m
GROUP BY m.u
) n 
LEFT OUTER JOIN
(
SELECT x_n3.u
FROM x_n3
JOIN v_n15 
ON (x_n3.t = v_n15.t AND x_n3.st <=> v_n15.st)
WHERE x_n3.`date` >= '2014-03-04' AND x_n3.`date` < '2014-09-03'
GROUP BY x_n3.u
) o
ON n.u = o.u 
WHERE n.u <> 0 AND n.ft <= '2014-09-02';

EXPLAIN
SELECT o.u, n.u
FROM
(
SELECT m.u, Min(`date`) as ft
FROM
(
SELECT u, `date` FROM x_n3 WHERE `date` < '2014-09-02'
UNION ALL
SELECT u, `date` FROM y_n1 WHERE `date` < '2014-09-02'
UNION ALL
SELECT u, `date` FROM z_n0 WHERE `date` < '2014-09-02'
) m
GROUP BY m.u
) n
LEFT OUTER JOIN
(
SELECT x_n3.u
FROM x_n3
JOIN v_n15
ON (x_n3.t = v_n15.t AND x_n3.st <=> v_n15.st)
WHERE x_n3.`date` >= '2014-03-04' AND x_n3.`date` < '2014-09-03'
GROUP BY x_n3.u
) o
ON n.u = o.u
WHERE n.u <> 0 AND n.ft <= '2014-09-02';

SELECT o.u, n.u
FROM 
(
SELECT m.u, Min(`date`) as ft
FROM 
(
SELECT u, `date` FROM x_n3 WHERE `date` < '2014-09-02' 
UNION ALL
SELECT u, `date` FROM y_n1 WHERE `date` < '2014-09-02' 
UNION ALL
SELECT u, `date` FROM z_n0 WHERE `date` < '2014-09-02' 
) m
GROUP BY m.u
) n 
LEFT OUTER JOIN
(
SELECT x_n3.u
FROM x_n3
JOIN v_n15 
ON (x_n3.t = v_n15.t AND x_n3.st <=> v_n15.st)
WHERE x_n3.`date` >= '2014-03-04' AND x_n3.`date` < '2014-09-03'
GROUP BY x_n3.u
) o
ON n.u = o.u 
WHERE n.u <> 0 AND n.ft <= '2014-09-02';
