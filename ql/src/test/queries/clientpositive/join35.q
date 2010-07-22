

CREATE TABLE dest_j1(key STRING, value STRING, val2 INT) STORED AS TEXTFILE;

EXPLAIN EXTENDED
INSERT OVERWRITE TABLE dest_j1
SELECT /*+ MAPJOIN(x) */ x.key, x.value, subq1.cnt
FROM 
( SELECT x.key as key, count(1) as cnt from src x where x.key < 20 group by x.key
     UNION ALL
  SELECT x1.key as key, count(1) as cnt from src x1 where x1.key > 100 group by x1.key
) subq1
JOIN src1 x ON (x.key = subq1.key);

INSERT OVERWRITE TABLE dest_j1
SELECT /*+ MAPJOIN(x) */ x.key, x.value, subq1.cnt
FROM 
( SELECT x.key as key, count(1) as cnt from src x where x.key < 20 group by x.key
     UNION ALL
  SELECT x1.key as key, count(1) as cnt from src x1 where x1.key > 100 group by x1.key
) subq1
JOIN src1 x ON (x.key = subq1.key);

select * from dest_j1 x order by x.key;



