CREATE TABLE dest_j1(key STRING, cnt INT);

EXPLAIN 
INSERT OVERWRITE TABLE dest_j1 
SELECT /*+ MAPJOIN(subq1) */ subq1.key, count(1) as cnt
FROM (select x.key, count(1) as cnt from src1 x group by x.key) subq1 JOIN 
     (select y.key, count(1) as cnt from src y group by y.key) subq2 ON (subq1.key = subq2.key)
group by subq1.key;

INSERT OVERWRITE TABLE dest_j1 
SELECT /*+ MAPJOIN(subq1) */ subq1.key, count(1) as cnt
FROM (select x.key, count(1) as cnt from src1 x group by x.key) subq1 JOIN 
     (select y.key, count(1) as cnt from src y group by y.key) subq2 ON (subq1.key = subq2.key)
group by subq1.key;

select * from dest_j1 x order by x.key;
