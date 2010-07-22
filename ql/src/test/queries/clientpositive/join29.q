CREATE TABLE dest_j1(key STRING, cnt1 INT, cnt2 INT);

EXPLAIN 
INSERT OVERWRITE TABLE dest_j1 
SELECT /*+ MAPJOIN(subq1) */ subq1.key, subq1.cnt, subq2.cnt
FROM (select x.key, count(1) as cnt from src1 x group by x.key) subq1 JOIN 
     (select y.key, count(1) as cnt from src y group by y.key) subq2 ON (subq1.key = subq2.key);

INSERT OVERWRITE TABLE dest_j1 
SELECT /*+ MAPJOIN(subq1) */ subq1.key, subq1.cnt, subq2.cnt
FROM (select x.key, count(1) as cnt from src1 x group by x.key) subq1 JOIN 
     (select y.key, count(1) as cnt from src y group by y.key) subq2 ON (subq1.key = subq2.key);

select * from dest_j1 x order by x.key;
