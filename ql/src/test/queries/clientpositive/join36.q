--! qt:dataset:src
-- SORT_QUERY_RESULTS

CREATE TABLE tmp1_n0(key INT, cnt INT);
CREATE TABLE tmp2_n0(key INT, cnt INT);
CREATE TABLE dest_j1_n13(key INT, value INT, val2 INT);

INSERT OVERWRITE TABLE tmp1_n0
SELECT key, count(1) from src group by key;

INSERT OVERWRITE TABLE tmp2_n0
SELECT key, count(1) from src group by key;
set hive.auto.convert.join=true;
EXPLAIN
INSERT OVERWRITE TABLE dest_j1_n13 
SELECT /*+ MAPJOIN(x) */ x.key, x.cnt, y.cnt
FROM tmp1_n0 x JOIN tmp2_n0 y ON (x.key = y.key);

INSERT OVERWRITE TABLE dest_j1_n13 
SELECT /*+ MAPJOIN(x) */ x.key, x.cnt, y.cnt
FROM tmp1_n0 x JOIN tmp2_n0 y ON (x.key = y.key);

select * from dest_j1_n13;



