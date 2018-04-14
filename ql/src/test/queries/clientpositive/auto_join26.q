--! qt:dataset:src1
--! qt:dataset:src
set hive.mapred.mode=nonstrict;
-- SORT_QUERY_RESULTS

CREATE TABLE dest_j1(key INT, cnt INT);
set hive.auto.convert.join = true;
EXPLAIN
INSERT OVERWRITE TABLE dest_j1 
SELECT x.key, count(1) FROM src1 x JOIN src y ON (x.key = y.key) group by x.key;

INSERT OVERWRITE TABLE dest_j1 
SELECT  x.key, count(1) FROM src1 x JOIN src y ON (x.key = y.key) group by x.key;

select * from dest_j1;
