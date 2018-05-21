--! qt:dataset:src1
--! qt:dataset:src
-- SORT_QUERY_RESULTS

CREATE TABLE dest_j1_n2(key INT, value STRING, val2 STRING) STORED AS TEXTFILE;
set hive.auto.convert.join=true;
EXPLAIN
INSERT OVERWRITE TABLE dest_j1_n2 
SELECT /*+ MAPJOIN(x) */ x.key, x.value, y.value
FROM src1 x JOIN src y ON (x.value = y.value);

INSERT OVERWRITE TABLE dest_j1_n2 
SELECT /*+ MAPJOIN(x) */ x.key, x.value, y.value
FROM src1 x JOIN src y ON (x.value = y.value);

select * from dest_j1_n2;
