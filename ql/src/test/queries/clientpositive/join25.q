-- SORT_QUERY_RESULTS

CREATE TABLE dest_j1(key INT, value STRING, val2 STRING) STORED AS TEXTFILE;

EXPLAIN
INSERT OVERWRITE TABLE dest_j1 
SELECT /*+ MAPJOIN(x) */ x.key, x.value, y.value
FROM src1 x JOIN src y ON (x.key = y.key);

INSERT OVERWRITE TABLE dest_j1 
SELECT /*+ MAPJOIN(x) */ x.key, x.value, y.value
FROM src1 x JOIN src y ON (x.key = y.key);

select * from dest_j1 x;



