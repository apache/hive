-- SORT_QUERY_RESULTS

CREATE TABLE dest_j1_n9(key INT, value STRING, val2 STRING) STORED AS TEXTFILE;

set hive.auto.convert.join=true;
EXPLAIN
INSERT OVERWRITE TABLE dest_j1_n9 
SELECT /*+ MAPJOIN(X) */ x.key, x.value, y.value
FROM src1 x JOIN src y ON (x.key = y.key);

INSERT OVERWRITE TABLE dest_j1_n9 
SELECT /*+ MAPJOIN(X) */ x.key, x.value, y.value
FROM src1 x JOIN src y ON (x.key = y.key);

select * from dest_j1_n9;



