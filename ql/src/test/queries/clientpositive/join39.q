-- SORT_QUERY_RESULTS

CREATE TABLE dest_j1(key STRING, value STRING, key1 string, val2 STRING) STORED AS TEXTFILE;
set hive.auto.convert.join=true;

explain
INSERT OVERWRITE TABLE dest_j1
SELECT /*+ MAPJOIN(y) */ x.key, x.value, y.key, y.value
FROM src x left outer JOIN (select * from src where key <= 100) y ON (x.key = y.key);


INSERT OVERWRITE TABLE dest_j1
SELECT /*+ MAPJOIN(y) */ x.key, x.value, y.key, y.value
FROM src x left outer JOIN (select * from src where key <= 100) y ON (x.key = y.key);

select * from dest_j1;



