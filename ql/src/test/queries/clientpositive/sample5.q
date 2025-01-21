--! qt:dataset:srcbucket
CREATE TABLE dest1_n69(key INT, value STRING) STORED AS TEXTFILE;

-- SORT_QUERY_RESULTS

-- no input pruning, sample filter
EXPLAIN EXTENDED
INSERT OVERWRITE TABLE dest1_n69 SELECT s.* 
-- here's another test
FROM srcbucket TABLESAMPLE (BUCKET 1 OUT OF 5 on key) s;

INSERT OVERWRITE TABLE dest1_n69 SELECT s.* 
-- here's another test
FROM srcbucket TABLESAMPLE (BUCKET 1 OUT OF 5 on key) s;

SELECT dest1_n69.* FROM dest1_n69 SORT BY key, value;
