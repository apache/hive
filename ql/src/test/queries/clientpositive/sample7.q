--! qt:dataset:srcbucket
CREATE TABLE dest1_n160(key INT, value STRING) STORED AS TEXTFILE;

-- both input pruning and sample filter
EXPLAIN EXTENDED
INSERT OVERWRITE TABLE dest1_n160 SELECT s.* 
FROM srcbucket TABLESAMPLE (BUCKET 1 OUT OF 4 on key) s
WHERE s.key > 100;

INSERT OVERWRITE TABLE dest1_n160 SELECT s.* 
FROM srcbucket TABLESAMPLE (BUCKET 1 OUT OF 4 on key) s
WHERE s.key > 100;

SELECT dest1_n160.* FROM dest1_n160
order by key, value;
