--! qt:disabled:disabled by 38f7a7f3839e in 2018
--! qt:dataset:srcbucket
CREATE TABLE dest1_n29(key INT, value STRING) STORED AS TEXTFILE;

-- input pruning, no sample filter
-- default table sample columns
EXPLAIN EXTENDED
INSERT OVERWRITE TABLE dest1_n29 SELECT s.* 
FROM srcbucket TABLESAMPLE (BUCKET 1 OUT OF 2) s;

INSERT OVERWRITE TABLE dest1_n29 SELECT s.* 
FROM srcbucket TABLESAMPLE (BUCKET 1 OUT OF 2) s;

SELECT dest1_n29.* FROM dest1_n29
order by key, value;
