--! qt:disabled:disabled by 38f7a7f3839e in 2018
--! qt:dataset:srcbucket
CREATE TABLE dest1_n118(key INT, value STRING) STORED AS TEXTFILE;

-- bucket column is the same as table sample
-- No need for sample filter
EXPLAIN EXTENDED
INSERT OVERWRITE TABLE dest1_n118 SELECT s.*
FROM srcbucket TABLESAMPLE (BUCKET 1 OUT OF 2 on key) s;

INSERT OVERWRITE TABLE dest1_n118 SELECT s.*
FROM srcbucket TABLESAMPLE (BUCKET 1 OUT OF 2 on key) s;

SELECT dest1_n118.* FROM dest1_n118
order by key, value;
