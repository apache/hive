--! qt:dataset:srcbucket2
--! qt:dataset:srcbucket
set hive.mapred.mode=nonstrict;
CREATE TABLE dest1_n27(key INT, value STRING) STORED AS TEXTFILE;

-- both input pruning and sample filter
EXPLAIN EXTENDED
INSERT OVERWRITE TABLE dest1_n27 SELECT s.*
FROM srcbucket TABLESAMPLE (BUCKET 1 OUT OF 4 on key) s;

INSERT OVERWRITE TABLE dest1_n27 SELECT s.*
FROM srcbucket TABLESAMPLE (BUCKET 1 OUT OF 4 on key) s;

SELECT dest1_n27.* FROM dest1_n27
order by key, value;

EXPLAIN EXTENDED SELECT s.* FROM srcbucket TABLESAMPLE (BUCKET 4 OUT OF 4 on key) s
ORDER BY key, value;
SELECT s.* FROM srcbucket TABLESAMPLE (BUCKET 4 OUT OF 4 on key) s
ORDER BY key, value;

EXPLAIN EXTENDED SELECT s.* FROM srcbucket TABLESAMPLE (BUCKET 1 OUT OF 2 on key) s
ORDER BY key, value;
SELECT s.* FROM srcbucket TABLESAMPLE (BUCKET 1 OUT OF 2 on key) s
ORDER BY key, value;

EXPLAIN EXTENDED SELECT s.* FROM srcbucket TABLESAMPLE (BUCKET 1 OUT OF 3 on key) s
ORDER BY key, value;
SELECT s.* FROM srcbucket TABLESAMPLE (BUCKET 1 OUT OF 3 on key) s
ORDER BY key, value;

EXPLAIN EXTENDED SELECT s.* FROM srcbucket TABLESAMPLE (BUCKET 2 OUT OF 3 on key) s
ORDER BY key, value;
SELECT s.* FROM srcbucket TABLESAMPLE (BUCKET 2 OUT OF 3 on key) s
ORDER BY key, value;

EXPLAIN EXTENDED SELECT s.* FROM srcbucket2 TABLESAMPLE (BUCKET 1 OUT OF 2 on key) s
ORDER BY key, value;
SELECT s.* FROM srcbucket2 TABLESAMPLE (BUCKET 1 OUT OF 2 on key) s
ORDER BY key, value;

EXPLAIN EXTENDED SELECT s.* FROM srcbucket2 TABLESAMPLE (BUCKET 2 OUT OF 4 on key) s
ORDER BY key, value;
SELECT s.* FROM srcbucket2 TABLESAMPLE (BUCKET 2 OUT OF 4 on key) s
ORDER BY key, value;

CREATE TABLE empty_bucket (key int, value string) CLUSTERED BY (key) INTO 2 BUCKETS STORED AS TEXTFILE;
EXPLAIN EXTENDED SELECT s.* FROM empty_bucket TABLESAMPLE (BUCKET 1 OUT OF 2 on key) s
ORDER BY key, value;
SELECT s.* FROM empty_bucket TABLESAMPLE (BUCKET 1 OUT OF 2 on key) s
ORDER BY key, value;



