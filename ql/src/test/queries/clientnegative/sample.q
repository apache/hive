--! qt:dataset:srcbucket
set hive.cbo.fallback.strategy=NEVER;
explain extended SELECT s.* FROM srcbucket TABLESAMPLE (BUCKET 5 OUT OF 4 on key) s