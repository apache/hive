--! qt:dataset:src
set hive.cbo.fallback.strategy=NEVER;

FROM UNIQUEJOIN src a (a.key), PRESERVE src b (b.key, b.val)
SELECT a.key, b.key;

