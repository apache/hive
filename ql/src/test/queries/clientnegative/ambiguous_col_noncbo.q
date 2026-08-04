--! qt:dataset:src
set hive.cbo.enable=false;
FROM (SELECT key, concat(value) AS key FROM src) a SELECT a.key;
