--! qt:dataset:src
SELECT min(map("key", key, "value", value))
FROM src;
