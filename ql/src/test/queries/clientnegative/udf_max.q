--! qt:dataset:src
SELECT max(map("key", key, "value", value))
FROM src;
