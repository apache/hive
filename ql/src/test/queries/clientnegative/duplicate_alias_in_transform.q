--! qt:dataset:src
FROM src SELECT TRANSFORM (key, value) USING "awk -F'\001' '{print $0}'" AS (foo, foo);