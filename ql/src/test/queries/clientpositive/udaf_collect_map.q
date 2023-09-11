CREATE TABLE test (key int, map_key string, map_value string, map_value_number bigint);
INSERT INTO test VALUES
  (1, 'k-1', 'v-1', 1), (1, 'k-2', 'v-2', 2),
  (2, 'k-1', 'v-1', 3), (2, null, 'v-null', 4), (2, 'k-null', null, 5),
  (3, null, 'v-null', 6),
  (4, 'k-dup', 'v-dup-1', 7), (4, 'k-1', 'v-1', 8), (4, 'k-dup', 'v-dup-2', 9);

DESCRIBE FUNCTION collect_map;
DESCRIBE FUNCTION EXTENDED collect_map;

set hive.map.aggr = false;
set hive.groupby.skewindata = false;

SELECT key, collect_map(map_key, map_value)
FROM test
GROUP BY key
ORDER BY key;

SELECT key, collect_map(map_key, map_value_number)
FROM test
GROUP BY key
ORDER BY key;

set hive.map.aggr = true;
set hive.groupby.skewindata = false;

SELECT key, collect_map(map_key, map_value)
FROM test
GROUP BY key
ORDER BY key;

SELECT key, collect_map(map_key, map_value_number)
FROM test
GROUP BY key
ORDER BY key;

set hive.map.aggr = false;
set hive.groupby.skewindata = true;

SELECT key, collect_map(map_key, map_value)
FROM test
GROUP BY key
ORDER BY key;

SELECT key, collect_map(map_key, map_value_number)
FROM test
GROUP BY key
ORDER BY key;

set hive.map.aggr = true;
set hive.groupby.skewindata = true;

SELECT key, collect_map(map_key, map_value)
FROM test
GROUP BY key
ORDER BY key;

SELECT key, collect_map(map_key, map_value_number)
FROM test
GROUP BY key
ORDER BY key;
