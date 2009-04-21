set hive.partition.pruning=strict;

EXPLAIN
SELECT src.key, src.value from src order by src.key;

SELECT src.key, src.value from src order by src.key;

