--! qt:dataset:src

set hive.fetch.task.conversion=none;

SELECT NOT (key <=> key) from src tablesample (1 rows);
