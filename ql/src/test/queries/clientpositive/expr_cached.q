--! qt:dataset:src
set hive.fetch.task.conversion=more;

-- should return a value
select * from src tablesample (1 rows) where length(key) <> reverse(key);