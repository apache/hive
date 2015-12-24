set hive.fetch.task.conversion=none;
set hive.explain.user=false;
set hive.vectorized.execution.enabled=true;

create table src_orc (bool0 boolean, key0 string, key1 string, key2 string) stored as orc;
insert overwrite table src_orc select true, key, value, key from src;
insert into table src_orc select true, key, value, key from src;

explain SELECT IF ( ( (CASE WHEN bool0 THEN 1 WHEN NOT bool0 THEN 0 END) = (CASE WHEN TRUE THEN 1 WHEN NOT TRUE THEN 0 END) ), key0, IF ( ( (CASE WHEN bool0 THEN 1 WHEN NOT bool0 THEN 0 END) = (CASE WHEN FALSE THEN 1 WHEN NOT FALSE THEN 0 END) ), key1, key2 ) ) FROM src_orc;

SELECT IF ( ( (CASE WHEN bool0 THEN 1 WHEN NOT bool0 THEN 0 END) = (CASE WHEN TRUE THEN 1 WHEN NOT TRUE THEN 0 END) ), key0, IF ( ( (CASE WHEN bool0 THEN 1 WHEN NOT bool0 THEN 0 END) = (CASE WHEN FALSE THEN 1 WHEN NOT FALSE THEN 0 END) ), key1, key2 ) ) FROM src_orc;

