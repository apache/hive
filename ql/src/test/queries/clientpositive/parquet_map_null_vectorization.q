set hive.mapred.mode=nonstrict;
set hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

DROP TABLE parquet_map_type;


CREATE TABLE parquet_map_type (
id int,
stringMap map<string, string>
) stored as parquet;


insert overwrite table parquet_map_type
SELECT 1, MAP('k1', null, 'k2', 'v2');


select id, stringMap from parquet_map_type;

select id, stringMap['k1'] from parquet_map_type group by id, stringMap['k1'];
