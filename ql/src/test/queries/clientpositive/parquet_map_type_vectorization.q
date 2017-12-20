set hive.mapred.mode=nonstrict;
set hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

DROP TABLE parquet_map_type_staging;
DROP TABLE parquet_map_type;

CREATE TABLE parquet_map_type_staging (
id int,
stringMap map<string, string>,
intMap map<int, int>,
doubleMap map<double, double>,
stringIndex string,
intIndex int,
doubleIndex double
) ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '|'
  COLLECTION ITEMS TERMINATED BY ','
  MAP KEYS TERMINATED BY ':';

CREATE TABLE parquet_map_type (
id int,
stringMap map<string, string>,
intMap map<int, int>,
doubleMap map<double, double>,
stringIndex string,
intIndex int,
doubleIndex double
) STORED AS PARQUET;

-- test data size < 1024
LOAD DATA LOCAL INPATH '../../data/files/parquet_vector_map_type.txt' OVERWRITE INTO TABLE parquet_map_type_staging;
INSERT OVERWRITE TABLE parquet_map_type
SELECT id, stringMap, intMap, doubleMap, stringIndex, intIndex, doubleIndex FROM parquet_map_type_staging where id < 1024;

-- verify the row number
select count(*) from parquet_map_type;
-- test element select with constant and variable
explain vectorization expression select stringMap, intMap, doubleMap, stringMap['k2'], intMap[456],
doubleMap[123.123], stringMap[stringIndex], intMap[intIndex], doubleMap[doubleIndex] from parquet_map_type limit 10;
select stringMap, intMap, doubleMap, stringMap['k2'], intMap[456], doubleMap[123.123],
stringMap[stringIndex], intMap[intIndex], doubleMap[doubleIndex] from parquet_map_type limit 10;
-- test complex select with map
explain vectorization expression select sum(intMap[123]), sum(doubleMap[123.123]), stringMap['k1']
from parquet_map_type where stringMap['k1'] like 'v100%' group by stringMap['k1'] order by stringMap['k1'] limit 10;
select sum(intMap[123]), sum(doubleMap[123.123]), stringMap['k1']
from parquet_map_type where stringMap['k1'] like 'v100%' group by stringMap['k1'] order by stringMap['k1'] limit 10;

-- test data size = 1024
INSERT OVERWRITE TABLE parquet_map_type
SELECT id, stringMap, intMap, doubleMap, stringIndex, intIndex, doubleIndex FROM parquet_map_type_staging where id < 1025;

-- verify the row number
select count(*) from parquet_map_type;
-- test element select with constant and variable
select stringMap, intMap, doubleMap, stringMap['k2'], intMap[456], doubleMap[123.123],
stringMap[stringIndex], intMap[intIndex], doubleMap[doubleIndex] from parquet_map_type limit 10;
-- test complex select with map
select sum(intMap[123]), sum(doubleMap[123.123]), stringMap['k1']
from parquet_map_type where stringMap['k1'] like 'v100%' group by stringMap['k1'] order by stringMap['k1'] limit 10;

-- test data size = 1025
INSERT OVERWRITE TABLE parquet_map_type
SELECT id, stringMap, intMap, doubleMap, stringIndex, intIndex, doubleIndex FROM parquet_map_type_staging;

-- verify the row number
select count(*) from parquet_map_type;
-- test element select with constant and variable
select stringMap, intMap, doubleMap, stringMap['k2'], intMap[456], doubleMap[123.123],
stringMap[stringIndex], intMap[intIndex], doubleMap[doubleIndex] from parquet_map_type limit 10;
-- test complex select with map
select sum(intMap[123]), sum(doubleMap[123.123]), stringMap['k1']
from parquet_map_type where stringMap['k1'] like 'v100%' group by stringMap['k1'] order by stringMap['k1'] limit 10;
