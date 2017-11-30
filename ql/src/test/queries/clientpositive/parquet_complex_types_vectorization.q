set hive.mapred.mode=nonstrict;
set hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

DROP TABLE parquet_complex_types_staging;
DROP TABLE parquet_complex_types;

CREATE TABLE parquet_complex_types_staging (
id int,
m1 map<string, varchar(5)>,
l1 array<int>,
st1 struct<c1:int, c2:string>,
listIndex int
) ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':';

CREATE TABLE parquet_complex_types (
id int,
m1 map<string, varchar(5)>,
l1 array<int>,
st1 struct<c1:int, c2:string>,
listIndex int
) STORED AS PARQUET;

-- test data size < 1024
LOAD DATA LOCAL INPATH '../../data/files/parquet_complex_types.txt' OVERWRITE INTO TABLE parquet_complex_types_staging;
INSERT OVERWRITE TABLE parquet_complex_types
SELECT id, m1, l1, st1, listIndex FROM parquet_complex_types_staging where id < 1024;

-- verify the row number
select count(*) from parquet_complex_types;
-- test element select with constant and variable
explain vectorization expression select l1, l1[0], l1[1], l1[listIndex], listIndex from parquet_complex_types limit 10;
select l1, l1[0], l1[1], l1[listIndex], listIndex from parquet_complex_types limit 10;
-- test complex select with list
explain vectorization expression select sum(l1[0]), l1[1] from parquet_complex_types where l1[0] > 1000 group by l1[1] order by l1[1] limit 10;
select sum(l1[0]), l1[1] from parquet_complex_types where l1[0] > 1000 group by l1[1] order by l1[1] desc limit 10;

-- test data size = 1024
INSERT OVERWRITE TABLE parquet_complex_types
SELECT id, m1, l1, st1, listIndex FROM parquet_complex_types_staging where id < 1025;

-- verify the row number
select count(*) from parquet_complex_types;
-- test element select with constant and variable
explain vectorization expression select l1, l1[0], l1[1], l1[listIndex], listIndex from parquet_complex_types limit 10;
select l1, l1[0], l1[1], l1[listIndex], listIndex from parquet_complex_types limit 10;
-- test complex select with list
explain vectorization expression select sum(l1[0]), l1[1] from parquet_complex_types where l1[0] > 1000 group by l1[1] order by l1[1] limit 10;
select sum(l1[0]), l1[1] from parquet_complex_types where l1[0] > 1000 group by l1[1] order by l1[1] desc limit 10;

-- test data size = 1025
INSERT OVERWRITE TABLE parquet_complex_types
SELECT id, m1, l1, st1, listIndex FROM parquet_complex_types_staging;

-- verify the row number
select count(*) from parquet_complex_types;
-- test element select with constant and variable
explain vectorization expression select l1, l1[0], l1[1], l1[listIndex], listIndex from parquet_complex_types limit 10;
select l1, l1[0], l1[1], l1[listIndex], listIndex from parquet_complex_types limit 10;
-- test complex select with list
explain vectorization expression select sum(l1[0]), l1[1] from parquet_complex_types where l1[0] > 1000 group by l1[1] order by l1[1] limit 10;
select sum(l1[0]), l1[1] from parquet_complex_types where l1[0] > 1000 group by l1[1] order by l1[1] desc limit 10;
