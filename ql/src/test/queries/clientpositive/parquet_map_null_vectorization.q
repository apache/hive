set hive.mapred.mode=nonstrict;
set hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

-- *** BOOLEAN ***
CREATE TABLE parquet_map_type_boolean (
id int,
booleanMap map<boolean, boolean>
) stored as parquet;

insert into parquet_map_type_boolean SELECT 1, MAP(true, null, false, true); -- NULL as value
insert into parquet_map_type_boolean (id) VALUES (2);
--insert into parquet_map_type_boolean SELECT 3, MAP(null, false, true, true); -- NULL as key, fails until HIVE-25484 is solved

select id, booleanMap from parquet_map_type_boolean;
select id, booleanMap[true] from parquet_map_type_boolean group by id, booleanMap[true];


-- *** STRING ***
CREATE TABLE parquet_map_type_string (
id int,
stringMap map<string, string>
) stored as parquet;

insert into parquet_map_type_string SELECT 1, MAP('k1', null, 'k2', 'v2'); -- NULL as value
--insert into parquet_map_type_string (id) VALUES (2);-- NULL as map, fails until HIVE-25484 is solved
--insert into parquet_map_type_string SELECT 3, MAP(null, 'k3', 'k4', 'v4'); -- NULL as key, fails until HIVE-25484 is solved

select id, stringMap from parquet_map_type_string;
select id, stringMap['k1'] from parquet_map_type_string group by id, stringMap['k1'];


-- *** INT ***
CREATE TABLE parquet_map_type_int (
id int,
intMap map<int, int>
) stored as parquet;

insert into parquet_map_type_int SELECT 1, MAP(1, null, 2, 3); -- NULL as value
insert into parquet_map_type_int (id) VALUES (2); -- NULL as map
--insert into parquet_map_type_int SELECT 3, MAP(null, 4, 5, 6); -- NULL as key, fails until HIVE-25484 is solved

select id, intMap from parquet_map_type_int;
select id, intMap[1] from parquet_map_type_int group by id, intMap[1];


-- *** DOUBLE ***
CREATE TABLE parquet_map_type_double (
id int,
doubleMap map<double, double>
) stored as parquet;

--insert into parquet_map_type_double SELECT 1, MAP(CAST(1.0 as DOUBLE), null, CAST(2.0 as DOUBLE), CAST(3.0 as DOUBLE)); -- NULL as value, fails until HIVE-25484 is solved
insert into parquet_map_type_double (id) VALUES (2);
--insert into parquet_map_type_double SELECT 3, MAP(null, CAST(4.0 as DOUBLE), CAST(5.0 as DOUBLE), CAST(6.0 as DOUBLE)); -- NULL as key, fails until HIVE-25484 is solved

select id, doubleMap from parquet_map_type_double;
select id, doubleMap[1.0] from parquet_map_type_double group by id, doubleMap[1.0];


-- *** DECIMAL ***
CREATE TABLE parquet_map_type_decimal (
id int,
decimalMap map<decimal(1,0), decimal(1,0)>
) stored as parquet;

insert into parquet_map_type_decimal SELECT 1, MAP(1.0, NULL, 2.0, 3.0);
insert into parquet_map_type_decimal (id) VALUES (2);
--insert into parquet_map_type_decimal SELECT 3, MAP(null, 4.0, 5.0, 6.0); -- NULL as key, fails until HIVE-25484 is solved

select id, decimalMap from parquet_map_type_decimal;
select id, decimalMap[1.0] from parquet_map_type_decimal group by id, decimalMap[1.0];


-- *** DATE ***
CREATE TABLE parquet_map_type_date (
id int,
dateMap map<date, date>
) stored as parquet;

insert into parquet_map_type_date SELECT 1, MAP(CAST('2015-11-29' AS DATE), NULL, CAST('2016-11-29' AS DATE), CAST('2017-11-29' AS DATE));
insert into parquet_map_type_date (id) VALUES (2);
--insert into parquet_map_type_date SELECT 3, MAP(null, CAST('2018-11-29' AS DATE), CAST('2019-11-29' AS DATE), CAST('2020-11-29' AS DATE)); -- NULL as key, fails until HIVE-25484 is solved

select id, dateMap from parquet_map_type_date;
select id, dateMap[CAST('2015-11-29' AS DATE)] from parquet_map_type_date group by id, dateMap[CAST('2015-11-29' AS DATE)];


-- *** TIMESTAMP: currently not supported by VectorizedListColumnReader.fillColumnVector ***