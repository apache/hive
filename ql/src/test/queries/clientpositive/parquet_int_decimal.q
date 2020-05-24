set hive.vectorized.execution.enabled=false;

DROP TABLE parquet_decimal_spark;
DROP TABLE parquet_decimal_bigint_spark;
DROP TABLE parquet_decimal_int_spark;
DROP TABLE parquet_decimal_smallint_spark;
DROP TABLE parquet_decimal_tinyint_spark;
DROP TABLE parquet_decimal_double_spark;
DROP TABLE parquet_decimal_float_spark;

CREATE TABLE parquet_decimal_spark (d1 decimal(4,2), d2 decimal(4,0), d3 decimal(12,5), d4 decimal(12,0)) stored as parquet;
CREATE TABLE parquet_decimal_bigint_spark (d1 bigint, d2 bigint, d3 bigint, d4 bigint) stored as parquet;
CREATE TABLE parquet_decimal_int_spark (d1 int, d2 int, d3 int, d4 int) stored as parquet;
CREATE TABLE parquet_decimal_smallint_spark (d1 smallint, d2 smallint, d3 smallint, d4 smallint) stored as parquet;
CREATE TABLE parquet_decimal_tinyint_spark (d1 tinyint, d2 tinyint, d3 tinyint, d4 tinyint) stored as parquet;
CREATE TABLE parquet_decimal_double_spark (d1 double, d2 double, d3 double, d4 double) stored as parquet;
CREATE TABLE parquet_decimal_float_spark (d1 float, d2 float, d3 float, d4 float) stored as parquet;
CREATE TABLE parquet_decimal_string_spark (d1 string, d2 string, d3 string, d4 string) stored as parquet;
CREATE TABLE parquet_decimal_varchar_spark (d1 varchar(100), d2 varchar(100), d3 varchar(100), d4 varchar(100)) stored as parquet;
CREATE TABLE parquet_decimal_char_spark (d1 char(4), d2 char(4), d3 char(4), d4 char(4)) stored as parquet;

LOAD DATA LOCAL INPATH '../../data/files/parquet_int_decimal_1.parquet' INTO TABLE parquet_decimal_spark;
LOAD DATA LOCAL INPATH '../../data/files/parquet_int_decimal_2.parquet' INTO TABLE parquet_decimal_spark;
LOAD DATA LOCAL INPATH '../../data/files/parquet_int_decimal_1.parquet' INTO TABLE parquet_decimal_bigint_spark;
LOAD DATA LOCAL INPATH '../../data/files/parquet_int_decimal_2.parquet' INTO TABLE parquet_decimal_bigint_spark;
LOAD DATA LOCAL INPATH '../../data/files/parquet_int_decimal_1.parquet' INTO TABLE parquet_decimal_int_spark;
LOAD DATA LOCAL INPATH '../../data/files/parquet_int_decimal_2.parquet' INTO TABLE parquet_decimal_int_spark;
LOAD DATA LOCAL INPATH '../../data/files/parquet_int_decimal_1.parquet' INTO TABLE parquet_decimal_smallint_spark;
LOAD DATA LOCAL INPATH '../../data/files/parquet_int_decimal_2.parquet' INTO TABLE parquet_decimal_smallint_spark;
LOAD DATA LOCAL INPATH '../../data/files/parquet_int_decimal_1.parquet' INTO TABLE parquet_decimal_tinyint_spark;
LOAD DATA LOCAL INPATH '../../data/files/parquet_int_decimal_2.parquet' INTO TABLE parquet_decimal_tinyint_spark;
LOAD DATA LOCAL INPATH '../../data/files/parquet_int_decimal_1.parquet' INTO TABLE parquet_decimal_double_spark;
LOAD DATA LOCAL INPATH '../../data/files/parquet_int_decimal_2.parquet' INTO TABLE parquet_decimal_double_spark;
LOAD DATA LOCAL INPATH '../../data/files/parquet_int_decimal_1.parquet' INTO TABLE parquet_decimal_float_spark;
LOAD DATA LOCAL INPATH '../../data/files/parquet_int_decimal_2.parquet' INTO TABLE parquet_decimal_float_spark;
LOAD DATA LOCAL INPATH '../../data/files/parquet_int_decimal_1.parquet' INTO TABLE parquet_decimal_string_spark;
LOAD DATA LOCAL INPATH '../../data/files/parquet_int_decimal_2.parquet' INTO TABLE parquet_decimal_string_spark;
LOAD DATA LOCAL INPATH '../../data/files/parquet_int_decimal_1.parquet' INTO TABLE parquet_decimal_varchar_spark;
LOAD DATA LOCAL INPATH '../../data/files/parquet_int_decimal_2.parquet' INTO TABLE parquet_decimal_varchar_spark;
LOAD DATA LOCAL INPATH '../../data/files/parquet_int_decimal_1.parquet' INTO TABLE parquet_decimal_char_spark;
LOAD DATA LOCAL INPATH '../../data/files/parquet_int_decimal_2.parquet' INTO TABLE parquet_decimal_char_spark;

SELECT * FROM parquet_decimal_spark;
SELECT * FROM parquet_decimal_bigint_spark;
SELECT * FROM parquet_decimal_int_spark;
SELECT * FROM parquet_decimal_smallint_spark;
SELECT * FROM parquet_decimal_tinyint_spark;
SELECT * FROM parquet_decimal_double_spark;
SELECT * FROM parquet_decimal_float_spark;

set hive.vectorized.execution.enabled=true;

SELECT * FROM parquet_decimal_spark order by d2;
SELECT * FROM parquet_decimal_bigint_spark order by d2;
SELECT * FROM parquet_decimal_int_spark order by d2;
SELECT * FROM parquet_decimal_smallint_spark order by d2;
SELECT * FROM parquet_decimal_tinyint_spark order by d2;
SELECT * FROM parquet_decimal_double_spark order by d2;
SELECT * FROM parquet_decimal_float_spark order by d2;
SELECT * FROM parquet_decimal_string_spark order by d2;
SELECT * FROM parquet_decimal_varchar_spark order by d2;
SELECT * FROM parquet_decimal_char_spark order by d2;
