-- all primitive types
-- timestamp_w_tz TIMESTAMP WITH LOCAL TIME ZONE is not supported by hive's parquet implementation
CREATE EXTERNAL TABLE test_all_types(tinyint_type TINYINT, smallint_type SMALLINT, bigint_type BIGINT, int_type INT, float_type FLOAT, double_type double, decimal_type DECIMAL(4,2), timestamp_type TIMESTAMP, date_type DATE, string_type STRING, varchar_type VARCHAR(100), char_type CHAR(34), boolean_type BOOLEAN, binary_type BINARY) STORED AS PARQUET LOCATION '${system:test.tmp.dir}/test_all_types';
-- insert two rows (the other tables only have 1 row)
INSERT INTO test_all_types VALUES (1, 2, 3, 4, 2.2, 2.2, 20.20, '2022-06-30 10:20:30', '2020-04-23', 'str1', 'varchar1', 'char', true, 'binary_maybe'),
       (1, 2, 3, 4, 2.2, 2.2, 20.20, '2022-06-30 10:20:30', '2020-04-23', 'str1', 'varchar1', 'char', true, 'binary_maybe');
SELECT * FROM test_all_types;
DESCRIBE test_all_types;
-- CREATE A LIKE table
CREATE TABLE like_test_all_types LIKE FILE PARQUET '${system:test.tmp.dir}/test_all_types/000000_0';
INSERT INTO like_test_all_types VALUES (1, 2, 3, 4, 2.2, 2.2, 20.20, '2022-06-30 10:20:30', '2020-04-23', 'str1', 'varchar1', 'char', true, 'binary_maybe'),
       (1, 2, 3, 4, 2.2, 2.2, 20.20, '2022-06-30 10:20:30', '2020-04-23', 'str1', 'varchar1', 'char', true, 'binary_maybe');
SELECT * FROM like_test_all_types;
DESCRIBE like_test_all_types;
DROP TABLE test_all_types;
DROP TABLE like_test_all_types;

-- test hive.parquet.infer.binary.as string
SET hive.parquet.infer.binary.as = String;
CREATE TABLE like_test_all_types LIKE FILE PARQUET '${system:test.tmp.dir}/test_all_types/000000_0';
INSERT INTO like_test_all_types VALUES (1, 2, 3, 4, 2.2, 2.2, 20.20, '2022-06-30 10:20:30', '2020-04-23', 'str1', 'varchar1', 'char', true, 'binary_maybe'),
       (1, 2, 3, 4, 2.2, 2.2, 20.20, '2022-06-30 10:20:30', '2020-04-23', 'str1', 'varchar1', 'char', true, 'binary_maybe');
SELECT * FROM like_test_all_types;
DESCRIBE like_test_all_types;
DROP TABLE test_all_types;
DROP TABLE like_test_all_types;
SET hive.parquet.infer.binary.as = binary;

-- complex types (struct, array, map, union)
-- union type is not supported by PARQUET in hive
-- array
CREATE EXTERNAL TABLE test_array(str_array array<string>) STORED AS PARQUET LOCATION '${system:test.tmp.dir}/test_array';
DESCRIBE test_array;
INSERT INTO test_array SELECT array("bob", "sue");
SELECT * FROM test_array;
CREATE TABLE like_test_array LIKE FILE PARQUET '${system:test.tmp.dir}/test_array/000000_0';
DESCRIBE like_test_array;
INSERT INTO like_test_array SELECT array("bob", "sue");
SELECT * FROM like_test_array;
DROP TABLE like_test_array;

-- map
CREATE EXTERNAL TABLE test_map(simple_map map<int, string>, map_to_struct map<string, struct<i : int>>, map_to_map map<date,map<int, string>>, map_to_array map<binary, array<array<int>>>) STORED AS PARQUET LOCATION '${system:test.tmp.dir}/test_map';
DESCRIBE test_map;
INSERT INTO test_map SELECT map(10, "foo"), map("bar", named_struct("i", 99)), map(cast('1984-01-01' as date), map(10, "goodbye")), map(cast("binary" as binary), array(array(1,2,3)));
SELECT * FROM test_map;
CREATE TABLE like_test_map LIKE FILE PARQUET '${system:test.tmp.dir}/test_map/000000_0';
DESCRIBE like_test_map;
INSERT INTO like_test_map SELECT map(10, "foo"), map("bar", named_struct("i", 99)), map(cast('1984-01-01' as date), map(10, "goodbye")), map(cast("binary" as binary), array(array(1,2,3)));
SELECT * FROM like_test_map;
DROP TABLE like_test_map;

-- struct
CREATE EXTERNAL TABLE test_complex_struct(struct_type struct<tinyint_type : tinyint, smallint_type : smallint, bigint_type : bigint, int_type : int, float_type : float, double_type : double, decimal_type : DECIMAL(4,2), timestamp_type : TIMESTAMP, date_type : DATE, string_type : STRING, varchar_type : VARCHAR(100), char_type : CHAR(34), boolean_type : boolean, binary_type : binary>) STORED AS PARQUET LOCATION '${system:test.tmp.dir}/test_complex_struct';
DESCRIBE test_complex_struct;
-- disable CBO due to the fact that type conversion causes CBO failure which causes the test to fail
-- non-CBO path works (HIVE-26398)
SET hive.cbo.enable=false;
INSERT INTO test_complex_struct SELECT named_struct("tinyint_type", cast(1 as tinyint), "smallint_type", cast(2 as smallint), "bigint_type", cast(3 as bigint), "int_type", 4, "float_type", cast(2.2 as float), "double_type", cast(2.2 as double), "decimal_type", cast(20.22 as decimal(4,2)), "timestamp_type", cast('2022-06-30 10:20:30' as timestamp), "date_type", cast('2020-04-23' as date), "string_type", 'str1', "varchar_type", cast('varchar1' as varchar(100)), "char_type", cast('char' as char(34)), "boolean_type", true, "binary_type", cast('binary_maybe' as binary));
SET hive.cbo.enable=true;
SELECT * FROM test_complex_struct;
-- varchar/char get created as string due to the fact that the parquet file has no information to derive this types and they are stored as string
CREATE TABLE like_test_complex_struct LIKE FILE PARQUET '${system:test.tmp.dir}/test_complex_struct/000000_0';
DESCRIBE like_test_complex_struct;
-- disable CBO due to the fact that type conversion causes CBO failure which causes the test to fail
-- non-CBO path works (HIVE-26398)
SET hive.cbo.enable=false;
INSERT INTO like_test_complex_struct SELECT named_struct("tinyint_type", cast(1 as tinyint), "smallint_type", cast(2 as smallint), "bigint_type", cast(3 as bigint), "int_type", 4, "float_type", cast(2.2 as float), "double_type", cast(2.2 as double), "decimal_type", cast(20.22 as decimal(4,2)), "timestamp_type", cast('2022-06-30 10:20:30' as timestamp), "date_type", cast('2020-04-23' as date), "string_type", 'str1', "varchar_type", 'varchar1', "char_type", 'char', "boolean_type", true, "binary_type", cast('binary_maybe' as binary));
SET hive.cbo.enable=true;
SELECT * FROM like_test_complex_struct;
DROP TABLE like_test_complex_struct;

-- test complex types that contain other complex types
CREATE EXTERNAL TABLE test_complex_complex(struct_type struct<i : int, s : string, m : map<string, array<int>>, struct_i : struct<str : string>>) STORED AS PARQUET LOCATION '${system:test.tmp.dir}/test_complex_complex';
DESCRIBE test_complex_complex;
INSERT INTO test_complex_complex SELECT named_struct("i", 10, "s", "hello, world", "m", map("arr", array(1,2,3,4)), "struct_i", named_struct("str", "test_str"));
SELECT * FROM test_complex_complex;
CREATE TABLE like_test_complex_complex LIKE FILE PARQUET '${system:test.tmp.dir}/test_complex_complex/000000_0';
DESCRIBE like_test_complex_complex;
INSERT INTO like_test_complex_complex SELECT named_struct("i", 10, "s", "hello, world", "m", map("arr", array(1,2,3,4)), "struct_i", named_struct("str", "test_str"));
SELECT * FROM like_test_complex_complex;
DROP TABLE like_test_complex_complex;

-- test adding partitioning to the destination table
CREATE TABLE like_test_partitioning LIKE FILE PARQUET '${system:test.tmp.dir}/test_all_types/000000_0' PARTITIONED BY (year STRING, month STRING);
DESCRIBE like_test_partitioning;
INSERT INTO like_test_partitioning PARTITION (year='1984', month='1') VALUES (1, 2, 3, 4, 2.2, 2.2, 20.20, '2022-06-30 10:20:30', '2020-04-23', 'str1', 'varchar1', 'char', true, 'binary_maybe'),
       (1, 2, 3, 4, 2.2, 2.2, 20.20, '2022-06-30 10:20:30', '2020-04-23', 'str1', 'varchar1', 'char', true, 'binary_maybe');
SELECT * FROM like_test_partitioning;
DROP TABLE like_test_partitioning;
