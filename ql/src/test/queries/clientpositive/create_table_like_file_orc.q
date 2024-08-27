-- all primitive types
-- timestamp_w_tz TIMESTAMP WITH LOCAL TIME ZONE is not supported by hive's orc implementation
CREATE EXTERNAL TABLE test_all_orc_types(tinyint_type TINYINT, smallint_type SMALLINT, bigint_type BIGINT, int_type INT, float_type FLOAT, double_type double, decimal_type DECIMAL(4,2), timestamp_type TIMESTAMP, date_type DATE, string_type STRING, varchar_type VARCHAR(100), char_type CHAR(34), boolean_type BOOLEAN, binary_type BINARY) STORED AS ORC LOCATION '${system:test.tmp.dir}/test_all_orc_types';
-- insert two rows (the other tables only have 1 row)
INSERT INTO test_all_orc_types VALUES (1, 2, 3, 4, 2.2, 2.2, 20.20, '2022-06-30 10:20:30', '2020-04-23', 'str1', 'varchar1', 'char', true, 'binary_maybe'),
       (1, 2, 3, 4, 2.2, 2.2, 20.20, '2022-06-30 10:20:30', '2020-04-23', 'str1', 'varchar1', 'char', true, 'binary_maybe');
	   SELECT * FROM test_all_orc_types;
DESCRIBE test_all_orc_types;
-- CREATE A LIKE table
CREATE TABLE like_test_all_orc_types LIKE FILE ORC '${system:test.tmp.dir}/test_all_orc_types/000000_0';
INSERT INTO like_test_all_orc_types VALUES (1, 2, 3, 4, 2.2, 2.2, 20.20, '2022-06-30 10:20:30', '2020-04-23', 'str1', 'varchar1', 'char', true, 'binary_maybe'),
       (1, 2, 3, 4, 2.2, 2.2, 20.20, '2022-06-30 10:20:30', '2020-04-23', 'str1', 'varchar1', 'char', true, 'binary_maybe');
SELECT * FROM like_test_all_orc_types;
DESCRIBE like_test_all_orc_types;
DROP TABLE test_all_orc_types;
DROP TABLE like_test_all_orc_types;

-- complex types (array, map, union, struct)
-- array
CREATE EXTERNAL TABLE test_orc_array(str_array array<string>) STORED AS ORC LOCATION '${system:test.tmp.dir}/test_orc_array';
DESCRIBE test_orc_array;
INSERT INTO test_orc_array SELECT array("bob", "sue");
SELECT * FROM test_orc_array;
CREATE TABLE like_test_orc_array LIKE FILE ORC '${system:test.tmp.dir}/test_orc_array/000000_0';
DESCRIBE like_test_orc_array;
INSERT INTO like_test_orc_array SELECT array("bob", "sue");
SELECT * FROM like_test_orc_array;
DROP TABLE like_test_orc_array;

-- map
CREATE EXTERNAL TABLE test_orc_map(simple_map map<int, string>, map_to_struct map<string, struct<i : int>>, map_to_map map<date,map<int, string>>, map_to_array map<binary, array<array<int>>>) STORED AS ORC LOCATION '${system:test.tmp.dir}/test_orc_map';
DESCRIBE test_orc_map;
INSERT INTO test_orc_map SELECT map(10, "foo"), map("bar", named_struct("i", 99)), map(cast('1984-01-01' as date), map(10, "goodbye")), map(cast("binary" as binary), array(array(1,2,3)));
SELECT * FROM test_orc_map;
CREATE TABLE like_test_orc_map LIKE FILE ORC '${system:test.tmp.dir}/test_orc_map/000000_0';
DESCRIBE like_test_orc_map;
INSERT INTO like_test_orc_map SELECT map(10, "foo"), map("bar", named_struct("i", 99)), map(cast('1984-01-01' as date), map(10, "goodbye")), map(cast("binary" as binary), array(array(1,2,3)));
SELECT * FROM like_test_orc_map;
DROP TABLE like_test_orc_map;

-- union
CREATE TABLE src_tbl (key STRING, value STRING) STORED AS TEXTFILE;
INSERT INTO src_tbl VALUES ('hello', 'world');
CREATE TABLE test_orc_union (foo UNIONTYPE<string>) STORED AS ORC LOCATION '${system:test.tmp.dir}/test_orc_union';
INSERT INTO test_orc_union SELECT create_union(0, key) FROM src_tbl LIMIT 2;
CREATE TABLE like_test_orc_union LIKE FILE ORC '${system:test.tmp.dir}/test_orc_union/000000_0';
DESCRIBE test_orc_union;
INSERT INTO like_test_orc_union SELECT create_union(0, key) FROM src_tbl LIMIT 2;
SELECT * FROM like_test_orc_union;
DROP TABLE like_test_orc_union;

-- struct
CREATE EXTERNAL TABLE test_complex_orc_struct(struct_type struct<tinyint_type : tinyint, smallint_type : smallint, bigint_type : bigint, int_type : int, float_type : float, double_type : double, decimal_type : DECIMAL(4,2), timestamp_type : TIMESTAMP, date_type : DATE, string_type : STRING, varchar_type : VARCHAR(100), char_type : CHAR(34), boolean_type : boolean, binary_type : binary>) STORED AS ORC LOCATION '${system:test.tmp.dir}/test_complex_orc_struct';
DESCRIBE test_complex_orc_struct;
-- disable CBO due to the fact that type conversion causes CBO failure which causes the test to fail
-- non-CBO path works (HIVE-26398)
SET hive.cbo.enable=false;
INSERT INTO test_complex_orc_struct SELECT named_struct("tinyint_type", cast(1 as tinyint), "smallint_type", cast(2 as smallint), "bigint_type", cast(3 as bigint), "int_type", 4, "float_type", cast(2.2 as float), "double_type", cast(2.2 as double), "decimal_type", cast(20.22 as decimal(4,2)), "timestamp_type", cast('2022-06-30 10:20:30' as timestamp), "date_type", cast('2020-04-23' as date), "string_type", 'str1', "varchar_type", cast('varchar1' as varchar(100)), "char_type", cast('char' as char(34)), "boolean_type", true, "binary_type", cast('binary_maybe' as binary));
SET hive.cbo.enable=true;
SELECT * FROM test_complex_orc_struct;
CREATE TABLE like_test_complex_orc_struct LIKE FILE ORC '${system:test.tmp.dir}/test_complex_orc_struct/000000_0';
DESCRIBE like_test_complex_orc_struct;
-- disable CBO due to the fact that type conversion causes CBO failure which causes the test to fail
-- non-CBO path works (HIVE-26398)
SET hive.cbo.enable=false;
INSERT INTO like_test_complex_orc_struct SELECT named_struct("tinyint_type", cast(1 as tinyint), "smallint_type", cast(2 as smallint), "bigint_type", cast(3 as bigint), "int_type", 4, "float_type", cast(2.2 as float), "double_type", cast(2.2 as double), "decimal_type", cast(20.22 as decimal(4,2)), "timestamp_type", cast('2022-06-30 10:20:30' as timestamp), "date_type", cast('2020-04-23' as date), "string_type", 'str1', "varchar_type", cast('varchar1' as varchar(100)), "char_type", cast('char' as char(34)), "boolean_type", true, "binary_type", cast('binary_maybe' as binary));
SET hive.cbo.enable=true;
SELECT * FROM like_test_complex_orc_struct;
DROP TABLE like_test_complex_orc_struct;

-- test complex types that contain other complex types
CREATE EXTERNAL TABLE test_orc_complex_complex(struct_type struct<i : int, s : string, m : map<string, array<int>>, struct_i : struct<str : string>>) STORED AS ORC LOCATION '${system:test.tmp.dir}/test_orc_complex_complex';
DESCRIBE test_orc_complex_complex;
INSERT INTO test_orc_complex_complex SELECT named_struct("i", 10, "s", "hello, world", "m", map("arr", array(1,2,3,4)), "struct_i", named_struct("str", "test_str"));
SELECT * FROM test_orc_complex_complex;
CREATE TABLE like_test_orc_complex_complex LIKE FILE ORC '${system:test.tmp.dir}/test_orc_complex_complex/000000_0';
DESCRIBE like_test_orc_complex_complex;
INSERT INTO like_test_orc_complex_complex SELECT named_struct("i", 10, "s", "hello, world", "m", map("arr", array(1,2,3,4)), "struct_i", named_struct("str", "test_str"));
SELECT * FROM like_test_orc_complex_complex;
DROP TABLE like_test_orc_complex_complex;

-- test adding partitioning to the destination table
CREATE TABLE like_test_orc_partitioning LIKE FILE ORC '${system:test.tmp.dir}/test_all_orc_types/000000_0' PARTITIONED BY (year STRING, month STRING);
DESCRIBE like_test_orc_partitioning;
INSERT INTO like_test_orc_partitioning PARTITION (year='1984', month='1') VALUES (1, 2, 3, 4, 2.2, 2.2, 20.20, '2022-06-30 10:20:30', '2020-04-23', 'str1', 'varchar1', 'char', true, 'binary_maybe'),
       (1, 2, 3, 4, 2.2, 2.2, 20.20, '2022-06-30 10:20:30', '2020-04-23', 'str1', 'varchar1', 'char', true, 'binary_maybe');
SELECT * FROM like_test_orc_partitioning;
DROP TABLE like_test_orc_partitioning;