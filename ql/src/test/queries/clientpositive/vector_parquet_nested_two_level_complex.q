set hive.vectorized.execution.enabled=true;
set hive.explain.user=false;
set hive.fetch.task.conversion=none;

create table nested_array_array_table (
nested_array_array  array<array<int>>)
STORED AS PARQUET;

create table nested_array_map_table (
nested_array_map  array<map<string,string>>)
STORED AS PARQUET;

create table nested_array_struct_table (
nested_array_map  array<struct<latitude: DOUBLE, longitude: DOUBLE>>)
STORED AS PARQUET;

create table nested_map_array_table (
nested_map_array  map<string,array<int>>)
STORED AS PARQUET;

create table nested_map_map_table (
nested_map_map    map<string,map<string,string>>)
STORED AS PARQUET;

create table nested_map_struct_table (
nested_map_struct    map<string,struct<latitude: DOUBLE, longitude: DOUBLE>>)
STORED AS PARQUET;

create table nested_struct_array_table (
nested_struct_array struct<s:string, i:bigint, a:array<int>>)
STORED AS PARQUET;

create table nested_struct_map_table (
nested_struct_map struct<s:string, i:bigint, m:map<string,string>>)
STORED AS PARQUET;

create table nested_struct_struct_table (
nested_struct_struct struct<s:string, i:bigint, s2:struct<latitude: DOUBLE, longitude: DOUBLE>>)
STORED AS PARQUET;


EXPLAIN VECTORIZATION DETAIL
SELECT * FROM nested_array_array_table;

EXPLAIN VECTORIZATION DETAIL
SELECT * FROM nested_array_map_table;

EXPLAIN VECTORIZATION DETAIL
SELECT * FROM nested_array_map_table;

EXPLAIN VECTORIZATION DETAIL
SELECT * FROM nested_map_array_table;

EXPLAIN VECTORIZATION DETAIL
SELECT * FROM nested_map_map_table;

EXPLAIN VECTORIZATION DETAIL
SELECT * FROM nested_map_struct_table;

EXPLAIN VECTORIZATION DETAIL
SELECT * FROM nested_struct_array_table;

EXPLAIN VECTORIZATION DETAIL
SELECT * FROM nested_struct_map_table;

EXPLAIN VECTORIZATION DETAIL
SELECT * FROM nested_struct_struct_table;
