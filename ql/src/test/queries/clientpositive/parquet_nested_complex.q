-- Suppress vectorization due to known bug.  See HIVE-19016.
set hive.vectorized.execution.enabled=false;
set hive.test.vectorized.execution.enabled.override=none;

-- start with the original nestedcomplex_n0 test

create table nestedcomplex_n0 (
simple_int int,
max_nested_array  array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<int>>>>>>>>>>>>>>>>>>>>>>>,
max_nested_map    array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<map<string,string>>>>>>>>>>>>>>>>>>>>>>,
max_nested_struct array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<struct<s:string, i:bigint>>>>>>>>>>>>>>>>>>>>>>>,
simple_string string)
ROW FORMAT SERDE
   'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
   'hive.serialization.extend.nesting.levels'='true',
   'line.delim'='\n'
)
;

describe nestedcomplex_n0;
describe extended nestedcomplex_n0;

load data local inpath '../../data/files/nested_complex.txt' overwrite into table nestedcomplex_n0;

-- and load the table into Parquet

CREATE TABLE parquet_nested_complex STORED AS PARQUET AS SELECT * FROM nestedcomplex_n0;

SELECT * FROM parquet_nested_complex SORT BY simple_int;

DROP TABLE nestedcomplex_n0;
DROP TABLE parquet_nested_complex;
