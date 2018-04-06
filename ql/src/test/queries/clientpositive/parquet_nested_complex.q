-- Suppress vectorization due to known bug.  See HIVE-19016.
set hive.vectorized.execution.enabled=false;
set hive.test.vectorized.execution.enabled.override=none;

-- start with the original nestedcomplex test

create table nestedcomplex (
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

describe nestedcomplex;
describe extended nestedcomplex;

load data local inpath '../../data/files/nested_complex.txt' overwrite into table nestedcomplex;

-- and load the table into Parquet

CREATE TABLE parquet_nested_complex STORED AS PARQUET AS SELECT * FROM nestedcomplex;

SELECT * FROM parquet_nested_complex SORT BY simple_int;

DROP TABLE nestedcomplex;
DROP TABLE parquet_nested_complex;
