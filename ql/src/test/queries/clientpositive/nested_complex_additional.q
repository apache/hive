create table nestedcomplex_additional (
simple_int int,
max_nested_array  array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<int>>>>>>>>>>>>>>>>>>>>>>>>>,
max_nested_map    array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<map<string,string>>>>>>>>>>>>>>>>>>>>>>>>>>,
max_nested_struct array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<array<struct<s:string, i:int>>>>>>>>>>>>>>>>>>>>>>>>>>,
simple_string string)
ROW FORMAT SERDE
   'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
   'hive.serialization.extend.additional.nesting.levels'='true',
   'line.delim'='\n'
)
;

describe nestedcomplex_additional;
describe extended nestedcomplex_additional;


load data local inpath '../../data/files/nestedcomplex_additional.txt' overwrite into table nestedcomplex_additional;

select * from nestedcomplex_additional sort by simple_int;
