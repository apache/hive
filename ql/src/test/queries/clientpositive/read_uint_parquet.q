-- Disable vectorization
SET hive.vectorized.execution.enabled=false;

create table testbasicint (uint_32_col int) stored as parquet;
load data local inpath '../../data/files/test_uint.parquet' into table testbasicint;
select * from testbasicint;
drop table testbasicint;

create table testbigintinv
(col_INT32_UINT_8 bigint,
 col_INT32_UINT_16 bigint,
 col_INT32_UINT_32 bigint,
 col_INT64_UINT_64 bigint) stored as parquet;
load data local inpath '../../data/files/data_including_invalid_values.parquet' into table testbigintinv;
select * from testbigintinv;
drop table testbigintinv;

create table testintinv
(col_INT32_UINT_8  int,
 col_INT32_UINT_16 int,
 col_INT32_UINT_32 int,
 col_INT64_UINT_64 int) stored as parquet;
load data local inpath '../../data/files/data_including_invalid_values.parquet' into table testintinv;
select * from testintinv;
drop table testintinv;

create table testsmallintinv
(col_INT32_UINT_8  smallint,
 col_INT32_UINT_16 smallint,
 col_INT32_UINT_32 smallint,
 col_INT64_UINT_64 smallint) stored as parquet;
load data local inpath '../../data/files/data_including_invalid_values.parquet' into table testsmallintinv;
select * from testsmallintinv;
drop table testsmallintinv;

create table testtinyintinv
(col_INT32_UINT_8  tinyint,
 col_INT32_UINT_16 tinyint,
 col_INT32_UINT_32 tinyint,
 col_INT64_UINT_64 tinyint) stored as parquet;
load data local inpath '../../data/files/data_including_invalid_values.parquet' into table testtinyintinv;
select * from testtinyintinv;
drop table testtinyintinv;

create table testbigintvalid
(col_INT32_UINT_8 bigint,
 col_INT32_UINT_16 bigint,
 col_INT32_UINT_32 bigint,
 col_INT64_UINT_64 bigint) stored as parquet;
load data local inpath '../../data/files/data_with_valid_values.parquet' into table testbigintvalid;
select * from testbigintvalid;
drop table testbigintvalid;

create table testintvalid
(col_INT32_UINT_8  int,
 col_INT32_UINT_16 int,
 col_INT32_UINT_32 int,
 col_INT64_UINT_64 int) stored as parquet;
load data local inpath '../../data/files/data_with_valid_values.parquet' into table testintvalid;
select * from testintvalid;
drop table testintvalid;

create table testsmallintvalid
(col_INT32_UINT_8  smallint,
 col_INT32_UINT_16 smallint,
 col_INT32_UINT_32 smallint,
 col_INT64_UINT_64 smallint) stored as parquet;
load data local inpath '../../data/files/data_with_valid_values.parquet' into table testsmallintvalid;
select * from testsmallintvalid;
drop table testsmallintvalid;

create table testtinyintvalid
(col_INT32_UINT_8  tinyint,
 col_INT32_UINT_16 tinyint,
 col_INT32_UINT_32 tinyint,
 col_INT64_UINT_64 tinyint) stored as parquet;
load data local inpath '../../data/files/data_with_valid_values.parquet' into table testtinyintvalid;
select * from testtinyintvalid;
drop table testtinyintvalid;
