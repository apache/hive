-- Enable vectorization
SET hive.vectorized.execution.enabled=true;
SET hive.fetch.task.conversion=none;

create table testbasicint_n0 (uint_32_col int) stored as parquet;
load data local inpath '../../data/files/test_uint.parquet' into table testbasicint_n0;
select * from testbasicint_n0;
drop table testbasicint_n0;

create table testbigintinv_n0
(col_INT32_UINT_8 bigint,
 col_INT32_UINT_16 bigint,
 col_INT32_UINT_32 bigint,
 col_INT64_UINT_64 bigint) stored as parquet;
load data local inpath '../../data/files/data_including_invalid_values.parquet' into table testbigintinv_n0;
select * from testbigintinv_n0;
drop table testbigintinv_n0;

create table testintinv_n0
(col_INT32_UINT_8  int,
 col_INT32_UINT_16 int,
 col_INT32_UINT_32 int,
 col_INT64_UINT_64 int) stored as parquet;
load data local inpath '../../data/files/data_including_invalid_values.parquet' into table testintinv_n0;
select * from testintinv_n0;
drop table testintinv_n0;

create table testsmallintinv_n0
(col_INT32_UINT_8  smallint,
 col_INT32_UINT_16 smallint,
 col_INT32_UINT_32 smallint,
 col_INT64_UINT_64 smallint) stored as parquet;
load data local inpath '../../data/files/data_including_invalid_values.parquet' into table testsmallintinv_n0;
select * from testsmallintinv_n0;
drop table testsmallintinv_n0;

create table testtinyintinv_n0
(col_INT32_UINT_8  tinyint,
 col_INT32_UINT_16 tinyint,
 col_INT32_UINT_32 tinyint,
 col_INT64_UINT_64 tinyint) stored as parquet;
load data local inpath '../../data/files/data_including_invalid_values.parquet' into table testtinyintinv_n0;
select * from testtinyintinv_n0;
drop table testtinyintinv_n0;

create table testfloatinv_n0
(col_INT32_UINT_8  float,
 col_INT32_UINT_16 float,
 col_INT32_UINT_32 float,
 col_INT64_UINT_64 float) stored as parquet;
load data local inpath '../../data/files/data_including_invalid_values.parquet' into table testfloatinv_n0;
select * from testfloatinv_n0;
drop table testfloatinv_n0;

create table testdoubleinv_n0
(col_INT32_UINT_8  double,
 col_INT32_UINT_16 double,
 col_INT32_UINT_32 double,
 col_INT64_UINT_64 double) stored as parquet;
load data local inpath '../../data/files/data_including_invalid_values.parquet' into table testdoubleinv_n0;
select * from testdoubleinv_n0;
drop table testdoubleinv_n0;

create table testdecimal22_2inv_n0
(col_INT32_UINT_8  decimal(22,2),
 col_INT32_UINT_16 decimal(22,2),
 col_INT32_UINT_32 decimal(22,2),
 col_INT64_UINT_64 decimal(22,2)) stored as parquet;
load data local inpath '../../data/files/data_including_invalid_values.parquet' into table testdecimal22_2inv_n0;
select * from testdecimal22_2inv_n0;
drop table testdecimal22_2inv_n0;

create table testdecimal13_2inv_n0
(col_INT32_UINT_8  decimal(13,2),
 col_INT32_UINT_16 decimal(13,2),
 col_INT32_UINT_32 decimal(13,2),
 col_INT64_UINT_64 decimal(13,2)) stored as parquet;
load data local inpath '../../data/files/data_including_invalid_values.parquet' into table testdecimal13_2inv_n0;
select * from testdecimal13_2inv_n0;
drop table testdecimal13_2inv_n0;

create table testdecimal8_2inv_n0
(col_INT32_UINT_8  decimal(8,2),
 col_INT32_UINT_16 decimal(8,2),
 col_INT32_UINT_32 decimal(8,2),
 col_INT64_UINT_64 decimal(8,2)) stored as parquet;
load data local inpath '../../data/files/data_including_invalid_values.parquet' into table testdecimal8_2inv_n0;
select * from testdecimal8_2inv_n0;
drop table testdecimal8_2inv_n0;

create table testdecimal6_2inv_n0
(col_INT32_UINT_8  decimal(6,2),
 col_INT32_UINT_16 decimal(6,2),
 col_INT32_UINT_32 decimal(6,2),
 col_INT64_UINT_64 decimal(6,2)) stored as parquet;
load data local inpath '../../data/files/data_including_invalid_values.parquet' into table testdecimal6_2inv_n0;
select * from testdecimal6_2inv_n0;
drop table testdecimal6_2inv_n0;

create table testdecimal3_2inv_n0
(col_INT32_UINT_8  decimal(3,2),
 col_INT32_UINT_16 decimal(3,2),
 col_INT32_UINT_32 decimal(3,2),
 col_INT64_UINT_64 decimal(3,2)) stored as parquet;
load data local inpath '../../data/files/data_including_invalid_values.parquet' into table testdecimal3_2inv_n0;
select * from testdecimal3_2inv_n0;
drop table testdecimal3_2inv_n0;

create table testbigintvalid_n0
(col_INT32_UINT_8 bigint,
 col_INT32_UINT_16 bigint,
 col_INT32_UINT_32 bigint,
 col_INT64_UINT_64 bigint) stored as parquet;
load data local inpath '../../data/files/data_with_valid_values.parquet' into table testbigintvalid_n0;
select * from testbigintvalid_n0;
drop table testbigintvalid_n0;

create table testintvalid_n0
(col_INT32_UINT_8  int,
 col_INT32_UINT_16 int,
 col_INT32_UINT_32 int,
 col_INT64_UINT_64 int) stored as parquet;
load data local inpath '../../data/files/data_with_valid_values.parquet' into table testintvalid_n0;
select * from testintvalid_n0;
drop table testintvalid_n0;

create table testsmallintvalid_n0
(col_INT32_UINT_8  smallint,
 col_INT32_UINT_16 smallint,
 col_INT32_UINT_32 smallint,
 col_INT64_UINT_64 smallint) stored as parquet;
load data local inpath '../../data/files/data_with_valid_values.parquet' into table testsmallintvalid_n0;
select * from testsmallintvalid_n0;
drop table testsmallintvalid_n0;

create table testtinyintvalid_n0
(col_INT32_UINT_8  tinyint,
 col_INT32_UINT_16 tinyint,
 col_INT32_UINT_32 tinyint,
 col_INT64_UINT_64 tinyint) stored as parquet;
load data local inpath '../../data/files/data_with_valid_values.parquet' into table testtinyintvalid_n0;
select * from testtinyintvalid_n0;
drop table testtinyintvalid_n0;

create table testfloatvalid_n0
(col_INT32_UINT_8  float,
 col_INT32_UINT_16 float,
 col_INT32_UINT_32 float,
 col_INT64_UINT_64 float) stored as parquet;
load data local inpath '../../data/files/data_with_valid_values.parquet' into table testfloatvalid_n0;
select * from testfloatvalid_n0;
drop table testfloatvalid_n0;

create table testdoublevalid_n0
(col_INT32_UINT_8  double,
 col_INT32_UINT_16 double,
 col_INT32_UINT_32 double,
 col_INT64_UINT_64 double) stored as parquet;
load data local inpath '../../data/files/data_with_valid_values.parquet' into table testdoublevalid_n0;
select * from testdoublevalid_n0;
drop table testdoublevalid_n0;

create table testdecimal22_2valid_n0
(col_INT32_UINT_8  decimal(22,2),
 col_INT32_UINT_16 decimal(22,2),
 col_INT32_UINT_32 decimal(22,2),
 col_INT64_UINT_64 decimal(22,2)) stored as parquet;
load data local inpath '../../data/files/data_with_valid_values.parquet' into table testdecimal22_2valid_n0;
select * from testdecimal22_2valid_n0;
drop table testdecimal22_2valid_n0;

create table testdecimal13_2valid_n0
(col_INT32_UINT_8  decimal(13,2),
 col_INT32_UINT_16 decimal(13,2),
 col_INT32_UINT_32 decimal(13,2),
 col_INT64_UINT_64 decimal(13,2)) stored as parquet;
load data local inpath '../../data/files/data_with_valid_values.parquet' into table testdecimal13_2valid_n0;
select * from testdecimal13_2valid_n0;
drop table testdecimal13_2valid_n0;

create table testdecimal8_2valid_n0
(col_INT32_UINT_8  decimal(8,2),
 col_INT32_UINT_16 decimal(8,2),
 col_INT32_UINT_32 decimal(8,2),
 col_INT64_UINT_64 decimal(8,2)) stored as parquet;
load data local inpath '../../data/files/data_with_valid_values.parquet' into table testdecimal8_2valid_n0;
select * from testdecimal8_2valid_n0;
drop table testdecimal8_2valid_n0;

create table testdecimal6_2valid_n0
(col_INT32_UINT_8  decimal(6,2),
 col_INT32_UINT_16 decimal(6,2),
 col_INT32_UINT_32 decimal(6,2),
 col_INT64_UINT_64 decimal(6,2)) stored as parquet;
load data local inpath '../../data/files/data_with_valid_values.parquet' into table testdecimal6_2valid_n0;
select * from testdecimal6_2valid_n0;
drop table testdecimal6_2valid_n0;

create table testdecimal3_2valid_n0
(col_INT32_UINT_8  decimal(3,2),
 col_INT32_UINT_16 decimal(3,2),
 col_INT32_UINT_32 decimal(3,2),
 col_INT64_UINT_64 decimal(3,2)) stored as parquet;
load data local inpath '../../data/files/data_with_valid_values.parquet' into table testdecimal3_2valid_n0;
select * from testdecimal3_2valid_n0;
drop table testdecimal3_2valid_n0;
