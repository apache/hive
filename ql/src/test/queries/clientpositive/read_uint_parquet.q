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

create table testfloatinv
(col_INT32_UINT_8  float,
 col_INT32_UINT_16 float,
 col_INT32_UINT_32 float,
 col_INT64_UINT_64 float) stored as parquet;
load data local inpath '../../data/files/data_including_invalid_values.parquet' into table testfloatinv;
select * from testfloatinv;
drop table testfloatinv;

create table testdoubleinv
(col_INT32_UINT_8  double,
 col_INT32_UINT_16 double,
 col_INT32_UINT_32 double,
 col_INT64_UINT_64 double) stored as parquet;
load data local inpath '../../data/files/data_including_invalid_values.parquet' into table testdoubleinv;
select * from testdoubleinv;
drop table testdoubleinv;

create table testdecimal22_2inv
(col_INT32_UINT_8  decimal(22,2),
 col_INT32_UINT_16 decimal(22,2),
 col_INT32_UINT_32 decimal(22,2),
 col_INT64_UINT_64 decimal(22,2)) stored as parquet;
load data local inpath '../../data/files/data_including_invalid_values.parquet' into table testdecimal22_2inv;
select * from testdecimal22_2inv;
drop table testdecimal22_2inv;

create table testdecimal13_2inv
(col_INT32_UINT_8  decimal(13,2),
 col_INT32_UINT_16 decimal(13,2),
 col_INT32_UINT_32 decimal(13,2),
 col_INT64_UINT_64 decimal(13,2)) stored as parquet;
load data local inpath '../../data/files/data_including_invalid_values.parquet' into table testdecimal13_2inv;
select * from testdecimal13_2inv;
drop table testdecimal13_2inv;

create table testdecimal8_2inv
(col_INT32_UINT_8  decimal(8,2),
 col_INT32_UINT_16 decimal(8,2),
 col_INT32_UINT_32 decimal(8,2),
 col_INT64_UINT_64 decimal(8,2)) stored as parquet;
load data local inpath '../../data/files/data_including_invalid_values.parquet' into table testdecimal8_2inv;
select * from testdecimal8_2inv;
drop table testdecimal8_2inv;

create table testdecimal6_2inv
(col_INT32_UINT_8  decimal(6,2),
 col_INT32_UINT_16 decimal(6,2),
 col_INT32_UINT_32 decimal(6,2),
 col_INT64_UINT_64 decimal(6,2)) stored as parquet;
load data local inpath '../../data/files/data_including_invalid_values.parquet' into table testdecimal6_2inv;
select * from testdecimal6_2inv;
drop table testdecimal6_2inv;

create table testdecimal3_2inv
(col_INT32_UINT_8  decimal(3,2),
 col_INT32_UINT_16 decimal(3,2),
 col_INT32_UINT_32 decimal(3,2),
 col_INT64_UINT_64 decimal(3,2)) stored as parquet;
load data local inpath '../../data/files/data_including_invalid_values.parquet' into table testdecimal3_2inv;
select * from testdecimal3_2inv;
drop table testdecimal3_2inv;

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

create table testfloatvalid
(col_INT32_UINT_8  float,
 col_INT32_UINT_16 float,
 col_INT32_UINT_32 float,
 col_INT64_UINT_64 float) stored as parquet;
load data local inpath '../../data/files/data_with_valid_values.parquet' into table testfloatvalid;
select * from testfloatvalid;
drop table testfloatvalid;

create table testdoublevalid
(col_INT32_UINT_8  double,
 col_INT32_UINT_16 double,
 col_INT32_UINT_32 double,
 col_INT64_UINT_64 double) stored as parquet;
load data local inpath '../../data/files/data_with_valid_values.parquet' into table testdoublevalid;
select * from testdoublevalid;
drop table testdoublevalid;

create table testdecimal22_2valid
(col_INT32_UINT_8  decimal(22,2),
 col_INT32_UINT_16 decimal(22,2),
 col_INT32_UINT_32 decimal(22,2),
 col_INT64_UINT_64 decimal(22,2)) stored as parquet;
load data local inpath '../../data/files/data_with_valid_values.parquet' into table testdecimal22_2valid;
select * from testdecimal22_2valid;
drop table testdecimal22_2valid;

create table testdecimal13_2valid
(col_INT32_UINT_8  decimal(13,2),
 col_INT32_UINT_16 decimal(13,2),
 col_INT32_UINT_32 decimal(13,2),
 col_INT64_UINT_64 decimal(13,2)) stored as parquet;
load data local inpath '../../data/files/data_with_valid_values.parquet' into table testdecimal13_2valid;
select * from testdecimal13_2valid;
drop table testdecimal13_2valid;

create table testdecimal8_2valid
(col_INT32_UINT_8  decimal(8,2),
 col_INT32_UINT_16 decimal(8,2),
 col_INT32_UINT_32 decimal(8,2),
 col_INT64_UINT_64 decimal(8,2)) stored as parquet;
load data local inpath '../../data/files/data_with_valid_values.parquet' into table testdecimal8_2valid;
select * from testdecimal8_2valid;
drop table testdecimal8_2valid;

create table testdecimal6_2valid
(col_INT32_UINT_8  decimal(6,2),
 col_INT32_UINT_16 decimal(6,2),
 col_INT32_UINT_32 decimal(6,2),
 col_INT64_UINT_64 decimal(6,2)) stored as parquet;
load data local inpath '../../data/files/data_with_valid_values.parquet' into table testdecimal6_2valid;
select * from testdecimal6_2valid;
drop table testdecimal6_2valid;

create table testdecimal3_2valid
(col_INT32_UINT_8  decimal(3,2),
 col_INT32_UINT_16 decimal(3,2),
 col_INT32_UINT_32 decimal(3,2),
 col_INT64_UINT_64 decimal(3,2)) stored as parquet;
load data local inpath '../../data/files/data_with_valid_values.parquet' into table testdecimal3_2valid;
select * from testdecimal3_2valid;
drop table testdecimal3_2valid;
