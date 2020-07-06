--! qt:dataset:impala_dataset
drop table if exists type_test;
create table type_test(
  tt_tinyint tinyint,
  tt_smallint smallint,
  tt_int int,
  tt_bigint bigint,
  tt_float float,
  tt_double double,
  tt_double_precision double precision,
  tt_decimal decimal,
  tt_decimal_pre decimal(8,4),
  tt_numeric numeric,
  tt_numeric_pre numeric(8,4),
  tt_timestamp timestamp,
  tt_string string,
  tt_varchar varchar(13),
  tt_char char(13),
  tt_boolean boolean
  -- BINARY NOT SUPPORTED tt_binary binary
) partitioned by (tt_date date) stored as parquet;

-- tests basic types
explain select * from type_test;

-- generates ImpalaBinaryCompExpr
explain select 1 < tt_int from type_test;

-- generates ImpalaFunctionCallExpr
explain select cast(tt_int as bigint) from type_test;

-- generates ImpalaStringLiteral
explain select tt_decimal, 'literal' from type_test;

-- generates ImpalaCaseExpr
explain select case when tt_int > 34 then 34 else tt_int end  from type_test;

-- generates ImpalaInExpr
explain select tt_int in (1,2,3,4) from type_test;

-- generates ImpalaTimestampArithmeticExpr
explain select cast('01/01/1984' as date) + interval 1 days;

-- generates ImpalaAnalyticExpr (ends up with a exchange in front, so does not exercise result file code)
explain select lag(tt_int,1) over(partition by tt_date) as yest from type_test;

-- generates ImpalaBoolLiteral
explain select 1 is null;

-- generates ImpalaIsNullExpr
explain select tt_int is null from type_test;

-- generates ImpalaCompoundExpr
explain select tt_int > 4 and tt_int < 10 or tt_int = 33 from type_test;

-- generates ImpalaDateLiteral
explain select date '1984-01-01';
