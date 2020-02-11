set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.fetch.task.conversion=none;
set hive.cbo.enable=false;
set hive.vectorized.use.checked.expressions=true;

--SORT_QUERY_RESULTS

CREATE TABLE test_overflow (
    ctinyint1 TINYINT,
    ctinyint2 TINYINT,
    csmallint1 SMALLINT,
    csmallint2 SMALLINT,
    cint1 INT,
    cint2 INT,
    cbigint1 BIGINT,
    cbigint2 BIGINT,
    cfloat1 FLOAT,
    cfloat2 FLOAT,
    cdouble1 DOUBLE,
    cdouble2 DOUBLE)
STORED AS PARQUET;

-- values stored in the columns are the min and max respectively for each column type
insert into test_overflow values (-128, 127, -32768, 32767, -2147483648, 2147483647, -9223372036854775808, 9223372036854775807, 1.401298464324817E-45, 3.4028234663852886E38, 4.9E-324, 1.7976931348623157E308);

insert into test_overflow values (127, -128, 32767, -32768, 2147483647, -2147483648, 9223372036854775807, -9223372036854775808, 3.4028234663852886E38, 1.401298464324817E-45, 1.7976931348623157E308, 4.9E-324);

-- stored values represent the MAX_RANGE/2 and MAX_RANGE/2 + 1 for integer types. These are used to cause overflow in pmod UDF
insert into test_overflow values (64, 65, 32767, -32768, 1073741824, 1073741825, 9223372036854775807, -9223372036854775808, 3.4028234663852886E38, 1.401298464324817E-45, 1.7976931348623157E308, 4.9E-324);

select * from test_overflow order by cint1;

-- the substraction in the where clause tips integer column below the min value causing it to underflow
set hive.vectorized.execution.enabled=true;
explain vectorization expression
select cint1, (cint1-2) from test_overflow where (cint1 - 2) > 0 order by cint1;
select cint1, (cint1-2) from test_overflow where (cint1 - 2) > 0 order by cint1;

-- results should match in non-vectorized execution
set hive.vectorized.execution.enabled=false;
select cint1, (cint1-2) from test_overflow where (cint1 - 2) > 0 order by cint1;

-- the addition in the where clause tips integer column over the max value causing it to overflow
set hive.vectorized.execution.enabled=true;
explain vectorization expression
select cint2, (cint2+2) from test_overflow where (cint2 + 2) < 0 order by cint2;
select cint2, (cint2+2) from test_overflow where (cint2 + 2) < 0 order by cint2;

-- results should match in non-vectorized execution
set hive.vectorized.execution.enabled=false;
select cint2, (cint2+2) from test_overflow where (cint2 + 2) < 0 order by cint2;


-- test overflow in multiply operator
set hive.vectorized.execution.enabled=true;
explain vectorization expression
select cint2, (cint2 * 2) from test_overflow where (cint2 * 2) < 0 order by cint2;
select cint2, (cint2 * 2) from test_overflow where (cint2 * 2) < 0 order by cint2;

-- results should match in non-vectorized execution
set hive.vectorized.execution.enabled=false;
select cint2, (cint2 * 2) from test_overflow where (cint2 * 2) < 0 order by cint2;


-- underflow in tinyint case
set hive.vectorized.execution.enabled=true;
explain vectorization expression
select ctinyint1, (ctinyint1-2Y) from test_overflow where (ctinyint1 - 2Y) > 0  order by ctinyint1;
select ctinyint1, (ctinyint1-2Y) from test_overflow where (ctinyint1 - 2Y) > 0  order by ctinyint1;

-- results should match in non-vectorized execution
set hive.vectorized.execution.enabled=false;
select ctinyint1, (ctinyint1-2Y) from test_overflow where (ctinyint1 - 2Y) > 0  order by ctinyint1;

-- overflow in tinyint case
set hive.vectorized.execution.enabled=true;
explain vectorization expression
select ctinyint2, (ctinyint2 + 2) from test_overflow where (ctinyint2 + 2Y) < 0  order by ctinyint2;
select ctinyint2, (ctinyint2 + 2) from test_overflow where (ctinyint2 + 2Y) < 0  order by ctinyint2;

-- results should match in non-vectorized execution
set hive.vectorized.execution.enabled=false;
select ctinyint2, (ctinyint2 + 2) from test_overflow where (ctinyint2 + 2Y) < 0 order by ctinyint2;

-- overflow for short datatype in multiply operation
set hive.vectorized.execution.enabled=true;
explain vectorization expression
select csmallint2, csmallint2 * 2 from test_overflow where (csmallint2 * 2S) < 0 order by csmallint2;
select csmallint2, csmallint2 * 2 from test_overflow where (csmallint2 * 2S) < 0 order by csmallint2;

-- results should match in non-vectorized execution
set hive.vectorized.execution.enabled=false;
explain vectorization expression
select csmallint2, csmallint2 * 2 from test_overflow where (csmallint2 * 2S) < 0 order by csmallint2;
select csmallint2, csmallint2 * 2 from test_overflow where (csmallint2 * 2S) < 0 order by csmallint2;

create table parquettable (t1 tinyint, t2 tinyint, i1 int, i2 int) stored as parquet;
insert into parquettable values (-104, 25,2147483647, 10), (-112, 24, -2147483648, 10), (54, 9, 2147483647, -50);


-- test ColSubstractCol operation underflow
explain vectorization expression select t1, t2, (t1-t2) as diff from parquettable where (t1-t2) < 50 order by diff desc;
select t1, t2, (t1-t2) as diff from parquettable where (t1-t2) < 50 order by diff desc;

-- the above query should return the same results in non-vectorized mode
set hive.vectorized.execution.enabled=false;
select t1, t2, (t1-t2) as diff from parquettable where (t1-t2) < 50 order by diff desc;

-- test integer ColSubstractCol overflow
set hive.vectorized.execution.enabled=true;
explain vectorization expression select i1, i2, (i1-i2) as diff from parquettable where (i1-i2) < 50 order by diff desc;
select i1, i2, (i1-i2) as diff from parquettable where (i1-i2) < 50 order by diff desc;

-- the above query should return the same results in non-vectorized mode
set hive.vectorized.execution.enabled=false;
select i1, i2, (i1-i2) as diff from parquettable where (i1-i2) < 50 order by diff desc;

--Test ColumnUnaryMinus.txt
set hive.vectorized.execution.enabled=false;
select cint1 from test_overflow where -cint1 >= 0 order by cint1;
select cfloat1 from test_overflow where -cfloat1 >= 0 order by cfloat1;

set hive.vectorized.execution.enabled=true;
select cint1 from test_overflow where -cint1 >= 0 order by cint1;
select cfloat1 from test_overflow where -cfloat1 >= 0 order by cfloat1;


-- test scalarMultiplyCol overflow
set hive.vectorized.execution.enabled=false;
select cint1, 2*cint2 from test_overflow where 2*cint2 >= 0 order by cint1;

set hive.vectorized.execution.enabled=true;
select cint1, 2*cint2 from test_overflow where 2*cint2 >= 0 order by cint1;

-- test ConstantVectorExpression overflow behavior
-- this works without checked expressions but good to have a test case exercising this
set hive.vectorized.execution.enabled=false;
select 2147483648 from test_overflow;

set hive.vectorized.execution.enabled=false;
select 2147483648 from test_overflow;

-- test PosMod vector expression, the third row will overflow the int range and cause the result to be negative
set hive.vectorized.execution.enabled=false;
select * from test_overflow where pmod(cint1, 1073741825) > 0 order by cint1;

-- results should match in non-vectorized execution
set hive.vectorized.execution.enabled=true;
select * from test_overflow where pmod(cint1, 1073741825) > 0  order by cint1;

-- cause short range overflow in pmod implementation, this works without posmod range checks but still good to have
set hive.vectorized.execution.enabled=false;
select * from test_overflow where pmod(csmallint1, 16385S) > 0  order by ctinyint1;

set hive.vectorized.execution.enabled=true;
explain vectorization expression select * from test_overflow where pmod(csmallint1, 16385S) > 0 order by ctinyint1;
select * from test_overflow where pmod(csmallint1, 16385S) > 0 order by ctinyint1;
