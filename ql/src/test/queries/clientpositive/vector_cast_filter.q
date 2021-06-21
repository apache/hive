--! qt:dataset:alltypesorc
SET hive.vectorized.execution.enabled=false;

explain
select count (distinct cint) from alltypesorc where !cstring1;

select count (distinct cint) from alltypesorc where !cstring1;

explain
select count (distinct cint) from alltypesorc where cint and cstring1;

select count (distinct cint) from alltypesorc where cint and cstring1;

explain
select count (distinct cint) from alltypesorc where cfloat or cint;

select count (distinct cint) from alltypesorc where cfloat or cint;

SET hive.vectorized.execution.enabled=true;

explain vectorization expression
select count (distinct cint) from alltypesorc where !cstring1;

select count (distinct cint) from alltypesorc where !cstring1;

explain vectorization expression
select count (distinct cint) from alltypesorc where cint and cstring1;

select count (distinct cint) from alltypesorc where cint and cstring1;

explain vectorization expression
select count (distinct cint) from alltypesorc where cfloat or cint;

select count (distinct cint) from alltypesorc where cfloat or cint;
