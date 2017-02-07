SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

-- HIVE-
explain vectorization expression
select count (distinct cint) from alltypesorc where cstring1;

select count (distinct cint) from alltypesorc where cstring1;

explain vectorization expression
select count (distinct cint) from alltypesorc where cint;

select count (distinct cint) from alltypesorc where cint;

explain vectorization expression
select count (distinct cint) from alltypesorc where cfloat;

select count (distinct cint) from alltypesorc where cfloat;

explain vectorization expression
select count (distinct cint) from alltypesorc where ctimestamp1;

select count (distinct cint) from alltypesorc where ctimestamp1;
