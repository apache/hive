SET hive.vectorized.execution.enabled=true;
set hive.fetch.task.conversion=none;

-- HIVE-
explain
select count (distinct cint) from alltypesorc where cstring1;

select count (distinct cint) from alltypesorc where cstring1;

explain
select count (distinct cint) from alltypesorc where cint;

select count (distinct cint) from alltypesorc where cint;

explain
select count (distinct cint) from alltypesorc where cfloat;

select count (distinct cint) from alltypesorc where cfloat;

explain
select count (distinct cint) from alltypesorc where ctimestamp1;

select count (distinct cint) from alltypesorc where ctimestamp1;
