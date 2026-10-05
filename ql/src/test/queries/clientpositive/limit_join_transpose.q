--! qt:dataset:src1
--! qt:dataset:src

-- HIVE-29580: the derived tables' columns are aliased explicitly because "select *" over the
-- src2/src3 join projects two columns named key (and value), making the src2.key references
-- ambiguous; the two key candidates are not even equal here since the join is on value.

SET hive.vectorized.execution.enabled=false;
set hive.mapred.mode=nonstrict;
set hive.optimize.limittranspose=false;

explain
select *
from src src1 left outer join src src2
on src1.key = src2.key
limit 1;

select *
from src src1 left outer join src src2
on src1.key = src2.key
limit 1;


set hive.optimize.limittranspose=true;
set hive.optimize.limittranspose.reductionpercentage=0.0001f;
set hive.optimize.limittranspose.reductiontuples=10;

explain
select *
from src src1 left outer join src src2
on src1.key = src2.key
limit 1;

select *
from src src1 left outer join src src2
on src1.key = src2.key
limit 1;


set hive.optimize.limittranspose.reductionpercentage=0.1f;
set hive.optimize.limittranspose.reductiontuples=10;

explain
select *
from src src1 left outer join src src2
on src1.key = src2.key
limit 1;

select *
from src src1 left outer join src src2
on src1.key = src2.key
limit 1;

explain
select *
from src src1 right outer join (
  select src2.key, src2.value, src3.key as key3, src3.value as value3
  from src src2 left outer join src src3
  on src2.value = src3.value) src2
on src1.key = src2.key
limit 1;

select *
from src src1 right outer join (
  select src2.key, src2.value, src3.key as key3, src3.value as value3
  from src src2 left outer join src src3
  on src2.value = src3.value) src2
on src1.key = src2.key
limit 1;


set hive.optimize.limittranspose.reductionpercentage=1f;
set hive.optimize.limittranspose.reductiontuples=0;

explain
select *
from src src1 right outer join (
  select src2.key, src2.value, src3.key as key3, src3.value as value3
  from src src2 left outer join src src3
  on src2.value = src3.value) src2
on src1.key = src2.key
limit 1;

select *
from src src1 right outer join (
  select src2.key, src2.value, src3.key as key3, src3.value as value3
  from src src2 left outer join src src3
  on src2.value = src3.value) src2
on src1.key = src2.key
limit 1;

explain
select *
from src src1 right outer join (
  select src2.key, src2.value
  from src src2 left outer join src src3
  on src2.value = src3.value) src2
on src1.key = src2.key
order by src2.key
limit 1;

select *
from src src1 right outer join (
  select src2.key, src2.value
  from src src2 left outer join src src3
  on src2.value = src3.value) src2
on src1.key = src2.key
order by src2.key
limit 1;

explain
select *
from src src1 right outer join (
  select src2.key, src2.value, src3.key as key3, src3.value as value3
  from src src2 left outer join src src3
  on src2.value = src3.value) src2
on src1.key = src2.key
limit 0;

select *
from src src1 right outer join (
  select src2.key, src2.value, src3.key as key3, src3.value as value3
  from src src2 left outer join src src3
  on src2.value = src3.value) src2
on src1.key = src2.key
limit 0;


set hive.mapred.mode=nonstrict;
set hive.optimize.limittranspose=false;

explain
select *
from src src1 left outer join src src2
on src1.key = src2.key
limit 1 offset 1;

select *
from src src1 left outer join src src2
on src1.key = src2.key
limit 1 offset 1;


set hive.optimize.limittranspose=true;
set hive.optimize.limittranspose.reductionpercentage=0.0001f;
set hive.optimize.limittranspose.reductiontuples=10;

explain
select *
from src src1 left outer join src src2
on src1.key = src2.key
limit 1 offset 1;

select *
from src src1 left outer join src src2
on src1.key = src2.key
limit 1 offset 1;


set hive.optimize.limittranspose.reductionpercentage=0.1f;
set hive.optimize.limittranspose.reductiontuples=10;

explain
select *
from src src1 left outer join src src2
on src1.key = src2.key
limit 1 offset 1;

select *
from src src1 left outer join src src2
on src1.key = src2.key
limit 1 offset 1;

explain
select *
from src src1 right outer join (
  select src2.key, src2.value, src3.key as key3, src3.value as value3
  from src src2 left outer join src src3
  on src2.value = src3.value) src2
on src1.key = src2.key
limit 1 offset 1;

select *
from src src1 right outer join (
  select src2.key, src2.value, src3.key as key3, src3.value as value3
  from src src2 left outer join src src3
  on src2.value = src3.value) src2
on src1.key = src2.key
limit 1 offset 1;


set hive.optimize.limittranspose.reductionpercentage=1f;
set hive.optimize.limittranspose.reductiontuples=0;

explain
select *
from src src1 right outer join (
  select src2.key, src2.value, src3.key as key3, src3.value as value3
  from src src2 left outer join src src3
  on src2.value = src3.value) src2
on src1.key = src2.key
limit 1 offset 1;

select *
from src src1 right outer join (
  select src2.key, src2.value, src3.key as key3, src3.value as value3
  from src src2 left outer join src src3
  on src2.value = src3.value) src2
on src1.key = src2.key
limit 1 offset 1;

explain
select *
from src src1 right outer join (
  select src2.key, src2.value
  from src src2 left outer join src src3
  on src2.value = src3.value) src2
on src1.key = src2.key
order by src2.key
limit 1 offset 1;

select *
from src src1 right outer join (
  select src2.key, src2.value
  from src src2 left outer join src src3
  on src2.value = src3.value) src2
on src1.key = src2.key
order by src2.key
limit 1 offset 1;
