set hive.mapred.mode=nonstrict;
set hive.optimize.limitjointranspose=false;

explain
select *
from src src1 left outer join src src2
on src1.key = src2.key
limit 1;

select *
from src src1 left outer join src src2
on src1.key = src2.key
limit 1;


set hive.optimize.limitjointranspose=true;
set hive.optimize.limitjointranspose.reductionpercentage=0.0001f;
set hive.optimize.limitjointranspose.reductiontuples=10;

explain
select *
from src src1 left outer join src src2
on src1.key = src2.key
limit 1;

select *
from src src1 left outer join src src2
on src1.key = src2.key
limit 1;


set hive.optimize.limitjointranspose.reductionpercentage=0.1f;
set hive.optimize.limitjointranspose.reductiontuples=10;

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
  select *
  from src src2 left outer join src src3
  on src2.value = src3.value) src2
on src1.key = src2.key
limit 1;

select *
from src src1 right outer join (
  select *
  from src src2 left outer join src src3
  on src2.value = src3.value) src2
on src1.key = src2.key
limit 1;

set hive.optimize.limitjointranspose.reductionpercentage=1f;
set hive.optimize.limitjointranspose.reductiontuples=0;

explain
select *
from src src1 right outer join (
  select *
  from src src2 left outer join src src3
  on src2.value = src3.value) src2
on src1.key = src2.key
limit 1;

select *
from src src1 right outer join (
  select *
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
  select *
  from src src2 left outer join src src3
  on src2.value = src3.value) src2
on src1.key = src2.key
limit 0;

select *
from src src1 right outer join (
  select *
  from src src2 left outer join src src3
  on src2.value = src3.value) src2
on src1.key = src2.key
limit 0;
