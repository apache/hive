set hive.mapred.mode=nonstrict;

create table s as select * from src limit 10;

explain
select key from s a
union all
select key from s b
order by key;

explain
select key from s a
union all
select key from s b
limit 0;

explain
select key from s a
union all
select key from s b
limit 5;

explain
select key from s a
union all
select key from s b
order by key
limit 5;

explain
select * from(
select src1.key, src2.value
from src src1 left outer join src src2
on src1.key = src2.key
limit 10)subq1
union all 
select * from(
select src1.key, src2.value
from src src1 left outer join src src2
on src1.key = src2.key
limit 10)subq2
limit 5;

set hive.optimize.limittranspose=true;

explain
select key from s a
union all
select key from s b
order by key;

explain
select key from s a
union all
select key from s b
limit 0;

explain
select key from s a
union all
select key from s b
limit 5;

explain
select key from s a
union all
select key from s b
order by key
limit 5;

explain
select * from(
select src1.key, src2.value
from src src1 left outer join src src2
on src1.key = src2.key
limit 10)subq1
union all 
select * from(
select src1.key, src2.value
from src src1 left outer join src src2
on src1.key = src2.key
limit 10)subq2
limit 5;

set hive.optimize.limittranspose.reductionpercentage=0.1f;

explain
select key from s a
union all
select key from s b
limit 5;

set hive.optimize.limittranspose.reductionpercentage=1f;
set hive.optimize.limittranspose.reductiontuples=8;

explain
select key from s a
union all
select key from s b
limit 5;