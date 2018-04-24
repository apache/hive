--! qt:dataset:src
--! qt:dataset:alltypesorc
SET hive.vectorized.execution.enabled=false;
set hive.explain.user=false;
set hive.limit.pushdown.memory.usage=0.3f;
set hive.optimize.reducededuplication.min.reducer=1;

explain
select key,value from src order by key limit 10,20;
select key,value from src order by key limit 10,20;

explain
select key,value from src order by key desc limit 10,20;
select key,value from src order by key desc limit 10,20;

explain
select value, sum(key + 1) as sum from src group by value order by value limit 10,20;
select value, sum(key + 1) as sum from src group by value order by value limit 10,20;

-- deduped RS
explain
select value,avg(key + 1) from src group by value order by value limit 10,20;
select value,avg(key + 1) from src group by value order by value limit 10,20;

-- distincts
explain
select distinct(cdouble) as dis from alltypesorc order by dis limit 10,20;
select distinct(cdouble) as dis from alltypesorc order by dis limit 10,20;

explain
select ctinyint, count(distinct(cdouble)) from alltypesorc group by ctinyint order by ctinyint limit 10,20;
select ctinyint, count(distinct(cdouble)) from alltypesorc group by ctinyint order by ctinyint limit 10,20;

explain
select ctinyint, count(cdouble) from (select ctinyint, cdouble from alltypesorc group by ctinyint, cdouble) t1 group by ctinyint order by ctinyint limit 10,20;
select ctinyint, count(cdouble) from (select ctinyint, cdouble from alltypesorc group by ctinyint, cdouble) t1 group by ctinyint order by ctinyint limit 10,20;

-- multi distinct
explain
select ctinyint, count(distinct(cstring1)), count(distinct(cstring2)) from alltypesorc group by ctinyint order by ctinyint limit 10,20;
select ctinyint, count(distinct(cstring1)), count(distinct(cstring2)) from alltypesorc group by ctinyint order by ctinyint limit 10,20;

-- limit zero
explain
select key,value from src order by key limit 0,0;
select key,value from src order by key limit 0,0;

-- 2MR (applied to last RS)
explain
select value, sum(key) as sum from src group by value order by sum, value limit 10,20;
select value, sum(key) as sum from src group by value order by sum, value limit 10,20;

set hive.map.aggr=false;
-- map aggregation disabled
explain
select value, sum(key) as sum from src group by value order by value limit 10,20;
select value, sum(key) as sum from src group by value order by value limit 10,20;

set hive.limit.pushdown.memory.usage=0.00002f;

-- flush for order-by
explain
select key,value,value,value,value,value,value,value,value from src order by key limit 30,70;
select key,value,value,value,value,value,value,value,value from src order by key limit 30,70;

-- flush for group-by
explain
select sum(key) as sum from src group by concat(key,value,value,value,value,value,value,value,value,value) order by sum limit 30,70;
select sum(key) as sum from src group by concat(key,value,value,value,value,value,value,value,value,value) order by sum limit 30,70;

-- subqueries
explain
select * from
(select key, count(1) from src group by key order by key limit 10,20) subq
join
(select key, count(1) from src group by key limit 20,20) subq2
on subq.key=subq2.key limit 3,5;
select * from
(select key, count(1) from src group by key order by key limit 10,20) subq
join
(select key, count(1) from src group by key order by key limit 20,20) subq2
on subq.key=subq2.key limit 3,5;

