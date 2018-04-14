--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.limit.pushdown.memory.usage=0.3f;
set hive.optimize.reducededuplication.min.reducer=1;

explain
select key, value, avg(key + 1) from src
group by key, value
order by key, value limit 20;

select key, value, avg(key + 1) from src
group by key, value
order by key, value limit 20;

explain
select key, value, avg(key + 1) from src
group by key, value
order by key, value desc limit 20;

select key, value, avg(key + 1) from src
group by key, value
order by key, value desc limit 20;

explain
select key, value, avg(key + 1) from src
group by key, value
order by key desc, value limit 20;

select key, value, avg(key + 1) from src
group by key, value
order by key desc, value limit 20;

explain
select key, value, avg(key + 1) from src
group by value, key
order by key, value limit 20;

select key, value, avg(key + 1) from src
group by value, key
order by key, value limit 20;

explain
select key, value, avg(key + 1) from src
group by value, key
order by key desc, value limit 20;

select key, value, avg(key + 1) from src
group by value, key
order by key desc, value limit 20;

explain
select key, value, avg(key + 1) from src
group by value, key
order by key desc limit 20;

select key, value, avg(key + 1) from src
group by value, key
order by key desc limit 20;

explain
select key, value, count(key + 1) as agg1 from src 
group by key, value
order by key, value, agg1 limit 20;

select key, value, count(key + 1) as agg1 from src 
group by key, value
order by key, value, agg1 limit 20;

explain
select key, value, count(key + 1) as agg1 from src 
group by key, value
order by key desc, value, agg1 limit 20;

select key, value, count(key + 1) as agg1 from src 
group by key, value
order by key desc, value, agg1 limit 20;

-- NOT APPLICABLE
explain
select value, avg(key + 1) myavg from src
group by value
order by myavg, value desc limit 20;

select value, avg(key + 1) myavg from src
group by value
order by myavg, value desc limit 20;

-- NOT APPLICABLE
explain
select key, value, avg(key + 1) from src
group by value, key with rollup
order by key, value limit 20;

explain
select key, value, avg(key + 1) from src
group by rollup(value, key)
order by key, value limit 20;

select key, value, avg(key + 1) from src
group by value, key with rollup
order by key, value limit 20;
