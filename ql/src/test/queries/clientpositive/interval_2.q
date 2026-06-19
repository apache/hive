--! qt:dataset:src
-- group-by/order-by/aggregation functions

select
  iym, count(*), min(key), max(key), min(iym), max(iym), min(idt), max(idt)
from (
  select
    key,
    interval_year_month(concat(key, '-1')) as iym,
    interval_day_time(concat(key, ' 1:1:1')) as idt
  from src) q1
group by iym 
order by iym asc
limit 5;

select
  iym, count(*), min(key), max(key), min(iym), max(iym), min(idt), max(idt)
from (
  select
    key,
    interval_year_month(concat(key, '-1')) as iym,
    interval_day_time(concat(key, ' 1:1:1')) as idt
  from src) q1
group by iym 
order by iym desc
limit 5;

-- same query as previous, with having clause
select
  iym, count(*), min(key), max(key), min(iym), max(iym), min(idt), max(idt)
from (
  select
    key,
    interval_year_month(concat(key, '-1')) as iym,
    interval_day_time(concat(key, ' 1:1:1')) as idt
  from src) q1
group by iym 
having max(idt) > interval '496 0:0:0' day to second
order by iym desc
limit 5;

select
  idt, count(*), min(key), max(key), min(iym), max(iym), min(idt), max(idt)
from (
  select
    key,
    interval_year_month(concat(key, '-1')) as iym,
    interval_day_time(concat(key, ' 1:1:1')) as idt
  from src) q1
group by idt 
order by idt asc
limit 5;

select
  idt, count(*), min(key), max(key), min(iym), max(iym), min(idt), max(idt)
from (
  select
    key,
    interval_year_month(concat(key, '-1')) as iym,
    interval_day_time(concat(key, ' 1:1:1')) as idt
  from src) q1
group by idt 
order by idt desc
limit 5;

-- same query as previous, with having clause
select
  idt, count(*), min(key), max(key), min(iym), max(iym), min(idt), max(idt)
from (
  select
    key,
    interval_year_month(concat(key, '-1')) as iym,
    interval_day_time(concat(key, ' 1:1:1')) as idt
  from src) q1
group by idt 
having max(iym) < interval '496-0' year to month
order by idt desc
limit 5;

select
  count(iym), count(idt), min(key), max(key), min(iym), max(iym), min(idt), max(idt)
from (
  select
    key,
    interval_year_month(concat(key, '-1')) as iym,
    interval_day_time(concat(key, ' 1:1:1')) as idt
  from src) q1;

