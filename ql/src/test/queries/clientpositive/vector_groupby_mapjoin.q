--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
SET hive.vectorized.execution.enabled = true;
set hive.fetch.task.conversion=none;
SET hive.auto.convert.join=true;
SET hive.auto.convert.join.noconditionaltask=true;
SET hive.auto.convert.join.noconditionaltask.size=1000000000;

-- HIVE-12738 -- We are checking if a MapJoin after a GroupBy will work properly.
explain vectorization expression
select *
from src
where not key in
(select key from src)
order by key;

select *
from src
where not key in
(select key from src)
order by key;

CREATE TABLE orcsrc STORED AS ORC AS SELECT * FROM src;

select *
from orcsrc
where not key in
(select key from orcsrc)
order by key;

select *
from orcsrc
where not key in
(select key from orcsrc)
order by key;
