create table lvj_stats (id string, f1 string);

insert into lvj_stats values
  ('a','v1'), ('a','v2'), ('a','v3'),
  ('b','v4'), ('b','v5'), ('b','v6');

analyze table lvj_stats compute statistics;
analyze table lvj_stats compute statistics for columns;

-- Test that LV columns' stats no longer inflate SELECT columns' sizes
explain
select id, f1, count(*)
from (select id, f1 from lvj_stats group by id, f1) sub
lateral view posexplode(array(f1, f1)) t1 as pos1, val1
group by id, f1;

select id, f1, count(*)
from (select id, f1 from lvj_stats group by id, f1) sub
lateral view posexplode(array(f1, f1)) t1 as pos1, val1
group by id, f1;

-- Test that LV columns' stats no longer override NDV of a base column
alter table lvj_stats update statistics for column id set('numDVs'='0','numNulls'='0');

explain
select id, count(*)
from (select id, f1 from lvj_stats group by id, f1) sub
lateral view posexplode(array(f1, f1)) t1 as pos1, val1
group by id;

select id, count(*)
from (select id, f1 from lvj_stats group by id, f1) sub
lateral view posexplode(array(f1, f1)) t1 as pos1, val1
group by id;
