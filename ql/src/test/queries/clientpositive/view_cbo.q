set hive.mapred.mode=nonstrict;
-- SORT_QUERY_RESULTS

explain
select key, value, avg(key + 1) from src
group by value, key with rollup
order by key, value limit 20;

drop view v;
create view v as
with q1 as ( select key from src where key = '5')
select * from q1;
desc formatted v;

drop view v;
create view v as
select b.key, count(*) as c
from src b
group by b.key
having exists
  (select a.key
  from src a
  where a.key = b.key and a.value > 'val_9'
  )
;
desc formatted v;

drop view v;
create view v as
select *
from src b
where not exists
  (select distinct a.key
  from src a
  where b.value = a.value and a.value > 'val_2'
  )
;
desc formatted v;

drop view v;
create view v as select a.key from src a join src b on a.key=b.key;
desc formatted v;

CREATE VIEW view15 AS
SELECT key,COUNT(value) AS value_count
FROM src
GROUP BY key;
desc formatted view15;

CREATE VIEW view16 AS
SELECT DISTINCT value
FROM src;

desc formatted view16;

drop view v;
create view v as select key from src;
desc formatted v;

drop view v;
create view v as select * from src;
desc formatted v;

drop view v;
create view v as select * from src intersect select * from src;
desc formatted v;

drop view v;
create view v as select * from src except select * from src;
desc formatted v;

explain select * from v;
