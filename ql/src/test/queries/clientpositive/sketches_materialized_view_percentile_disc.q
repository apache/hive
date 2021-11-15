--! qt:transactional
set hive.fetch.task.conversion=none;

create table sketch_input (id int, category char(1))
STORED AS ORC
TBLPROPERTIES ('transactional'='true');

insert into table sketch_input values
  (1,'a'),(1, 'a'), (2, 'a'), (3, 'a'), (4, 'a'), (5, 'a'), (6, 'a'), (7, 'a'), (8, 'a'), (9, 'a'), (10, 'a'),
  (6,'b'),(6, 'b'), (7, 'b'), (8, 'b'), (9, 'b'), (10, 'b'), (11, 'b'), (12, 'b'), (13, 'b'), (14, 'b'), (15, 'b')
; 

-- create an mv for the intermediate results
create  materialized view mv_1 as
  select category, ds_kll_sketch(cast(id as float)) from sketch_input group by category;

-- bi mode on
set hive.optimize.bi.enabled=true;

explain
select 'rewrite; mv matching', category, percentile_disc(0.2) within group (order by id) from sketch_input group by category;
select 'rewrite; mv matching', category, percentile_disc(0.2) within group (order by id) from sketch_input group by category;

set hive.optimize.bi.enabled=false;

explain
select 'no rewrite; no mv usage', category, percentile_disc(0.2) within group (order by id) from sketch_input group by category;
select 'no rewrite; no mv usage', category, percentile_disc(0.2) within group (order by id) from sketch_input group by category;

set hive.optimize.bi.enabled=true;

insert into table sketch_input values
  (1,'a'),(1, 'a'), (2, 'a'), (3, 'a'), (4, 'a'), (5, 'a'), (6, 'a'), (7, 'a'), (8, 'a'), (9, 'a'), (10, 'a'),
  (6,'b'),(6, 'b'), (7, 'b'), (8, 'b'), (9, 'b'), (10, 'b'), (11, 'b'), (12, 'b'), (13, 'b'), (14, 'b'), (15, 'b')
;

explain
select 'rewrite; but no mv usage', category, percentile_disc(0.2) within group (order by id) from sketch_input group by category;
select 'rewrite; but no mv usage', category, percentile_disc(0.2) within group (order by id) from sketch_input group by category;

explain
alter materialized view mv_1 rebuild;
alter materialized view mv_1 rebuild;

explain
select 'rewrite; mv matching', category, percentile_disc(0.2) within group (order by id) from sketch_input group by category;
select 'rewrite; mv matching', category, percentile_disc(0.2) within group (order by id) from sketch_input group by category;

-- rewrite+mv matching with rollup
explain
select 'rewrite;mv matching with rollup',percentile_disc(0.2) within group (order by id) from sketch_input;
select 'rewrite;mv matching with rollup',percentile_disc(0.2) within group (order by id) from sketch_input;

drop materialized view mv_1;
