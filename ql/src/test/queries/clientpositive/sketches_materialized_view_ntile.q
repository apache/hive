--! qt:transactional
set hive.fetch.task.conversion=none;
set hive.strict.checks.type.safety=false;

create table sketch_input (id int, category char(1))
STORED AS ORC
TBLPROPERTIES ('transactional'='true');

insert into table sketch_input values
  (1,'a'),(1, 'a'), (2, 'a'), (3, 'a'), (4, 'a'), (5, 'a'), (6, 'a'), (7, 'a'), (8, 'a'), (9, 'a'), (10, 'a'),
  (6,'b'),(6, 'b'), (7, 'b'), (8, 'b'), (9, 'b'), (10, 'b'), (11, 'b'), (12, 'b'), (13, 'b'), (14, 'b'), (15, 'b')
; 

-- create an mv for the intermediate results
create  materialized view mv_1 as
  select category,ds_kll_sketch(cast(id as float)) from sketch_input group by category;

-- bi mode on
set hive.optimize.bi.enabled=true;

explain
select 'rewrite; mv matching', id, ntile(4) over (order by id) from sketch_input order by id;
select 'rewrite; mv matching', id, ntile(4) over (order by id) from sketch_input order by id;

set hive.optimize.bi.enabled=false;

explain
select 'no rewrite; no mv matching', id, ntile(4) over (order by id) from sketch_input order by id;
select 'no rewrite; no mv matching', id, ntile(4) over (order by id) from sketch_input order by id;

set hive.optimize.bi.enabled=true;

insert into table sketch_input values
  (1,'a'),(1, 'a'), (2, 'a'), (3, 'a'), (4, 'a'), (5, 'a'), (6, 'a'), (7, 'a'), (8, 'a'), (9, 'a'), (10, 'a'),
  (6,'b'),(6, 'b'), (7, 'b'), (8, 'b'), (9, 'b'), (10, 'b'), (11, 'b'), (12, 'b'), (13, 'b'), (14, 'b'), (15, 'b')
;

explain
select 'rewrite; no mv matching', id, ntile(4) over (order by id) from sketch_input order by id;
select 'rewrite; no mv matching', id, ntile(4) over (order by id) from sketch_input order by id;

explain
alter materialized view mv_1 rebuild;
alter materialized view mv_1 rebuild;

explain
select 'rewrite; mv matching', id, ntile(4) over (order by id) from sketch_input order by id;
select 'rewrite; mv matching', id, ntile(4) over (order by id) from sketch_input order by id;

-- rewrite+mv matching with rollup
explain
select 'rewrite; mv matching', category, id, ntile(4) over (partition by category order by id) from sketch_input order by category,id;
select 'rewrite; mv matching', category, id, ntile(4) over (partition by category order by id) from sketch_input order by category,id;

drop materialized view mv_1;
