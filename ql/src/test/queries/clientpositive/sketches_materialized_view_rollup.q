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
  select category, ds_hll_sketch(id),count(id) from sketch_input group by category;

-- see if we use the mv
explain
select category, round(ds_hll_estimate(ds_hll_sketch(id))) from sketch_input group by category;
select category, round(ds_hll_estimate(ds_hll_sketch(id))) from sketch_input group by category;

-- the mv should be used - the rollup should be possible
explain
select round(ds_hll_estimate(ds_hll_sketch(id))) from sketch_input;
select round(ds_hll_estimate(ds_hll_sketch(id))) from sketch_input;

drop materialized view mv_1;
