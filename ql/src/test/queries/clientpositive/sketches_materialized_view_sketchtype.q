--! qt:transactional
set hive.fetch.task.conversion=none;

create table sketch_input (id int, category char(1))
STORED AS ORC
TBLPROPERTIES ('transactional'='true');

insert into table sketch_input values
  (1,'a'),(1, 'a'), (2, 'a'), (3, 'a'), (4, 'a'), (5, 'a'), (6, 'a'), (7, 'a'), (8, 'a'), (9, 'a'), (10, 'a'),
  (6,'b'),(6, 'b'), (7, 'b'), (8, 'b'), (9, 'b'), (10, 'b'), (11, 'b'), (12, 'b'), (13, 'b'), (14, 'b'), (15, 'b')
; 

-- bi mode on
set hive.optimize.bi.enabled=true;

-- create an mv for the intermediate results
create materialized view mv_1 as
  select category, count(distinct id) from sketch_input group by category;

explain
select 'rewrite; mv matching', category, count(distinct id) from sketch_input group by category;

-- switch to a different count distinct sketch implementation
set hive.optimize.bi.rewrite.countdistinct.sketch=cpc;

explain
select 'rewrite; no mv matching', category, count(distinct id) from sketch_input group by category;

drop materialized view mv_1;
