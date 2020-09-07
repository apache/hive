--! qt:transactional
set hive.fetch.task.conversion=none;
set hive.optimize.bi.enabled=true;

create table sketch_input (id int, category char(1))
STORED AS ORC
TBLPROPERTIES ('transactional'='true');

insert into table sketch_input values
  (1,'a'),(1, 'a'), (2, 'a'), (3, 'a'), (4, 'a'), (5, 'a'), (6, 'a'), (7, 'a'), (8, 'a'), (9, 'a'), (10, 'a'),
  (6,'b'),(6, 'b'), (7, 'b'), (8, 'b'), (9, 'b'), (10, 'b'), (11, 'b'), (12, 'b'), (13, 'b'), (14, 'b'), (15, 'b')
; 

explain
create  materialized view mv_1 as
  select 'no-rewrite-may-happen',category, count(distinct id) from sketch_input group by category;
create  materialized view mv_1 as
  select 'no-rewrite-may-happen',category, count(distinct id) from sketch_input group by category;

insert into table sketch_input values
  (1,'a'),(1, 'a'), (2, 'a'), (3, 'a'), (4, 'a'), (5, 'a'), (6, 'a'), (7, 'a'), (8, 'a'), (9, 'a'), (10, 'a'),
  (6,'b'),(6, 'b'), (7, 'b'), (8, 'b'), (9, 'b'), (10, 'b'), (11, 'b'), (12, 'b'), (13, 'b'), (14, 'b'), (15, 'b')
;

explain
alter materialized view mv_1 rebuild;
alter materialized view mv_1 rebuild;

-- see if we use the mv
explain
select 'rewritten;mv not used',category, count(distinct id) from sketch_input group by category;
select 'rewritten;mv not used',category, count(distinct id) from sketch_input group by category;

set hive.optimize.bi.enabled=false;

explain
select 'mv used',category, count(distinct id) from sketch_input group by category;
select 'mv used',category, count(distinct id) from sketch_input group by category;
