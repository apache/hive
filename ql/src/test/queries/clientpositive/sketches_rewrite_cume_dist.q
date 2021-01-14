--! qt:transactional


create table sketch_input (id int, category char(1))
STORED AS ORC
TBLPROPERTIES ('transactional'='true');

insert into table sketch_input values
  (1,'a'),(1, 'a'), (2, 'a'), (3, 'a'), (4, 'a'), (5, 'a'), (6, 'a'), (7, 'a'), (8, 'a'), (9, 'a'), (10, 'a'),
  (6,'b'),(6, 'b'), (7, 'b'), (8, 'b'), (9, 'b'), (10, 'b'), (11, 'b'), (12, 'b'), (13, 'b'), (14, 'b'), (15, 'b')
; 

select id,cume_dist() over (order by id) from sketch_input;

select id,cume_dist() over (order by id),ds_kll_cdf(ds, CAST(id AS FLOAT) )[0]
from sketch_input
join ( select ds_kll_sketch(cast(id as float)) as ds from sketch_input ) q
order by id;

set hive.optimize.bi.enabled=true;

-- see if rewrite happens
explain
select id,'rewrite',cume_dist() over (order by id) from sketch_input order by id;

select id,'rewrite',cume_dist() over (order by id) from sketch_input order by id;

-- see if rewrite happens in nested expressions
explain
select id,'rewrite',count(id) over ()*cume_dist() over (order by id) from sketch_input order by id;

select id,'rewrite',count(id) over ()*cume_dist() over (order by id) from sketch_input order by id;


insert into sketch_input values (null,'a'),(null,'b');

explain
select id,'rewrite',cume_dist() over (order by id nulls first) from sketch_input order by id nulls first;

select id,'rewrite',cume_dist() over (order by id nulls first) from sketch_input order by id nulls first;

explain
select id,'rewrite',cume_dist() over (order by id nulls last) from sketch_input order by id nulls last;

select id,'rewrite',cume_dist() over (order by id nulls last) from sketch_input order by id nulls last;

select id,cume_dist() over (order by id) from sketch_input order by id;
