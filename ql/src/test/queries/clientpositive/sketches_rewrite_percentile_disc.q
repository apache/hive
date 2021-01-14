--! qt:transactional


create table sketch_input (id int, category char(1))
STORED AS ORC
TBLPROPERTIES ('transactional'='true');

insert into table sketch_input values
  (1,'a'),(1, 'a'), (2, 'a'), (3, 'a'), (4, 'a'), (5, 'a'), (6, 'a'), (7, 'a'), (8, 'a'), (9, 'a'), (10, 'a'),
  (6,'b'),(6, 'b'), (7, 'b'), (8, 'b'), (9, 'b'), (10, 'b'), (11, 'b'), (12, 'b'), (13, 'b'), (14, 'b'), (15, 'b')
; 

select percentile_disc(0.3) within group(order by id) from sketch_input;

set hive.optimize.bi.enabled=true;

-- see if rewrite happens
explain
select percentile_disc(0.3) within group(order by id) from sketch_input;

select percentile_disc(0.3) within group(order by id) from sketch_input;

